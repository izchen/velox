/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/dwio/avro/reader/AvroSchemaConverter.h"

#include <boost/algorithm/string.hpp>
#include <avro/Schema.hh>

#include <unordered_set>

namespace facebook::velox::avro {
using dwio::common::ReaderOptions;

namespace {

void applyDecimalLogicalType(
    const ::avro::LogicalType& logical,
    AvroTypeInfo& info) {
  constexpr int32_t kMaxDecimalPrecision{
      static_cast<int32_t>(LongDecimalType::kMaxPrecision)};
  const auto precisionValue = logical.precision();
  const auto scaleValue = logical.scale();
  if (precisionValue > kMaxDecimalPrecision) {
    VELOX_UNSUPPORTED(
        "Avro decimal precision exceeds the maximum supported by Velox: "
        "precision {}, maximum {}.",
        precisionValue,
        kMaxDecimalPrecision);
  }
  VELOX_CHECK_GE(precisionValue, 1);
  VELOX_CHECK_GE(scaleValue, 0);
  VELOX_CHECK_LE(scaleValue, precisionValue);
  const auto precision = static_cast<uint8_t>(precisionValue);
  const auto scale = static_cast<uint8_t>(scaleValue);
  info.logicalType = AvroLogicalType::kDecimal;
  info.decimalPrecision = precision;
  info.decimalScale = scale;
  info.veloxType = DECIMAL(precision, scale);
}

::avro::NodePtr resolveIfSymbolic(const ::avro::NodePtr& node) {
  if (node->type() == ::avro::Type::AVRO_SYMBOLIC) {
    return ::avro::resolveSymbol(node);
  }
  return node;
}

std::shared_ptr<AvroTypeInfo> buildUnionType(
    const ::avro::NodePtr& node,
    const ReaderOptions& options) {
  VELOX_CHECK(
      node->leaves() > 0,
      "Invalid Avro union: a union must contain at least one schema branch.");

  auto info = std::make_shared<AvroTypeInfo>();
  info->avroType = ::avro::Type::AVRO_UNION;
  std::vector<std::shared_ptr<AvroTypeInfo>> nonNullInfos;
  nonNullInfos.reserve(node->leaves());
  bool allIntsOrLongs = true;
  bool allFloatsOrDoubles = true;

  for (size_t i = 0; i < node->leaves(); ++i) {
    const auto branchNode = resolveIfSymbolic(node->leafAt(i));
    if (branchNode->type() == ::avro::Type::AVRO_NULL) {
      info->nullUnionBranchIndex = i;
      continue;
    }

    auto childInfo = buildTypeInfo(branchNode, options);
    nonNullInfos.push_back(childInfo);

    if (childInfo->logicalType != AvroLogicalType::kNone) {
      allIntsOrLongs = false;
      allFloatsOrDoubles = false;
      continue;
    }

    switch (branchNode->type()) {
      case ::avro::Type::AVRO_INT:
      case ::avro::Type::AVRO_LONG:
        allFloatsOrDoubles = false;
        break;
      case ::avro::Type::AVRO_FLOAT:
      case ::avro::Type::AVRO_DOUBLE:
        allIntsOrLongs = false;
        break;
      default:
        allIntsOrLongs = false;
        allFloatsOrDoubles = false;
        break;
    }
  }

  info->nullable = info->nullUnionBranchIndex.has_value();

  // For a union schema of ["null"],
  // NULL represents the type itself, not nullability.
  if (nonNullInfos.empty()) {
    nonNullInfos.push_back(buildTypeInfo(node->leafAt(0), options));
  }

  info->children = std::move(nonNullInfos);

  if (info->children.size() == 1) {
    auto childInfo = info->children.front();
    childInfo->unionKind = AvroUnionKind::kSimple;
    childInfo->nullUnionBranchIndex = info->nullUnionBranchIndex;
    childInfo->nullable = info->nullable;
    return childInfo;
  }

  // info->children.size() > 1
  if (allIntsOrLongs) {
    info->unionKind = AvroUnionKind::kNumericPromotion;
    info->veloxType = BIGINT();
    info->children.clear();
    return info;
  }
  if (allFloatsOrDoubles) {
    info->unionKind = AvroUnionKind::kNumericPromotion;
    info->veloxType = DOUBLE();
    info->children.clear();
    return info;
  }

  info->unionKind = AvroUnionKind::kStruct;
  std::vector<std::string> fieldNames;
  std::vector<TypePtr> childTypes;
  fieldNames.reserve(info->children.size());
  childTypes.reserve(info->children.size());
  for (size_t index = 0; index < info->children.size(); ++index) {
    fieldNames.push_back("member" + std::to_string(index));
    childTypes.push_back(info->children[index]->veloxType);
    info->childSourceIndices.push_back(index);
  }
  info->fieldNames = fieldNames;
  info->veloxType = ROW(std::move(fieldNames), std::move(childTypes));
  return info;
}

} // namespace

std::shared_ptr<AvroTypeInfo> buildTypeInfo(
    const ::avro::NodePtr& node,
    const ReaderOptions& options) {
  const auto resolvedNode = resolveIfSymbolic(node);
  auto info = std::make_shared<AvroTypeInfo>();
  info->avroType = resolvedNode->type();
  const auto logical = resolvedNode->logicalType();
  const auto logicalType = logical.type();
  switch (resolvedNode->type()) {
    case ::avro::Type::AVRO_NULL:
      info->veloxType = UNKNOWN();
      info->nullable = true;
      break;
    case ::avro::Type::AVRO_BOOL:
      info->veloxType = BOOLEAN();
      break;
    case ::avro::Type::AVRO_INT:
      if (logicalType == ::avro::LogicalType::Type::DATE) {
        info->logicalType = AvroLogicalType::kDate;
        info->veloxType = DATE();
      } else if (logicalType == ::avro::LogicalType::Type::TIME_MILLIS) {
        info->logicalType = AvroLogicalType::kTimeMillis;
        info->veloxType = TIME();
      } else {
        info->veloxType = INTEGER();
      }
      break;
    case ::avro::Type::AVRO_LONG:
      if (logicalType == ::avro::LogicalType::Type::TIME_MICROS) {
        info->logicalType = AvroLogicalType::kTimeMicros;
        info->veloxType = TIME();
      } else if (logicalType == ::avro::LogicalType::Type::TIMESTAMP_MILLIS) {
        info->logicalType = AvroLogicalType::kTimestampMillis;
        info->veloxType = TIMESTAMP();
      } else if (logicalType == ::avro::LogicalType::Type::TIMESTAMP_MICROS) {
        info->logicalType = AvroLogicalType::kTimestampMicros;
        info->veloxType = TIMESTAMP();
      } else if (logicalType == ::avro::LogicalType::Type::TIMESTAMP_NANOS) {
        info->logicalType = AvroLogicalType::kTimestampNanos;
        info->veloxType = TIMESTAMP();
      } else {
        info->veloxType = BIGINT();
      }
      break;
    case ::avro::Type::AVRO_FLOAT:
      info->veloxType = REAL();
      break;
    case ::avro::Type::AVRO_DOUBLE:
      info->veloxType = DOUBLE();
      break;
    case ::avro::Type::AVRO_STRING:
      if (logicalType == ::avro::LogicalType::Type::UUID) {
        info->logicalType = AvroLogicalType::kUuid;
      }
      info->veloxType = VARCHAR();
      break;
    case ::avro::Type::AVRO_BYTES:
      if (logicalType == ::avro::LogicalType::Type::DECIMAL) {
        applyDecimalLogicalType(logical, *info);
      } else {
        info->veloxType = VARBINARY();
      }
      break;
    case ::avro::Type::AVRO_FIXED:
      if (logicalType == ::avro::LogicalType::Type::DECIMAL) {
        const auto fixedSize = resolvedNode->fixedSize();
        if (fixedSize > sizeof(int128_t)) {
          VELOX_UNSUPPORTED(
              "Avro fixed decimal encoding is wider than the maximum supported "
              "by the Velox Avro reader: size {} bytes, maximum {} bytes.",
              fixedSize,
              sizeof(int128_t));
        }
        applyDecimalLogicalType(logical, *info);
      } else if (logicalType == ::avro::LogicalType::Type::UUID) {
        info->logicalType = AvroLogicalType::kUuid;
        info->veloxType = VARBINARY();
      } else {
        info->veloxType = VARBINARY();
      }
      break;
    case ::avro::Type::AVRO_ENUM:
      info->veloxType = VARCHAR();
      break;
    case ::avro::Type::AVRO_ARRAY: {
      auto elementInfo = buildTypeInfo(resolvedNode->leafAt(0), options);
      info->veloxType = ARRAY(elementInfo->veloxType);
      info->children.push_back(elementInfo);
      break;
    }
    case ::avro::Type::AVRO_MAP: {
      // Avro spec mandates that map keys are strings
      auto valueInfo = buildTypeInfo(resolvedNode->leafAt(1), options);
      info->veloxType = MAP(VARCHAR(), valueInfo->veloxType);
      info->children.push_back(valueInfo);
      break;
    }
    case ::avro::Type::AVRO_RECORD: {
      std::vector<std::string> names;
      std::vector<TypePtr> children;
      auto count = resolvedNode->leaves();
      names.reserve(count);
      children.reserve(count);
      info->children.reserve(count);
      std::unordered_set<std::string> fieldNameSeen;
      fieldNameSeen.reserve(count);
      for (size_t i = 0; i < count; ++i) {
        auto fieldName = resolvedNode->nameAt(i);
        if (options.fileColumnNamesReadAsLowerCase()) {
          boost::algorithm::to_lower(fieldName);
        }
        VELOX_CHECK(
            fieldNameSeen.insert(fieldName).second,
            "Avro schema found duplicated field: {}",
            fieldName);
        names.push_back(fieldName);
        auto childInfo = buildTypeInfo(resolvedNode->leafAt(i), options);
        children.push_back(childInfo->veloxType);
        info->children.push_back(childInfo);
        info->childSourceIndices.push_back(i);
      }
      info->fieldNames = names;
      info->veloxType = ROW(std::move(names), std::move(children));
      break;
    }
    case ::avro::Type::AVRO_UNION:
      return buildUnionType(resolvedNode, options);
    default:
      VELOX_UNSUPPORTED(
          "Unsupported Avro type (enum value = {}). "
          "Please refer to avro::Type in "
          "https://github.com/apache/avro/blob/main/lang/c%2B%2B/include/avro/Types.hh "
          "to find the corresponding type.",
          static_cast<int>(resolvedNode->type()));
  }
  if (logicalType != ::avro::LogicalType::Type::NONE &&
      info->logicalType == AvroLogicalType::kNone) {
    VELOX_UNSUPPORTED(
        "Unsupported Avro logical type (enum value = {}). "
        "Please refer to avro::LogicalType::Type in "
        "https://github.com/apache/avro/blob/main/lang/c%2B%2B/include/avro/LogicalType.hh "
        "to find the corresponding logical type.",
        static_cast<int>(logicalType));
  }
  return info;
}

} // namespace facebook::velox::avro
