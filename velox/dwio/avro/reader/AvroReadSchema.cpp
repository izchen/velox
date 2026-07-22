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

#include "velox/dwio/avro/reader/AvroReadSchema.h"

#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "velox/common/base/Exceptions.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/ScanSpec.h"

namespace facebook::velox::avro {
namespace {

std::string childPath(const std::string& path, std::string_view childName) {
  std::string result{path};
  result.push_back('.');
  result.append(childName.data(), childName.size());
  return result;
}

void checkCompatibleRequestedType(
    bool condition,
    const AvroTypeInfo& source,
    const TypePtr& requestedType,
    const std::string& path) {
  VELOX_USER_CHECK(
      condition,
      "Avro requested type mismatch at '{}': cannot read {} as {}.",
      path,
      source.veloxType->toString(),
      requestedType->toString());
}

void validateRequestedLeafTypeForRead(
    const AvroTypeInfo& source,
    const TypePtr& requestedType,
    const std::string& path) {
  switch (source.logicalType) {
    case AvroLogicalType::kDate:
      checkCompatibleRequestedType(
          *requestedType == *DATE(), source, requestedType, path);
      return;
    case AvroLogicalType::kTimeMillis:
    case AvroLogicalType::kTimeMicros:
      checkCompatibleRequestedType(
          *requestedType == *TIME(), source, requestedType, path);
      return;
    case AvroLogicalType::kTimestampMillis:
    case AvroLogicalType::kTimestampMicros:
    case AvroLogicalType::kTimestampNanos:
      checkCompatibleRequestedType(
          *requestedType == *TIMESTAMP(), source, requestedType, path);
      return;
    case AvroLogicalType::kDecimal: {
      checkCompatibleRequestedType(
          requestedType->isDecimal(), source, requestedType, path);
      const auto [precision, scale] = getDecimalPrecisionScale(*requestedType);
      VELOX_USER_CHECK_GE(
          precision,
          source.decimalPrecision,
          "Avro requested decimal precision is smaller than file precision "
          "at '{}': requested {}, file {}.",
          path,
          static_cast<int32_t>(precision),
          static_cast<int32_t>(source.decimalPrecision));
      VELOX_USER_CHECK_EQ(
          scale,
          source.decimalScale,
          "Avro requested decimal scale differs from file scale at '{}': requested {}, file {}.",
          path,
          static_cast<int32_t>(scale),
          static_cast<int32_t>(source.decimalScale));
      return;
    }
    case AvroLogicalType::kUuid:
      if (source.avroType == ::avro::Type::AVRO_STRING) {
        checkCompatibleRequestedType(
            *requestedType == *VARCHAR(), source, requestedType, path);
      } else {
        VELOX_DCHECK_EQ(source.avroType, ::avro::Type::AVRO_FIXED);
        checkCompatibleRequestedType(
            *requestedType == *VARBINARY(), source, requestedType, path);
      }
      return;
    case AvroLogicalType::kNone:
      break;
  }

  switch (source.avroType) {
    case ::avro::Type::AVRO_NULL:
      return;
    case ::avro::Type::AVRO_BOOL:
      checkCompatibleRequestedType(
          *requestedType == *BOOLEAN(), source, requestedType, path);
      return;
    case ::avro::Type::AVRO_INT:
      checkCompatibleRequestedType(
          *requestedType == *INTEGER(), source, requestedType, path);
      return;
    case ::avro::Type::AVRO_LONG:
      checkCompatibleRequestedType(
          *requestedType == *BIGINT(), source, requestedType, path);
      return;
    case ::avro::Type::AVRO_FLOAT:
      checkCompatibleRequestedType(
          *requestedType == *REAL(), source, requestedType, path);
      return;
    case ::avro::Type::AVRO_DOUBLE:
      checkCompatibleRequestedType(
          *requestedType == *DOUBLE(), source, requestedType, path);
      return;
    case ::avro::Type::AVRO_STRING:
    case ::avro::Type::AVRO_ENUM:
      checkCompatibleRequestedType(
          *requestedType == *VARCHAR(), source, requestedType, path);
      return;
    case ::avro::Type::AVRO_BYTES:
    case ::avro::Type::AVRO_FIXED:
      checkCompatibleRequestedType(
          *requestedType == *VARBINARY(), source, requestedType, path);
      return;
    default:
      checkCompatibleRequestedType(false, source, requestedType, path);
      return;
  }
}

size_t findChildIndex(
    const AvroTypeInfo& source,
    std::string_view fieldName,
    const std::string& path) {
  for (size_t i = 0; i < source.fieldNames.size(); ++i) {
    if (source.fieldNames[i] == fieldName) {
      return i;
    }
  }
  VELOX_USER_FAIL(
      "Requested Avro field not found at '{}': {}.", path, fieldName);
}

TypePtr findRequestedChildType(
    const RowType* requestedRowType,
    std::string_view fieldName) {
  if (!requestedRowType) {
    return nullptr;
  }
  const auto index = requestedRowType->getChildIdxIfExists(fieldName);
  if (!index.has_value()) {
    return nullptr;
  }
  return requestedRowType->childAt(index.value());
}

void rejectUnsupportedScanSpecFeaturesForAvro(
    const common::ScanSpec& spec,
    const std::string& path,
    bool hasTransformFallback) {
  hasTransformFallback = hasTransformFallback || spec.hasTransform();
  VELOX_USER_CHECK(
      spec.extractionType() == common::ScanSpec::ExtractionType::kNone ||
          hasTransformFallback,
      "Avro reader does not support native extraction pushdown without a "
      "post-read transform at '{}'.",
      path);
  VELOX_USER_CHECK_NULL(
      spec.deltaUpdate(),
      "Avro reader does not support delta updates at '{}'.",
      path);
  switch (spec.columnType()) {
    case common::ScanSpec::ColumnType::kRowIndex:
      VELOX_USER_FAIL(
          "Avro reader does not support row index columns at '{}'.", path);
      return;
    case common::ScanSpec::ColumnType::kComposite:
      VELOX_USER_FAIL(
          "Avro reader does not support composite columns at '{}'.", path);
      return;
    case common::ScanSpec::ColumnType::kRegular:
      break;
  }

  for (const auto& childSpec : spec.children()) {
    rejectUnsupportedScanSpecFeaturesForAvro(
        *childSpec,
        childPath(path, childSpec->fieldName()),
        hasTransformFallback);
  }
}

std::shared_ptr<AvroTypeInfo> buildReadTypeInfo(
    const AvroTypeInfo& source,
    TypePtr requestedOutputType,
    const common::ScanSpec* scanSpec,
    const std::string& path);

bool hasProjectedChildren(const common::ScanSpec& spec) {
  for (const auto& child : spec.children()) {
    if (child->projectOut()) {
      return true;
    }
  }
  return false;
}

std::shared_ptr<AvroTypeInfo> buildReadRowTypeInfo(
    const AvroTypeInfo& source,
    TypePtr requestedOutputType,
    const common::ScanSpec* scanSpec,
    const std::string& path) {
  const RowType* requestedRowType = nullptr;
  if (requestedOutputType) {
    checkCompatibleRequestedType(
        requestedOutputType->isRow(), source, requestedOutputType, path);
    requestedRowType = &requestedOutputType->asRow();
  }
  VELOX_DCHECK(requestedOutputType || scanSpec);

  if (scanSpec && scanSpec->projectOut()) {
    VELOX_CHECK(hasProjectedChildren(*scanSpec));
  }

  auto target = std::make_shared<AvroTypeInfo>(source);
  target->fieldNames.clear();
  target->children.clear();
  target->childSourceIndices.clear();

  const auto expectedChildren =
      scanSpec ? scanSpec->children().size() : requestedRowType->size();
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  names.reserve(expectedChildren);
  types.reserve(expectedChildren);
  target->children.reserve(expectedChildren);
  target->childSourceIndices.reserve(expectedChildren);

  // Adds one field that must be read from the Avro file.
  auto addFileBackedChild = [&](std::string_view fieldName,
                                TypePtr childRequestedOutputType,
                                const common::ScanSpec* childScanSpec) {
    const auto sourceIndex = findChildIndex(source, fieldName, path);
    auto childInfo = buildReadTypeInfo(
        *source.children[sourceIndex],
        std::move(childRequestedOutputType),
        childScanSpec,
        childPath(path, fieldName));
    names.emplace_back(fieldName.data(), fieldName.size());
    types.push_back(childInfo->veloxType);
    target->children.push_back(std::move(childInfo));
    target->childSourceIndices.push_back(sourceIndex);
  };

  // Translates a ScanSpec child into read schema.
  // (constants are projected later)
  auto addScanSpecField = [&](const common::ScanSpec& childSpec) {
    const auto childRequestedOutputType =
        findRequestedChildType(requestedRowType, childSpec.fieldName());
    if (childSpec.isConstant()) {
      if (childRequestedOutputType) {
        const auto fieldPath = childPath(path, childSpec.fieldName());
        VELOX_USER_CHECK(
            *childSpec.constantValue()->type() == *childRequestedOutputType,
            "Avro scan spec constant at '{}' has type {}, but requested type expects {}.",
            fieldPath,
            childSpec.constantValue()->type()->toString(),
            childRequestedOutputType->toString());
      }
      return;
    }

    if (requestedRowType) {
      VELOX_USER_CHECK(
          childRequestedOutputType,
          "Avro scan spec reads field '{}' at '{}', but requested type {} "
          "does not contain it.",
          childSpec.fieldName(),
          path,
          requestedOutputType->toString());
    }
    addFileBackedChild(
        childSpec.fieldName(), childRequestedOutputType, &childSpec);
  };

  if (!scanSpec) {
    for (size_t i = 0; i < requestedRowType->size(); ++i) {
      const auto& fieldName = requestedRowType->nameOf(i);
      addFileBackedChild(fieldName, requestedRowType->childAt(i), nullptr);
    }
  } else {
    // Add fields required by ScanSpec, including filter-only fields. When
    // requestedType is present, it must cover all file-backed read paths.
    for (const auto& childSpec : scanSpec->children()) {
      addScanSpecField(*childSpec);
    }
  }

  target->fieldNames = names;
  target->veloxType = ROW(std::move(names), std::move(types));
  return target;
}

std::shared_ptr<AvroTypeInfo> buildReadTypeInfo(
    const AvroTypeInfo& source,
    TypePtr requestedOutputType,
    const common::ScanSpec* scanSpec,
    const std::string& path) {
  // This path is already selected by its parent.
  VELOX_DCHECK(requestedOutputType || scanSpec);
  if (source.unionKind == AvroUnionKind::kNumericPromotion) {
    if (requestedOutputType) {
      checkCompatibleRequestedType(
          *requestedOutputType == *source.veloxType,
          source,
          requestedOutputType,
          path);
    }
    return std::make_shared<AvroTypeInfo>(source);
  }

  if (source.unionKind == AvroUnionKind::kStruct) {
    return buildReadRowTypeInfo(
        source, std::move(requestedOutputType), scanSpec, path);
  }

  if (source.avroType == ::avro::Type::AVRO_RECORD) {
    return buildReadRowTypeInfo(
        source, std::move(requestedOutputType), scanSpec, path);
  }

  auto target = std::make_shared<AvroTypeInfo>(source);
  switch (source.avroType) {
    case ::avro::Type::AVRO_ARRAY: {
      TypePtr requestedElementOutputType;
      if (requestedOutputType) {
        checkCompatibleRequestedType(
            requestedOutputType->isArray(), source, requestedOutputType, path);
        requestedElementOutputType =
            requestedOutputType->asArray().elementType();
      }
      const common::ScanSpec* elementSpec = nullptr;
      if (scanSpec) {
        elementSpec =
            scanSpec->childByName(common::ScanSpec::kArrayElementsFieldName);
      }
      if (!requestedElementOutputType && !elementSpec) {
        VELOX_DCHECK(scanSpec);
        // ScanSpec selects the entire array, so there are no element-level
        // constraints to apply.
        return target;
      }

      target->children = {buildReadTypeInfo(
          *source.children.front(),
          std::move(requestedElementOutputType),
          elementSpec,
          path + "[]")};
      target->veloxType = ARRAY(target->children.front()->veloxType);
      return target;
    }
    case ::avro::Type::AVRO_MAP: {
      TypePtr requestedValueOutputType;
      if (requestedOutputType) {
        checkCompatibleRequestedType(
            requestedOutputType->isMap(), source, requestedOutputType, path);
        checkCompatibleRequestedType(
            *requestedOutputType->asMap().keyType() == *VARCHAR(),
            source,
            requestedOutputType,
            childPath(path, "key"));
        requestedValueOutputType = requestedOutputType->asMap().valueType();
      }
      const common::ScanSpec* valueSpec = nullptr;
      if (scanSpec) {
        valueSpec =
            scanSpec->childByName(common::ScanSpec::kMapValuesFieldName);
      }
      if (!requestedValueOutputType && !valueSpec) {
        VELOX_DCHECK(scanSpec);
        // ScanSpec selects the entire map, so there are no element-level
        // constraints to apply.
        return target;
      }

      target->children = {buildReadTypeInfo(
          *source.children.front(),
          std::move(requestedValueOutputType),
          valueSpec,
          childPath(path, "value"))};
      target->veloxType = MAP(VARCHAR(), target->children.front()->veloxType);
      return target;
    }
    default:
      if (!requestedOutputType) {
        return target;
      }

      validateRequestedLeafTypeForRead(source, requestedOutputType, path);
      if (source.avroType == ::avro::Type::AVRO_NULL) {
        target->veloxType = requestedOutputType;
        return target;
      }
      if (source.logicalType == AvroLogicalType::kDecimal) {
        const auto [precision, scale] =
            getDecimalPrecisionScale(*requestedOutputType);
        target->veloxType = requestedOutputType;
        target->decimalPrecision = precision;
        target->decimalScale = scale;
      }
      return target;
  }
}

} // namespace

std::shared_ptr<const AvroReadSchema> buildAvroReadSchema(
    const std::shared_ptr<const AvroTypeInfo>& fileTypeInfo,
    const RowTypePtr& fileRowType,
    const dwio::common::RowReaderOptions& options) {
  const auto& requestedType = options.requestedType();
  const auto& scanSpec = options.scanSpec();
  if (scanSpec) {
    rejectUnsupportedScanSpecFeaturesForAvro(
        *scanSpec, "root", /*hasTransformFallback=*/false);
  }
  if (!requestedType && !scanSpec) {
    return std::make_shared<AvroReadSchema>(fileTypeInfo, fileRowType);
  }

  auto typeInfo =
      buildReadTypeInfo(*fileTypeInfo, requestedType, scanSpec.get(), "root");
  auto rowType = std::static_pointer_cast<const RowType>(typeInfo->veloxType);
  return std::make_shared<AvroReadSchema>(std::move(typeInfo), rowType);
}

} // namespace facebook::velox::avro
