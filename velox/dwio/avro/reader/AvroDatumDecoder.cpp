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

#include "velox/dwio/avro/reader/AvroDatumDecoder.h"

#include <avro/Generic.hh>

#include <algorithm>
#include <optional>

#include "velox/expression/VectorWriters.h"

namespace facebook::velox::avro {
namespace {

using exec::GenericWriter;

struct ResolvedDatum {
  const AvroTypeInfo* info;
  const ::avro::GenericDatum* datum;
  std::optional<size_t> unionChildIndex;
};

void writeDecimal(
    const AvroTypeInfo& info,
    const uint8_t* data,
    size_t size,
    GenericWriter& writer) {
  // Decode Avro decimal bytes (big-endian, two's complement) into int128.
  int128_t unscaledValue = 0;
  if (size != 0) {
    VELOX_CHECK(
        size <= sizeof(int128_t),
        "Decimal value encoded with {} bytes exceeds supported precision.",
        size);

    uint128_t acc = 0;
    for (size_t i = 0; i < size; ++i) {
      acc = (acc << 8) | static_cast<uint128_t>(data[i]);
    }

    // Sign-extend negative values when encoded with fewer than 16 bytes.
    if ((data[0] & 0x80) != 0 && size < sizeof(int128_t)) {
      const uint32_t missingBits = static_cast<uint32_t>(128 - size * 8);
      acc |= (~static_cast<uint128_t>(0)) << (128 - missingBits);
    }

    unscaledValue = static_cast<int128_t>(acc);
  }

  VELOX_CHECK(
      DecimalUtil::valueInPrecisionRange<int128_t>(
          unscaledValue, info.decimalPrecision),
      "Decimal value {} exceeds precision {}.",
      unscaledValue,
      static_cast<int32_t>(info.decimalPrecision));

  // Write using the physical width required by the Velox decimal type.
  if (info.veloxType->isShortDecimal()) {
    writer.castTo<int64_t>() = static_cast<int64_t>(unscaledValue);
  } else {
    writer.castTo<int128_t>() = unscaledValue;
  }
}

std::optional<ResolvedDatum> resolveUnionAndNull(
    const AvroTypeInfo& info,
    const ::avro::GenericDatum& datum) {
  // For unions, datum.type() reflects the active branch,
  // matching both AVRO_NULL values and unions with NULL as the selected branch.
  if (datum.type() == ::avro::Type::AVRO_NULL) {
    VELOX_CHECK(
        info.nullable, "Encountered null value for non-nullable Avro schema.");
    return std::nullopt;
  }

  if (info.unionKind == AvroUnionKind::kNone) {
    // already validated during avro->velox schema mapping.
    VELOX_CHECK(
        !datum.isUnion(), "Encountered union datum without union schema.");
    return ResolvedDatum{&info, &datum, std::nullopt};
  }

  // In AvroTypeInfo, the AVRO_NULL union branch represents nullability
  // rather than a child type, so we remap the union branch index
  // accordingly.
  size_t childIndex = datum.unionBranch();
  if (info.nullable) {
    const auto nullIndex = info.nullUnionBranchIndex.value();
    if (childIndex > nullIndex) {
      childIndex -= 1;
    }
  }
  return ResolvedDatum{&info, &datum, childIndex};
}

void writeDatum(const ResolvedDatum& resolved, GenericWriter& writer) {
  const auto& resolvedInfo = *resolved.info;
  const auto& resolvedDatum = *resolved.datum;

  if (resolved.unionChildIndex.has_value()) {
    switch (resolvedInfo.unionKind) {
      case AvroUnionKind::kNumericPromotion: {
        const auto branchType = resolvedDatum.type();
        if (*resolvedInfo.veloxType == *BIGINT()) {
          int64_t value = 0;
          if (branchType == ::avro::Type::AVRO_INT) {
            value = static_cast<int64_t>(resolvedDatum.value<int32_t>());
          } else if (branchType == ::avro::Type::AVRO_LONG) {
            value = resolvedDatum.value<int64_t>();
          } else {
            // Unreachable: already validated during avro->velox schema mapping.
            VELOX_UNREACHABLE(
                "Unsupported Avro union branch {} for BIGINT promotion.",
                static_cast<int>(branchType));
          }
          writer.castTo<int64_t>() = value;
          return;
        }
        if (*resolvedInfo.veloxType == *DOUBLE()) {
          double value = 0;
          if (branchType == ::avro::Type::AVRO_FLOAT) {
            value = static_cast<double>(resolvedDatum.value<float>());
          } else if (branchType == ::avro::Type::AVRO_DOUBLE) {
            value = resolvedDatum.value<double>();
          } else {
            // Unreachable: already validated during avro->velox schema mapping.
            VELOX_UNREACHABLE(
                "Unsupported Avro union branch {} for DOUBLE promotion.",
                static_cast<int>(branchType));
          }
          writer.castTo<double>() = value;
          return;
        }
        // Unreachable: already validated during avro->velox schema mapping.
        VELOX_UNREACHABLE(
            "Unsupported numeric promotion target {}.",
            resolvedInfo.veloxType->toString());
      }
      case AvroUnionKind::kStruct: {
        const auto selectedSourceIndex = resolved.unionChildIndex.value();
        const auto selectedIt = std::find(
            resolvedInfo.childSourceIndices.begin(),
            resolvedInfo.childSourceIndices.end(),
            selectedSourceIndex);
        std::optional<size_t> selectedIndex;
        if (selectedIt != resolvedInfo.childSourceIndices.end()) {
          selectedIndex = static_cast<size_t>(
              selectedIt - resolvedInfo.childSourceIndices.begin());
        }
        auto& rowWriter = writer.castTo<DynamicRow>();
        for (size_t i = 0; i < resolvedInfo.children.size(); ++i) {
          if (selectedIndex.has_value() && i == selectedIndex.value()) {
            continue;
          }
          rowWriter.set_null_at(static_cast<int32_t>(i));
        }
        if (!selectedIndex.has_value()) {
          return;
        }
        auto& childWriter = rowWriter.get_writer_at(
            static_cast<int32_t>(selectedIndex.value()));
        ResolvedDatum childResolved{
            resolvedInfo.children[selectedIndex.value()].get(),
            resolved.datum,
            std::nullopt};
        writeDatum(childResolved, childWriter);
        return;
      }
      default:
        break;
    }
  }

  switch (resolvedDatum.type()) {
    case ::avro::Type::AVRO_BOOL:
      writer.castTo<bool>() = resolvedDatum.value<bool>();
      return;

    case ::avro::Type::AVRO_INT: {
      const auto value = resolvedDatum.value<int32_t>();
      switch (resolvedInfo.logicalType) {
        case AvroLogicalType::kDate:
          writer.castTo<int32_t>() = value;
          return;
        case AvroLogicalType::kTimeMillis:
          writer.castTo<int64_t>() = static_cast<int64_t>(value) * 1000;
          return;
        default:
          writer.castTo<int32_t>() = value;
          return;
      }
    }

    case ::avro::Type::AVRO_LONG: {
      const auto value = resolvedDatum.value<int64_t>();
      switch (resolvedInfo.logicalType) {
        case AvroLogicalType::kTimeMicros:
          writer.castTo<int64_t>() = value;
          return;
        case AvroLogicalType::kTimestampMillis:
          writer.castTo<Timestamp>() = Timestamp::fromMillis(value);
          return;
        case AvroLogicalType::kTimestampMicros:
          writer.castTo<Timestamp>() = Timestamp::fromMicros(value);
          return;
        case AvroLogicalType::kTimestampNanos:
          writer.castTo<Timestamp>() = Timestamp::fromNanos(value);
          return;
        default:
          writer.castTo<int64_t>() = value;
          return;
      }
    }

    case ::avro::Type::AVRO_FLOAT:
      writer.castTo<float>() = resolvedDatum.value<float>();
      return;

    case ::avro::Type::AVRO_DOUBLE:
      writer.castTo<double>() = resolvedDatum.value<double>();
      return;

    case ::avro::Type::AVRO_STRING: {
      auto& value = resolvedDatum.value<std::string>();
      writer.castTo<Varchar>().copy_from(value);
      return;
    }

    case ::avro::Type::AVRO_BYTES: {
      auto& value = resolvedDatum.value<std::vector<uint8_t>>();
      if (resolvedInfo.logicalType == AvroLogicalType::kDecimal) {
        writeDecimal(resolvedInfo, value.data(), value.size(), writer);
      } else {
        writer.castTo<Varbinary>().copy_from(value);
      }
      return;
    }

    case ::avro::Type::AVRO_FIXED: {
      const auto& fixed = resolvedDatum.value<::avro::GenericFixed>().value();
      if (resolvedInfo.logicalType == AvroLogicalType::kDecimal) {
        writeDecimal(resolvedInfo, fixed.data(), fixed.size(), writer);
      } else {
        writer.castTo<Varbinary>().copy_from(fixed);
      }
      return;
    }

    case ::avro::Type::AVRO_ENUM: {
      auto& value = resolvedDatum.value<::avro::GenericEnum>().symbol();
      writer.castTo<Varchar>().copy_from(value);
      return;
    }

    case ::avro::Type::AVRO_ARRAY: {
      auto& arrayWriter = writer.castTo<Array<Any>>();
      const auto& elements =
          resolvedDatum.value<::avro::GenericArray>().value();
      arrayWriter.reserve(static_cast<vector_size_t>(elements.size()));
      for (const auto& element : elements) {
        auto resolvedElement =
            resolveUnionAndNull(*resolvedInfo.children.front(), element);
        if (!resolvedElement.has_value()) {
          arrayWriter.add_null();
          continue;
        }

        auto& elementWriter = arrayWriter.add_item();
        writeDatum(*resolvedElement, elementWriter);
      }
      return;
    }

    case ::avro::Type::AVRO_MAP: {
      auto& mapWriter = writer.castTo<Map<Varchar, Any>>();
      const auto& entries = resolvedDatum.value<::avro::GenericMap>().value();
      mapWriter.reserve(static_cast<vector_size_t>(entries.size()));
      for (const auto& [key, valueDatum] : entries) {
        auto resolvedValue =
            resolveUnionAndNull(*resolvedInfo.children.front(), valueDatum);
        if (!resolvedValue.has_value()) {
          auto& keyWriter = mapWriter.add_null();
          keyWriter.copy_from(key);
          continue;
        }

        auto&& [keyWriter, valueWriter] = mapWriter.add_item();
        keyWriter.copy_from(key);
        writeDatum(*resolvedValue, valueWriter);
      }
      return;
    }

    case ::avro::Type::AVRO_RECORD: {
      auto& rowWriter = writer.castTo<DynamicRow>();
      const auto& record = resolvedDatum.value<::avro::GenericRecord>();
      for (size_t i = 0; i < resolvedInfo.children.size(); ++i) {
        const auto sourceIndex = resolvedInfo.childSourceIndices[i];
        auto resolvedField = resolveUnionAndNull(
            *resolvedInfo.children[i], record.fieldAt(sourceIndex));
        if (!resolvedField.has_value()) {
          rowWriter.set_null_at(static_cast<int32_t>(i));
          continue;
        }

        auto& childWriter = rowWriter.get_writer_at(i);
        writeDatum(*resolvedField, childWriter);
      }
      return;
    }

    default:
      // Unreachable: already validated during avro->velox schema mapping.
      VELOX_UNREACHABLE(
          "Unsupported Avro datum type reached at runtime (enum value = {}).",
          static_cast<int>(resolvedDatum.type()));
  }
}

} // namespace

void decodeAvroDatum(
    const AvroTypeInfo& typeInfo,
    const ::avro::GenericDatum& datum,
    exec::GenericWriter& writer) {
  const ResolvedDatum resolved{&typeInfo, &datum, std::nullopt};
  writeDatum(resolved, writer);
}

} // namespace facebook::velox::avro
