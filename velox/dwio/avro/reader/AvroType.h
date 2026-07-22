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

#pragma once

#include <avro/Types.hh>

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "velox/type/Type.h"

namespace facebook::velox::avro {

/// Identifies the Avro logical type applied to a physical type.
enum class AvroLogicalType {
  kNone,
  kDate,
  kTimeMillis,
  kTimeMicros,
  kTimestampMillis,
  kTimestampMicros,
  kTimestampNanos,
  kDecimal,
  kUuid,
};

/// Identifies how an Avro union is represented in Velox.
enum class AvroUnionKind {
  kNone,
  kSimple,
  kNumericPromotion,
  kStruct,
};

/// Describes the Velox representation and decoding metadata for an Avro type.
struct AvroTypeInfo {
  /// Physical type of the corresponding Avro schema node.
  ::avro::Type avroType{::avro::Type::AVRO_NULL};

  /// Velox type produced for the schema node.
  TypePtr veloxType;

  /// True when the schema node accepts null values.
  bool nullable{false};

  /// Names of record fields or materialized union members.
  std::vector<std::string> fieldNames;

  /// Metadata for materialized child values.
  std::vector<std::shared_ptr<AvroTypeInfo>> children;

  /// Maps materialized children to their positions in the source datum.
  std::vector<size_t> childSourceIndices;

  /// Logical interpretation applied while decoding the physical value.
  AvroLogicalType logicalType{AvroLogicalType::kNone};

  /// Precision used for decimal values.
  uint8_t decimalPrecision{0};

  /// Scale used for decimal values.
  uint8_t decimalScale{0};

  /// Representation selected for an Avro union.
  AvroUnionKind unionKind{AvroUnionKind::kNone};

  /// Position of the null branch in the source union, if present.
  std::optional<size_t> nullUnionBranchIndex;
};

} // namespace facebook::velox::avro
