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

#include <utility>

#include "velox/dwio/avro/reader/AvroType.h"

namespace facebook::velox::dwio::common {
class RowReaderOptions;
} // namespace facebook::velox::dwio::common

namespace facebook::velox::avro {

/// Describes the decoded row shape needed by an Avro row reader.
struct AvroReadSchema {
  /// Creates a read schema from decoding metadata and its Velox row type.
  AvroReadSchema(
      std::shared_ptr<const AvroTypeInfo> typeInfoIn,
      RowTypePtr rowTypeIn)
      : typeInfo(std::move(typeInfoIn)), rowType(std::move(rowTypeIn)) {}

  /// Decoding metadata projected to the fields required for this read.
  std::shared_ptr<const AvroTypeInfo> typeInfo;

  /// Velox row type produced before post-read ScanSpec processing.
  RowTypePtr rowType;
};

/// Builds the decoded row shape required by requestedType and ScanSpec.
std::shared_ptr<const AvroReadSchema> buildAvroReadSchema(
    const std::shared_ptr<const AvroTypeInfo>& fileTypeInfo,
    const RowTypePtr& fileRowType,
    const dwio::common::RowReaderOptions& options);

} // namespace facebook::velox::avro
