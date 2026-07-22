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

#include "velox/vector/BaseVector.h"

namespace facebook::velox::common {
class ScanSpec;
} // namespace facebook::velox::common

namespace facebook::velox::dwio::common {
struct Mutation;
} // namespace facebook::velox::dwio::common

namespace facebook::velox::avro {

/// Applies ScanSpec projection, filtering, transforms, and row-level mutation
/// to decoded Avro rows.
VectorPtr processAvroRows(
    const VectorPtr& input,
    const common::ScanSpec* scanSpec,
    const dwio::common::Mutation* mutation);

} // namespace facebook::velox::avro
