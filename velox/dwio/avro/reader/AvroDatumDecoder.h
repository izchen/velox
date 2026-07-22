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

#include "velox/dwio/avro/reader/AvroType.h"

namespace avro {
class GenericDatum;
} // namespace avro

namespace facebook::velox::exec {
class GenericWriter;
} // namespace facebook::velox::exec

namespace facebook::velox::avro {

/// Decodes an Avro datum into the current position of a Velox vector writer.
void decodeAvroDatum(
    const AvroTypeInfo& typeInfo,
    const ::avro::GenericDatum& datum,
    exec::GenericWriter& writer);

} // namespace facebook::velox::avro
