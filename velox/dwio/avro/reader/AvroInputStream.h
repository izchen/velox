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

#include <avro/Stream.hh>

#include <memory>

namespace facebook::velox::memory {
class MemoryPool;
} // namespace facebook::velox::memory

namespace facebook::velox::dwio::common {
class ReadFileInputStream;
} // namespace facebook::velox::dwio::common

namespace facebook::velox::avro {

/// Adapts a Velox file input stream to an avro-cpp seekable input stream.
::avro::SeekableInputStreamPtr createAvroInputStream(
    std::shared_ptr<dwio::common::ReadFileInputStream> input,
    uint64_t start,
    uint64_t length,
    memory::MemoryPool& pool);

} // namespace facebook::velox::avro
