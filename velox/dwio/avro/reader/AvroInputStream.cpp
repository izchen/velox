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

#include "velox/dwio/avro/reader/AvroInputStream.h"

#include <utility>
#include <vector>

#include "velox/common/base/Exceptions.h"
#include "velox/dwio/common/InputStream.h"
#include "velox/dwio/common/PositionProvider.h"
#include "velox/dwio/common/SeekableInputStream.h"

namespace facebook::velox::avro {
namespace {

// Adapts dwio::common::ReadFileInputStream to avro-cpp's
// ::avro::SeekableInputStream, with support for multiple backup() calls
// after a single next().
class ReadFileAvroInputStream : public ::avro::SeekableInputStream {
 public:
  ReadFileAvroInputStream(
      std::shared_ptr<dwio::common::ReadFileInputStream> input,
      uint64_t start,
      uint64_t length,
      memory::MemoryPool& pool) {
    stream_ = std::make_unique<dwio::common::SeekableFileInputStream>(
        std::move(input),
        start,
        length,
        pool,
        dwio::common::LogType::FILE,
        input->getNaturalReadSize());
  }

  bool next(const uint8_t** data, size_t* len) override {
    const void* rawData = nullptr;
    int32_t size = 0;
    if (!stream_->Next(&rawData, &size)) {
      *data = nullptr;
      *len = 0;
      return false;
    }
    *data = static_cast<const uint8_t*>(rawData);
    *len = static_cast<size_t>(size);
    pushback_ = 0;
    return true;
  }

  void backup(size_t len) override {
    if (pushback_ > 0) {
      // SeekableFileInputStream allows BackUp() only once after each Next().
      // Reconsume the previous pushback before backing up the cumulative size.
      const void* rawData = nullptr;
      int32_t size = 0;
      VELOX_CHECK(
          stream_->Next(&rawData, &size),
          "Underlying stream failed to replay backed-up bytes.");
      VELOX_CHECK_EQ(
          static_cast<size_t>(size),
          pushback_,
          "Underlying stream replayed an unexpected number of backed-up "
          "bytes.");
    }
    pushback_ += len;
    stream_->BackUp(static_cast<int32_t>(pushback_));
  }

  void skip(size_t len) override {
    stream_->SkipInt64(static_cast<int64_t>(len));
    pushback_ = 0;
  }

  size_t byteCount() const override {
    return static_cast<size_t>(stream_->ByteCount());
  }

  void seek(int64_t position) override {
    const std::vector<uint64_t> positions{static_cast<uint64_t>(position)};
    dwio::common::PositionProvider provider(positions);
    stream_->seekToPosition(provider);
    pushback_ = 0;
  }

 private:
  std::unique_ptr<dwio::common::SeekableFileInputStream> stream_;
  size_t pushback_ = 0;
};

} // namespace

::avro::SeekableInputStreamPtr createAvroInputStream(
    std::shared_ptr<dwio::common::ReadFileInputStream> input,
    uint64_t start,
    uint64_t length,
    memory::MemoryPool& pool) {
  return std::make_unique<ReadFileAvroInputStream>(
      std::move(input), start, length, pool);
}

} // namespace facebook::velox::avro
