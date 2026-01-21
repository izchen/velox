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

#include "velox/dwio/avro/reader/AvroReader.h"

#include <avro/Compiler.hh>
#include <avro/DataFile.hh>
#include <avro/Stream.hh>
#include <avro/Schema.hh>

namespace facebook::velox::avro {

namespace {
using dwio::common::BufferedInput;
using dwio::common::LogType;
using dwio::common::ReaderOptions;

// Adapts dwio::common::ReadFileInputStream to avro-cpp's ::avro::SeekableInputStream,
// with support for multiple backup() calls after a single next().
class ReadFileAvroInputStream : public ::avro::SeekableInputStream {
public:
  ReadFileAvroInputStream(
      std::shared_ptr<dwio::common::ReadFileInputStream> input,
      uint64_t start,
      uint64_t length,
      memory::MemoryPool& pool) {
    stream_ = std::make_unique<dwio::common::SeekableFileInputStream>(
        std::move(input), start, length, pool, LogType::FILE,
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
      const void* rawData = nullptr;
      int32_t size = 0;
      stream_->Next(&rawData, &size);
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
    std::vector<uint64_t> positions{static_cast<uint64_t>(position)};
    dwio::common::PositionProvider provider(positions);
    stream_->seekToPosition(provider);
    pushback_ = 0;
  }

private:
  std::unique_ptr<dwio::common::SeekableFileInputStream> stream_;
  size_t pushback_ = 0;
};
}

AvroReader::AvroReader(
    std::unique_ptr<BufferedInput> input,
    const ReaderOptions& options) {
  auto readFileInput = input->getInputStream();
  auto length = readFileInput->getLength();
  auto stream = std::make_unique<ReadFileAvroInputStream>(
      readFileInput, 0, length, options.memoryPool());


  // File row type precedence (highest to lowest):
  // - avro.schema.literal provided by the user
  // - options.fileSchema()
  // - schema embedded in the Avro file
  ::avro::ValidSchema avroSchema;
  if (options.serDeOptions().avroSchema.has_value()) {
    try {
      ::avro::compileJsonSchema(schemaStream, schema);
    } catch (const std::exception& e) {
      VELOX_USER_FAIL(
          "Failed to parse Avro schema override from '{}': {}",
          kAvroSchemaLiteralKey,
          e.what());
    }
  }



  options.fileSchema();

  // options.serDeOptions().escapeChar

  auto overrideSchema = loadOverrideSchema(options);
  std::unique_ptr<::avro::DataFileReader<::avro::GenericDatum>> reader;
  if (overrideSchema.has_value()) {
    reader = std::make_unique<::avro::DataFileReader<::avro::GenericDatum>>(
        std::move(stream), *overrideSchema);
    avroSchema = *overrideSchema;
  } else {
    reader = std::make_unique<::avro::DataFileReader<::avro::GenericDatum>>(
        std::move(stream));
    avroSchema = reader->readerSchema();
  }

  auto typeInfo = buildTypeInfo(schema.root(), options);
  contents_ = std::make_shared<AvroFileContents>(
      std::move(input),
      options,
      std::move(schema),
      std::move(typeInfo),
      std::move(reader),
      avroScanBatchBytes);
}

} // namespace facebook::velox::avro
