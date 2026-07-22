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

#include <algorithm>
#include <avro/Compiler.hh>
#include <avro/DataFile.hh>
#include <avro/Generic.hh>
#include <avro/Schema.hh>
#include <limits>
#include <sstream>

#include "velox/dwio/avro/reader/AvroDatumDecoder.h"
#include "velox/dwio/avro/reader/AvroInputStream.h"
#include "velox/dwio/avro/reader/AvroSchemaConverter.h"
#include "velox/dwio/avro/reader/AvroType.h"
#include "velox/dwio/common/Options.h"
#include "velox/expression/VectorWriters.h"

namespace facebook::velox::avro {

namespace {
using dwio::common::BufferedInput;
using dwio::common::ReaderOptions;
using dwio::common::TypeWithId;

// ::avro::DataFileReaderBase::pastSync(position) internally evaluates
// `position + SyncSize`. To avoid signed int64 overflow (UB),
// the maximum safe value for `position` is
// std::numeric_limits<int64_t>::max() - SyncSize.
constexpr int64_t kMaxSafeAvroReaderPosition =
    std::numeric_limits<int64_t>::max() - ::avro::SyncSize;

constexpr std::string_view kAvroScanBatchBytesKey = "avro.scan.batch.bytes";
constexpr uint64_t kDefaultAvroScanBatchBytes = 100UL << 20; // 100MB

uint64_t loadAvroScanBatchBytes(const dwio::common::RowReaderOptions& options) {
  const auto& params = options.serdeParameters();
  const auto it = params.find(std::string(kAvroScanBatchBytesKey));
  if (it == params.end() || it->second.empty()) {
    return kDefaultAvroScanBatchBytes;
  }

  uint64_t bytes = 0;
  try {
    bytes = folly::to<uint64_t>(it->second);
  } catch (const folly::ConversionError& e) {
    VELOX_USER_FAIL(
        "Invalid value for '{}': '{}'. Details: {}.",
        kAvroScanBatchBytesKey,
        it->second,
        e.what());
  }
  VELOX_USER_CHECK_GT(
      bytes,
      0,
      "Invalid value for '{}': '{}'. Expected a positive integer number of bytes.",
      kAvroScanBatchBytesKey,
      it->second);
  return bytes;
}

} // namespace

struct AvroFileContents {
  AvroFileContents(
      std::shared_ptr<AvroTypeInfo> typeInfoIn,
      std::shared_ptr<dwio::common::ReadFileInputStream> readFileInputIn,
      uint64_t lengthIn,
      ::avro::ValidSchema avroSchemaIn,
      std::shared_ptr<const RowType> rowTypeIn,
      std::shared_ptr<const TypeWithId> schemaWithIdIn,
      memory::MemoryPool& poolIn)
      : typeInfo(std::move(typeInfoIn)),
        readFileInput(std::move(readFileInputIn)),
        length(lengthIn),
        avroSchema(std::move(avroSchemaIn)),
        rowType(std::move(rowTypeIn)),
        schemaWithId(std::move(schemaWithIdIn)),
        pool(poolIn) {}

  std::shared_ptr<AvroTypeInfo> typeInfo;
  std::shared_ptr<dwio::common::ReadFileInputStream> readFileInput;
  uint64_t length;
  ::avro::ValidSchema avroSchema;
  std::shared_ptr<const RowType> rowType;
  std::shared_ptr<const TypeWithId> schemaWithId;
  memory::MemoryPool& pool;
};

std::unique_ptr<::avro::DataFileReader<::avro::GenericDatum>>
createAvroDataFileReader(const AvroFileContents& contents) {
  auto stream = createAvroInputStream(
      contents.readFileInput, 0, contents.length, contents.pool);
  // TODO: Generate a projected reader schema from requestedType and ScanSpec.
  // avro-cpp does not provide a convenient public API for cloning and pruning
  // schemas while preserving metadata. Passing the projected schema to
  // avro-cpp’s resolving decoder would avoid materializing unrequested fields.
  return std::make_unique<::avro::DataFileReader<::avro::GenericDatum>>(
      std::move(stream), contents.avroSchema);
}

AvroReader::AvroReader(
    const std::unique_ptr<BufferedInput>& input,
    const ReaderOptions& options) {
  auto readFileInput = input->getInputStream();
  auto length = readFileInput->getLength();

  // Reader schema precedence: user-configured `avro.schema.literal`,
  // otherwise the schema embedded in the Avro file.
  ::avro::ValidSchema avroSchema;
  if (options.serDeOptions().avroSchema.has_value()) {
    std::istringstream schemaStream(options.serDeOptions().avroSchema.value());
    try {
      ::avro::compileJsonSchema(schemaStream, avroSchema);
    } catch (const std::exception& e) {
      VELOX_USER_FAIL(
          "Failed to parse Avro schema override from '{}': {}",
          options.serDeOptions().kAvroSchema,
          e.what());
    }
  } else {
    auto stream =
        createAvroInputStream(readFileInput, 0, length, options.memoryPool());
    ::avro::DataFileReader<::avro::GenericDatum> avroReader(std::move(stream));
    avroSchema = avroReader.readerSchema();
  }

  auto avroSchemaRoot = avroSchema.root();
  VELOX_CHECK_EQ(
      avroSchemaRoot->type(),
      ::avro::Type::AVRO_RECORD,
      "Avro root schema must be of type RECORD, but got Avro type enum value {}. "
      "Please refer to avro::Type in "
      "https://github.com/apache/avro/blob/main/lang/c%2B%2B/include/avro/Types.hh "
      "to find the corresponding type.",
      static_cast<int>(avroSchemaRoot->type()));
  auto typeInfo = buildTypeInfo(avroSchemaRoot, options);
  auto rowType = std::static_pointer_cast<const RowType>(typeInfo->veloxType);
  std::shared_ptr<const TypeWithId> schemaWithId = TypeWithId::create(rowType);

  contents_ = std::make_shared<AvroFileContents>(
      std::move(typeInfo),
      std::move(readFileInput),
      length,
      std::move(avroSchema),
      std::move(rowType),
      std::move(schemaWithId),
      options.memoryPool());
}

std::optional<uint64_t> AvroReader::numberOfRows() const {
  return std::nullopt;
}

std::unique_ptr<dwio::common::ColumnStatistics> AvroReader::columnStatistics(
    uint32_t /*index*/) const {
  return nullptr;
}

const RowTypePtr& AvroReader::rowType() const {
  return contents_->rowType;
}

const std::shared_ptr<const TypeWithId>& AvroReader::typeWithId() const {
  return contents_->schemaWithId;
}

std::unique_ptr<dwio::common::RowReader> AvroReader::createRowReader(
    const dwio::common::RowReaderOptions& options) const {
  return std::make_unique<AvroRowReader>(contents_, options);
}

namespace {
// Avro-cpp does not expose OCF block metadata such as row counts, so derive an
// absolute split row number on demand by scanning from the file start.
// TODO: Use block metadata when avro-cpp exposes it.
uint64_t countRowsBeforeBlock(
    const AvroFileContents& contents,
    int64_t blockStart) {
  auto reader = createAvroDataFileReader(contents);
  ::avro::GenericDatum datum(reader->readerSchema());
  uint64_t numRows{0};
  while (reader->read(datum)) {
    if (reader->previousSync() >= blockStart) {
      break;
    }
    ++numRows;
  }
  return numRows;
}

} // namespace

AvroRowReader::AvroRowReader(
    std::shared_ptr<AvroFileContents> contents,
    const dwio::common::RowReaderOptions& options)
    : contents_(std::move(contents)),
      reader_(createAvroDataFileReader(*contents_)),
      datum_(std::make_unique<::avro::GenericDatum>(reader_->readerSchema())),
      splitLimit_(
          options.limit() >= static_cast<uint64_t>(kMaxSafeAvroReaderPosition)
              ? kMaxSafeAvroReaderPosition
              : static_cast<int64_t>(options.limit())),
      avroScanBatchBytes_(loadAvroScanBatchBytes(options)),
      atEnd_(false),
      splitStartPosition_(0),
      splitStartRowNumber_(0),
      numRowsConsumedInSplit_(0),
      rowSizeSampleCount_(0),
      rowSizeSampleBytes_(0) {
  if (options.rowNumberColumnInfo().has_value()) {
    // TODO: Support implicit row number columns using currentFileRowNumber().
    VELOX_UNSUPPORTED(
        "Avro reader does not support implicit row number columns.");
  }
  if (options.offset() > 0) {
    reader_->sync(static_cast<int64_t>(options.offset()));
    if (reader_->pastSync(splitLimit_)) {
      atEnd_ = true;
    } else {
      splitStartPosition_ = reader_->previousSync();
      splitStartRowNumber_.reset();
    }
  }
  uint64_t skip = options.skipRows();
  while (skip > 0) {
    if (reader_->pastSync(splitLimit_) || !reader_->read(*datum_)) {
      atEnd_ = true;
      break;
    }
    ++numRowsConsumedInSplit_;
    --skip;
  }
  if (skip > 0) {
    atEnd_ = true;
  }
  if (!atEnd_ && reader_->pastSync(splitLimit_)) {
    atEnd_ = true;
  }
}

uint64_t AvroRowReader::currentFileRowNumber() {
  if (!splitStartRowNumber_.has_value()) {
    splitStartRowNumber_ =
        countRowsBeforeBlock(*contents_, splitStartPosition_);
  }
  return splitStartRowNumber_.value() + numRowsConsumedInSplit_;
}

int64_t AvroRowReader::nextRowNumber() {
  return atEnd_ ? kAtEnd : static_cast<int64_t>(currentFileRowNumber());
}

std::optional<size_t> AvroRowReader::estimatedRowSize() const {
  if (rowSizeSampleCount_ == 0 || rowSizeSampleBytes_ == 0) {
    return std::nullopt;
  }
  return std::max<size_t>(1, rowSizeSampleBytes_ / rowSizeSampleCount_);
}

int64_t AvroRowReader::nextReadSize(const uint64_t size) {
  if (atEnd_) {
    return kAtEnd;
  }
  const auto rowSize = estimatedRowSize();
  if (!rowSize.has_value()) {
    return static_cast<int64_t>(size);
  }
  const auto rowsByBytes =
      std::max<uint64_t>(1, avroScanBatchBytes_ / rowSize.value());

  return static_cast<int64_t>(std::min<uint64_t>(size, rowsByBytes));
}

void AvroRowReader::updateRuntimeStats(
    dwio::common::RuntimeStatistics& /*stats*/) const {}

void AvroRowReader::resetFilterCaches() {
  // No-op because the basic Avro reader does not cache filter results.
}

uint64_t AvroRowReader::next(
    const uint64_t size,
    VectorPtr& result,
    const dwio::common::Mutation* mutation) {
  if (mutation != nullptr) {
    VELOX_UNSUPPORTED("Avro reader does not support mutations.");
  }
  if (atEnd_ || size == 0) {
    return 0;
  }
  const auto rowsToRead = nextReadSize(size);
  SelectivityVector rows(rowsToRead);
  const auto& rowType = contents_->rowType;
  if (result && !result->type()->equivalent(*rowType)) {
    result.reset();
  }
  BaseVector::ensureWritable(rows, rowType, &contents_->pool, result);
  auto rowVector = std::static_pointer_cast<RowVector>(result);
  rowVector->resize(rowsToRead);
  exec::VectorWriter<Any> writer;
  writer.init(*rowVector);
  const auto* rootInfo = contents_->typeInfo.get();

  vector_size_t numRead = 0;
  while (numRead < rowsToRead) {
    if (reader_->pastSync(splitLimit_) || !reader_->read(*datum_)) {
      atEnd_ = true;
      break;
    }
    writer.setOffset(numRead);

    decodeAvroDatum(*rootInfo, *datum_, writer.current());
    writer.commit(true);
    ++numRead;
  }
  numRowsConsumedInSplit_ += numRead;
  if (!atEnd_ && reader_->pastSync(splitLimit_)) {
    atEnd_ = true;
  }
  writer.finish();
  rowVector->resize(numRead);
  if (numRead > 0) {
    const auto batchBytes = rowVector->estimateFlatSize();
    rowSizeSampleCount_ += numRead;
    rowSizeSampleBytes_ += batchBytes;
  }
  return numRead;
}

} // namespace facebook::velox::avro
