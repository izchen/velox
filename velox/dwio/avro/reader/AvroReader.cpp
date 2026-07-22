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
#include <avro/Generic.hh>
#include <avro/Schema.hh>
#include <limits>

#include "velox/dwio/avro/reader/AvroDatumDecoder.h"
#include "velox/dwio/avro/reader/AvroInputStream.h"
#include "velox/dwio/avro/reader/AvroSchemaConverter.h"
#include "velox/dwio/avro/reader/AvroType.h"
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

  auto stream =
      createAvroInputStream(readFileInput, 0, length, options.memoryPool());
  ::avro::DataFileReader<::avro::GenericDatum> avroReader(std::move(stream));
  auto avroSchema = avroReader.readerSchema();

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

AvroRowReader::AvroRowReader(
    std::shared_ptr<AvroFileContents> contents,
    const dwio::common::RowReaderOptions& /*options*/)
    : contents_(std::move(contents)),
      reader_(createAvroDataFileReader(*contents_)),
      datum_(std::make_unique<::avro::GenericDatum>(reader_->readerSchema())),
      atEnd_(reader_->pastSync(kMaxSafeAvroReaderPosition)),
      numRowsConsumed_(0) {}

int64_t AvroRowReader::nextRowNumber() {
  return atEnd_ ? kAtEnd : static_cast<int64_t>(numRowsConsumed_);
}

std::optional<size_t> AvroRowReader::estimatedRowSize() const {
  return std::nullopt;
}

int64_t AvroRowReader::nextReadSize(const uint64_t size) {
  if (atEnd_) {
    return kAtEnd;
  }
  return static_cast<int64_t>(size);
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
    if (!reader_->read(*datum_)) {
      atEnd_ = true;
      break;
    }
    writer.setOffset(numRead);

    decodeAvroDatum(*rootInfo, *datum_, writer.current());
    writer.commit(true);
    ++numRead;
  }
  numRowsConsumed_ += numRead;
  if (!atEnd_ && reader_->pastSync(kMaxSafeAvroReaderPosition)) {
    atEnd_ = true;
  }
  writer.finish();
  rowVector->resize(numRead);
  return numRead;
}

} // namespace facebook::velox::avro
