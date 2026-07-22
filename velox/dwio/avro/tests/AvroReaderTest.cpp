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

#include "velox/dwio/avro/tests/AvroReaderTestBase.h"

#include <avro/Compiler.hh>
#include <avro/DataFile.hh>
#include <avro/Generic.hh>

#include <limits>
#include <string_view>
#include <utility>

#include "velox/common/base/VeloxException.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/ReaderFactory.h"

using namespace facebook::velox::test;

namespace facebook::velox::avro {
namespace {
using namespace facebook::velox::common::testutil;

// Tracks reads that start at the beginning of an in-memory file.
class FileStartReadTrackingFile : public InMemoryReadFile {
 public:
  using InMemoryReadFile::InMemoryReadFile;

  std::string_view pread(
      uint64_t offset,
      uint64_t length,
      void* buffer,
      const FileIoContext& context = {}) const override {
    if (offset == 0) {
      ++numReadsFromFileStart_;
    }
    return InMemoryReadFile::pread(offset, length, buffer, context);
  }

  uint64_t numReadsFromFileStart() const {
    return numReadsFromFileStart_;
  }

 private:
  mutable uint64_t numReadsFromFileStart_{0};
};

// Tests Avro reader orchestration, splits, and batching.
class AvroReaderTest : public AvroReaderTestBase {};

TEST_F(AvroReaderTest, rejectsNonRecordRootSchema) {
  const std::string schemaJson = R"JSON("int")JSON";
  auto filePath = writeAvroFile(
      schemaJson,
      [](auto& /*writer*/, const ::avro::ValidSchema& /*schema*/) {});

  EXPECT_THROW(createReader(filePath), VeloxRuntimeError);
}

TEST_F(AvroReaderTest, createsMultipleRowReaders) {
  const auto filePath = writeAllTypesRecord();
  auto reader = createReader(filePath);

  auto firstRowReader = createRowReader(*reader);
  auto secondRowReader = createRowReader(*reader);

  VectorPtr firstResult;
  ASSERT_EQ(firstRowReader->next(1, firstResult), 1);
  VectorPtr secondResult;
  ASSERT_EQ(secondRowReader->next(1, secondResult), 1);
  assertEqualVectors(firstResult, secondResult);
}

TEST_F(AvroReaderTest, reportsAtEndForEmptyFile) {
  const std::string schemaJson = R"JSON(
    {
      "type": "record",
      "name": "EmptyFileRecord",
      "fields": [
        {"name": "value", "type": "int"}
      ]
    })JSON";
  const auto filePath = writeAvroFile(schemaJson, [](auto&, const auto&) {});
  auto reader = createReader(filePath);
  auto rowReader = createRowReader(*reader);

  EXPECT_EQ(rowReader->nextRowNumber(), dwio::common::RowReader::kAtEnd);
  EXPECT_EQ(rowReader->nextReadSize(1), dwio::common::RowReader::kAtEnd);
  VectorPtr result;
  EXPECT_EQ(rowReader->next(1, result), 0);
}

TEST_F(AvroReaderTest, reportsAtEndAfterExactRead) {
  const auto filePath = writeRequestedTypeRecord();
  auto reader = createReader(filePath);
  auto rowReader = createRowReader(*reader);

  VectorPtr result;
  ASSERT_EQ(rowReader->next(2, result), 2);
  EXPECT_EQ(rowReader->nextRowNumber(), dwio::common::RowReader::kAtEnd);
  EXPECT_EQ(rowReader->nextReadSize(2), dwio::common::RowReader::kAtEnd);
}

TEST_F(AvroReaderTest, nextRowNumberIsFileAbsoluteForNonZeroSplit) {
  const auto [filePath, splitOffset] = writeSplitRowNumberRecord();

  ASSERT_GT(splitOffset, 0);
  auto reader = createReader(filePath);
  dwio::common::RowReaderOptions rowOptions;
  rowOptions.range(splitOffset, std::numeric_limits<uint64_t>::max());
  auto rowReader = createRowReader(*reader, rowOptions);

  EXPECT_EQ(rowReader->nextRowNumber(), 1);

  VectorPtr result;
  ASSERT_EQ(rowReader->next(1, result), 1);
  auto expected = makeRowVector({makeFlatVector<int32_t>({20})});
  assertEqualVectors(expected, result);
  EXPECT_EQ(rowReader->nextRowNumber(), 2);
}

TEST_F(AvroReaderTest, resolvesNonZeroSplitRowNumberLazily) {
  const auto [filePath, splitOffset] = writeSplitRowNumberRecord();

  ASSERT_GT(splitOffset, 0);
  LocalReadFile localFile(filePath->getPath());
  std::string fileData(localFile.size(), '\0');
  localFile.pread(0, fileData.size(), fileData.data());
  auto readTrackingFile =
      std::make_shared<FileStartReadTrackingFile>(std::move(fileData));
  auto input =
      std::make_unique<dwio::common::BufferedInput>(readTrackingFile, *pool());
  auto factory = dwio::common::getReaderFactory(dwio::common::FileFormat::AVRO);
  auto reader = factory->createReader(
      std::move(input), dwio::common::ReaderOptions{pool()});

  const auto numReadsFromFileStartBeforeRowReader =
      readTrackingFile->numReadsFromFileStart();
  dwio::common::RowReaderOptions rowOptions;
  rowOptions.range(splitOffset, std::numeric_limits<uint64_t>::max());
  auto rowReader = createRowReader(*reader, rowOptions);

  // Constructing the row reader reads one header without scanning the prefix.
  EXPECT_EQ(
      readTrackingFile->numReadsFromFileStart(),
      numReadsFromFileStartBeforeRowReader + 1);

  // Reading data does not resolve the split's absolute starting row.
  VectorPtr result;
  ASSERT_EQ(rowReader->next(1, result), 1);
  EXPECT_EQ(
      readTrackingFile->numReadsFromFileStart(),
      numReadsFromFileStartBeforeRowReader + 1);

  // The first row number request resolves the split's absolute starting row.
  rowReader->nextRowNumber();
  EXPECT_EQ(
      readTrackingFile->numReadsFromFileStart(),
      numReadsFromFileStartBeforeRowReader + 2);

  // Later row number requests reuse the resolved starting row.
  rowReader->nextRowNumber();
  EXPECT_EQ(
      readTrackingFile->numReadsFromFileStart(),
      numReadsFromFileStartBeforeRowReader + 2);
}

TEST_F(AvroReaderTest, rejectsRowNumberColumnInfo) {
  auto reader = createReader(writeRequestedTypeRecord());
  dwio::common::RowNumberColumnInfo rowNumberColumnInfo;
  rowNumberColumnInfo.insertPosition = 0;
  rowNumberColumnInfo.name = "$row_number";
  dwio::common::RowReaderOptions rowOptions;
  rowOptions.setRowNumberColumnInfo(rowNumberColumnInfo);
  VELOX_ASSERT_THROW_CODE(
      createRowReader(*reader, rowOptions),
      error_code::kUnsupported,
      "Avro reader does not support implicit row number columns");
}

TEST_F(AvroReaderTest, scanBatchBytesRespected) {
  const std::string schemaJson = R"JSON(
    {
      "type": "record",
      "name": "BatchRecord",
      "fields": [
        {"name": "index", "type": "int"}
      ]
    })JSON";
  auto filePath = writeAvroFile(
      schemaJson, [](auto& writer, const ::avro::ValidSchema& schema) {
        ::avro::GenericDatum datum(schema.root());
        auto& record = datum.value<::avro::GenericRecord>();
        for (int i = 0; i < 20; ++i) {
          record.fieldAt(0).value<int>() = i;
          writer.write(datum);
        }
      });

  auto reader = createReader(filePath);
  dwio::common::RowReaderOptions rowOptions;
  rowOptions.setSerdeParameters({{"avro.scan.batch.bytes", "1"}});
  auto rowReader = createRowReader(*reader, rowOptions);
  VectorPtr firstBatch;
  ASSERT_EQ(rowReader->next(10, firstBatch), 10);
  VectorPtr secondBatch;
  ASSERT_EQ(rowReader->next(10, secondBatch), 1);
  VectorPtr thirdBatch;
  ASSERT_EQ(rowReader->next(10, thirdBatch), 1);

  auto expected = makeRowVector({makeFlatVector<int>({11})});
  assertEqualVectors(expected, thirdBatch);
}

} // namespace
} // namespace facebook::velox::avro
