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

#include "velox/common/base/VeloxException.h"

using namespace facebook::velox::test;

namespace facebook::velox::avro {
namespace {

// Tests basic Avro reader orchestration.
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

} // namespace
} // namespace facebook::velox::avro
