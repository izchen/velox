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

#include "velox/common/base/BitUtil.h"
#include "velox/common/base/VeloxException.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/testutil/TempFilePath.h"
#include "velox/dwio/common/Mutation.h"
#include "velox/type/Filter.h"
#include "velox/type/Type.h"

using namespace facebook::velox::test;

namespace facebook::velox::avro {
namespace {
using namespace facebook::velox::common::testutil;

// Tests Avro schema conversion through the reader interface.
class AvroSchemaConverterTest : public AvroReaderTestBase {};

TEST_F(AvroSchemaConverterTest, allTypesSchemaMapping) {
  const auto filePath = writeAllTypesRecord();
  auto reader = createReader(filePath);
  auto rowType = reader->rowType();

  ASSERT_EQ(rowType->size(), 24);
  EXPECT_EQ(rowType->nameOf(0), "nullCol");
  EXPECT_EQ(rowType->childAt(0)->kind(), TypeKind::UNKNOWN);
  EXPECT_EQ(rowType->childAt(1)->kind(), TypeKind::BOOLEAN);
  EXPECT_EQ(rowType->childAt(2)->kind(), TypeKind::INTEGER);
  EXPECT_EQ(rowType->childAt(3)->kind(), TypeKind::BIGINT);
  EXPECT_EQ(rowType->childAt(4)->kind(), TypeKind::REAL);
  EXPECT_EQ(rowType->childAt(5)->kind(), TypeKind::DOUBLE);
  EXPECT_EQ(rowType->childAt(6)->kind(), TypeKind::VARCHAR);
  EXPECT_EQ(rowType->childAt(7)->kind(), TypeKind::VARBINARY);
  EXPECT_EQ(rowType->childAt(8)->kind(), TypeKind::VARCHAR);
  EXPECT_EQ(rowType->childAt(9)->kind(), TypeKind::VARBINARY);
  EXPECT_EQ(rowType->childAt(10)->kind(), TypeKind::ARRAY);
  EXPECT_EQ(rowType->childAt(11)->kind(), TypeKind::MAP);
  EXPECT_EQ(rowType->childAt(12)->kind(), TypeKind::ROW);
  EXPECT_EQ(rowType->childAt(13)->kind(), TypeKind::INTEGER);
  EXPECT_TRUE(rowType->childAt(14)->isDate());
  EXPECT_TRUE(rowType->childAt(15)->isTime());
  EXPECT_TRUE(rowType->childAt(16)->isTime());
  EXPECT_EQ(rowType->childAt(17)->kind(), TypeKind::TIMESTAMP);
  EXPECT_EQ(rowType->childAt(18)->kind(), TypeKind::TIMESTAMP);
  EXPECT_EQ(rowType->childAt(19)->kind(), TypeKind::TIMESTAMP);
  EXPECT_EQ(rowType->childAt(20)->kind(), TypeKind::VARCHAR);
  EXPECT_EQ(rowType->childAt(21)->kind(), TypeKind::VARBINARY);
  ASSERT_TRUE(rowType->childAt(22)->isDecimal());
  const auto [bytesPrecision, bytesScale] =
      getDecimalPrecisionScale(*rowType->childAt(22));
  EXPECT_EQ(bytesPrecision, 9);
  EXPECT_EQ(bytesScale, 2);
  ASSERT_TRUE(rowType->childAt(23)->isDecimal());
  const auto [fixedPrecision, fixedScale] =
      getDecimalPrecisionScale(*rowType->childAt(23));
  EXPECT_EQ(fixedPrecision, 7);
  EXPECT_EQ(fixedScale, 3);
}

TEST_F(AvroSchemaConverterTest, unionMapping) {
  const auto filePath = writeUnionRecord();

  auto reader = createReader(filePath);
  auto rowType = reader->rowType();
  ASSERT_EQ(rowType->size(), 11);
  EXPECT_EQ(rowType->childAt(0)->kind(), TypeKind::VARCHAR);
  EXPECT_EQ(rowType->childAt(1)->kind(), TypeKind::UNKNOWN);
  EXPECT_EQ(rowType->childAt(2)->kind(), TypeKind::INTEGER);
  EXPECT_EQ(rowType->childAt(3)->kind(), TypeKind::BIGINT);
  EXPECT_EQ(rowType->childAt(4)->kind(), TypeKind::DOUBLE);
  EXPECT_EQ(rowType->childAt(5)->kind(), TypeKind::BIGINT);
  EXPECT_EQ(rowType->childAt(6)->kind(), TypeKind::DOUBLE);
  ASSERT_EQ(rowType->childAt(7)->kind(), TypeKind::ROW);
  const auto& mixedRow = rowType->childAt(7)->asRow();
  ASSERT_EQ(mixedRow.size(), 2);
  EXPECT_EQ(mixedRow.nameOf(0), "member0");
  EXPECT_EQ(mixedRow.nameOf(1), "member1");
  EXPECT_EQ(mixedRow.childAt(0)->kind(), TypeKind::INTEGER);
  EXPECT_EQ(mixedRow.childAt(1)->kind(), TypeKind::VARCHAR);
  ASSERT_EQ(rowType->childAt(8)->kind(), TypeKind::ROW);
  const auto& nullableMixedUnion = rowType->childAt(8)->asRow();
  ASSERT_EQ(nullableMixedUnion.size(), 3);
  EXPECT_EQ(nullableMixedUnion.nameOf(0), "member0");
  EXPECT_EQ(nullableMixedUnion.nameOf(1), "member1");
  EXPECT_EQ(nullableMixedUnion.nameOf(2), "member2");
  EXPECT_EQ(nullableMixedUnion.childAt(0)->kind(), TypeKind::INTEGER);
  EXPECT_EQ(nullableMixedUnion.childAt(1)->kind(), TypeKind::BIGINT);
  EXPECT_EQ(nullableMixedUnion.childAt(2)->kind(), TypeKind::VARCHAR);
  ASSERT_EQ(rowType->childAt(9)->kind(), TypeKind::ROW);
  const auto& simpleNestedRecord = rowType->childAt(9)->asRow();
  ASSERT_EQ(simpleNestedRecord.size(), 2);
  EXPECT_EQ(simpleNestedRecord.nameOf(0), "nestedInt");
  EXPECT_EQ(simpleNestedRecord.nameOf(1), "nestedString");
  EXPECT_EQ(simpleNestedRecord.childAt(0)->kind(), TypeKind::INTEGER);
  EXPECT_EQ(simpleNestedRecord.childAt(1)->kind(), TypeKind::VARCHAR);
  ASSERT_EQ(rowType->childAt(10)->kind(), TypeKind::ROW);
  const auto& nullableNestedRecord = rowType->childAt(10)->asRow();
  ASSERT_EQ(nullableNestedRecord.size(), 2);
  EXPECT_EQ(nullableNestedRecord.nameOf(0), "nestedInt");
  EXPECT_EQ(nullableNestedRecord.nameOf(1), "nestedString");
  EXPECT_EQ(nullableNestedRecord.childAt(0)->kind(), TypeKind::INTEGER);
  EXPECT_EQ(nullableNestedRecord.childAt(1)->kind(), TypeKind::VARCHAR);
}

TEST_F(AvroSchemaConverterTest, logicalUnionMapping) {
  const auto filePath = writeLogicalUnionRecord();

  auto reader = createReader(filePath);
  auto rowType = reader->rowType();
  ASSERT_EQ(rowType->size(), 8);

  const auto expectUnionType = [&](size_t fieldIndex,
                                   const TypePtr& firstType,
                                   const TypePtr& secondType) {
    ASSERT_EQ(rowType->childAt(fieldIndex)->kind(), TypeKind::ROW);
    const auto& unionType = rowType->childAt(fieldIndex)->asRow();
    ASSERT_EQ(unionType.size(), 2);
    EXPECT_EQ(unionType.nameOf(0), "member0");
    EXPECT_EQ(unionType.nameOf(1), "member1");
    EXPECT_TRUE(*unionType.childAt(0) == *firstType);
    EXPECT_TRUE(*unionType.childAt(1) == *secondType);
  };

  expectUnionType(0, DATE(), BIGINT());
  expectUnionType(1, INTEGER(), TIME());
  expectUnionType(2, TIME(), BIGINT());
  expectUnionType(3, INTEGER(), TIMESTAMP());
  expectUnionType(4, DATE(), TIMESTAMP());
  expectUnionType(5, TIME(), TIMESTAMP());
  EXPECT_TRUE(*rowType->childAt(6) == *BIGINT());
  EXPECT_TRUE(*rowType->childAt(7) == *DOUBLE());
}

TEST_F(AvroSchemaConverterTest, lowerCaseFieldNames) {
  const std::string schemaJson = R"JSON(
    {
      "type": "record",
      "name": "CaseRecord",
      "fields": [
        {"name": "FooBar", "type": "int"},
        {"name": "Baz", "type": "string"}
      ]
    })JSON";
  auto filePath = writeAvroFile(
      schemaJson,
      [](auto& /*writer*/, const ::avro::ValidSchema& /*schema*/) {});

  dwio::common::ReaderOptions options{pool()};
  options.setFileColumnNamesReadAsLowerCase(true);
  auto reader = createReader(filePath, options);
  auto rowType = reader->rowType();
  ASSERT_EQ(rowType->size(), 2);
  EXPECT_EQ(rowType->nameOf(0), "foobar");
  EXPECT_EQ(rowType->nameOf(1), "baz");
}

TEST_F(AvroSchemaConverterTest, rejectsDuplicateFieldNames) {
  const std::string schemaJson = R"JSON(
    {
      "type": "record",
      "name": "DupRecord",
      "fields": [
        {"name": "Foo", "type": "int"},
        {"name": "foo", "type": "long"}
      ]
    })JSON";
  auto filePath = writeAvroFile(
      schemaJson,
      [](auto& /*writer*/, const ::avro::ValidSchema& /*schema*/) {});

  dwio::common::ReaderOptions options{pool()};
  options.setFileColumnNamesReadAsLowerCase(true);
  EXPECT_THROW(createReader(filePath, options), VeloxRuntimeError);
}

TEST_F(AvroSchemaConverterTest, rejectsUnsupportedLogicalType) {
  const std::string schemaJson = R"JSON(
    {
      "type": "record",
      "name": "LogicalRecord",
      "fields": [
        {"name": "badLogical", "type": {"type": "long", "logicalType": "local-timestamp-millis"}}
      ]
    })JSON";
  auto filePath = writeAvroFile(
      schemaJson,
      [](auto& /*writer*/, const ::avro::ValidSchema& /*schema*/) {});

  EXPECT_THROW(createReader(filePath), VeloxUserError);
}

TEST_F(AvroSchemaConverterTest, rejectsUnsupportedDecimalMetadata) {
  const auto expectUnsupported = [&](const std::string& schemaJson,
                                     std::string_view expectedError) {
    auto filePath = writeAvroFile(
        schemaJson,
        [](auto& /*writer*/, const ::avro::ValidSchema& /*schema*/) {});
    VELOX_ASSERT_THROW_CODE(
        createReader(filePath), error_code::kUnsupported, expectedError);
  };

  expectUnsupported(
      R"JSON(
        {
          "type": "record",
          "name": "HighPrecisionDecimalRecord",
          "fields": [
            {"name": "value", "type": {
              "type": "bytes",
              "logicalType": "decimal",
              "precision": 39,
              "scale": 2
            }}
          ]
        })JSON",
      "Avro decimal precision exceeds the maximum supported by Velox");
  expectUnsupported(
      R"JSON(
        {
          "type": "record",
          "name": "WideFixedDecimalRecord",
          "fields": [
            {"name": "value", "type": {
              "type": "fixed",
              "name": "WideFixedDecimal",
              "size": 17,
              "logicalType": "decimal",
              "precision": 38,
              "scale": 2
            }}
          ]
        })JSON",
      "Avro fixed decimal encoding is wider than the maximum supported by "
      "the Velox Avro reader");
}

TEST_F(AvroSchemaConverterTest, complexNestedSchemaMapping) {
  auto filePath = writeComplexNestedRecord();

  auto reader = createReader(filePath);
  auto rowType = reader->rowType();
  ASSERT_EQ(rowType->size(), 2);
  ASSERT_EQ(rowType->childAt(0)->kind(), TypeKind::ROW);
  ASSERT_EQ(rowType->childAt(1)->kind(), TypeKind::ARRAY);

  const auto& metaRow = rowType->childAt(0)->asRow();
  ASSERT_EQ(metaRow.size(), 2);
  EXPECT_EQ(metaRow.childAt(0)->kind(), TypeKind::ARRAY);
  EXPECT_EQ(metaRow.childAt(1)->kind(), TypeKind::MAP);
  const auto& attrsMap = metaRow.childAt(1)->asMap();
  EXPECT_EQ(attrsMap.keyType()->kind(), TypeKind::VARCHAR);
  EXPECT_EQ(attrsMap.valueType()->kind(), TypeKind::VARCHAR);

  const auto& payloadArray = rowType->childAt(1)->asArray();
  const auto& payloadRow = payloadArray.elementType()->asRow();
  ASSERT_EQ(payloadRow.size(), 2);
  EXPECT_EQ(payloadRow.childAt(0)->kind(), TypeKind::ARRAY);
  EXPECT_EQ(payloadRow.childAt(1)->kind(), TypeKind::MAP);

  const auto& flags = payloadRow.childAt(0)->asArray();
  EXPECT_EQ(flags.elementType()->kind(), TypeKind::BOOLEAN);
  const auto& propertyMap = payloadRow.childAt(1)->asMap();
  const auto& propertyRow = propertyMap.valueType()->asRow();
  ASSERT_EQ(propertyRow.size(), 2);
  EXPECT_EQ(propertyRow.childAt(0)->kind(), TypeKind::VARCHAR);
  EXPECT_EQ(propertyRow.childAt(1)->kind(), TypeKind::ARRAY);
  const auto& propertyRowValue = propertyRow.childAt(1)->asArray();
  EXPECT_EQ(propertyRowValue.elementType()->kind(), TypeKind::INTEGER);
}

} // namespace
} // namespace facebook::velox::avro
