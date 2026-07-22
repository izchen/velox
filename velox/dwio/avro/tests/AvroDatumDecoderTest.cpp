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

#include <avro/DataFile.hh>
#include <avro/Generic.hh>

#include "velox/common/testutil/TempFilePath.h"
#include "velox/dwio/avro/reader/AvroDatumDecoder.h"
#include "velox/dwio/avro/reader/AvroSchemaConverter.h"
#include "velox/expression/VectorWriters.h"
#include "velox/type/Type.h"

using namespace facebook::velox::test;

namespace facebook::velox::avro {
namespace {
using namespace facebook::velox::common::testutil;

// Tests Avro datum decoding directly into Velox vectors.
class AvroDatumDecoderTest : public AvroReaderTestBase {
 protected:
  VectorPtr decodeRows(
      const std::shared_ptr<TempFilePath>& filePath,
      vector_size_t maxRows,
      vector_size_t expectedRows) const {
    ::avro::DataFileReader<::avro::GenericDatum> reader(
        filePath->getPath().c_str());
    dwio::common::ReaderOptions options{pool()};
    auto typeInfo = buildTypeInfo(reader.readerSchema().root(), options);
    auto rowType =
        std::static_pointer_cast<const RowType>(typeInfo->veloxType);

    SelectivityVector rows(maxRows);
    VectorPtr result;
    BaseVector::ensureWritable(rows, rowType, pool(), result);
    auto rowVector = std::static_pointer_cast<RowVector>(result);
    rowVector->resize(maxRows);

    exec::VectorWriter<Any> writer;
    writer.init(*rowVector);
    ::avro::GenericDatum datum(reader.readerSchema());
    vector_size_t numRows{0};
    while (numRows < maxRows && reader.read(datum)) {
      writer.setOffset(numRows);
      decodeAvroDatum(*typeInfo, datum, writer.current());
      writer.commit(true);
      ++numRows;
    }
    writer.finish();
    rowVector->resize(numRows);
    EXPECT_EQ(numRows, expectedRows);
    return rowVector;
  }
};

TEST_F(AvroDatumDecoderTest, readsAllTypesData) {
  const auto filePath = writeAllTypesRecord();
  auto result = decodeRows(filePath, 5, 2);

  auto expected = makeRowVector({
      makeNullConstant(TypeKind::UNKNOWN, 2),
      makeFlatVector<bool>({true, false}),
      makeFlatVector<int32_t>({123, -456}),
      makeFlatVector<int64_t>({7890, -9876}),
      makeFlatVector<float>({1.5F, -2.5F}),
      makeFlatVector<double>({3.25, -4.75}),
      makeFlatVector<std::string>({"alpha", "beta"}),
      makeFlatVector<std::string>({"\x01\x02", "\x0A\x0B\x0C"}, VARBINARY()),
      makeFlatVector<std::string>({"RED", "GREEN"}),
      makeFlatVector<std::string>({"\xAA\xBB", "\xCC\xDD"}, VARBINARY()),
      makeArrayVector<int32_t>({{1, 2}, {3, 4}}),
      makeMapVector<std::string, int64_t>(
          {{{"a", 10}, {"b", 20}}, {{"c", -5}}}),
      makeRowVector(
          {makeFlatVector<int32_t>({101, 202}),
           makeFlatVector<std::string>({"sub-alpha", "sub-beta"})}),
      makeNullableFlatVector<int32_t>({42, std::nullopt}),
      makeFlatVector<int32_t>({1000, 2000}, DATE()),
      makeFlatVector<int64_t>({1234 * 1000L, 4321 * 1000L}, TIME()),
      makeFlatVector<int64_t>({5678, 8765}, TIME()),
      makeFlatVector<Timestamp>(
          {Timestamp::fromMillis(1700), Timestamp::fromMillis(2700)}),
      makeFlatVector<Timestamp>(
          {Timestamp::fromMicros(3500), Timestamp::fromMicros(4500)}),
      makeFlatVector<Timestamp>(
          {Timestamp::fromNanos(9876543210), Timestamp::fromNanos(1234567890)}),
      makeFlatVector<std::string>(
          {"123e4567-e89b-12d3-a456-426655440000",
           "00000000-0000-0000-0000-000000000000"}),
      makeFlatVector<std::string>(
          {std::string(
               "\x10\x11\x12\x13\x14\x15\x16\x17"
               "\x18\x19\x1A\x1B\x1C\x1D\x1E\x1F",
               16),
           std::string(
               "\xFF\xEE\xDD\xCC\xBB\xAA\x99\x88"
               "\x77\x66\x55\x44\x33\x22\x11\x00",
               16)},
          VARBINARY()),
      makeFlatVector<int64_t>({12345, -4200}, DECIMAL(9, 2)),
      makeFlatVector<int64_t>({6789, -1357}, DECIMAL(7, 3)),
  });
  assertEqualVectors(expected, result);
}

TEST_F(AvroDatumDecoderTest, readsComplexNestedData) {
  const auto filePath = writeComplexNestedRecord();
  auto result = decodeRows(filePath, 1, 1);
  auto ids = makeArrayVector<int64_t>({{101, 202}});
  auto attrsKeys = makeFlatVector<std::string>({"alpha", "beta"});
  auto attrsValues =
      makeNullableFlatVector<std::string>({std::nullopt, "beta"});
  auto attrs = makeMapVector({0, 2}, attrsKeys, attrsValues);
  auto meta = makeRowVector({ids, attrs});

  auto flagsElements = makeNullableFlatVector<bool>({std::nullopt, true});
  auto flags = makeArrayVector({0, 2}, flagsElements);
  auto propertyKeys = makeFlatVector<std::string>({"k1"});
  auto propertyNames = makeFlatVector<std::string>({"p1"});
  auto propertyValuesElements = makeFlatVector<int32_t>({1, 2});
  auto propertyValues = makeArrayVector({0, 2}, propertyValuesElements);
  auto propertyRow = makeRowVector({propertyNames, propertyValues});
  auto properties = makeMapVector({0, 1}, propertyKeys, propertyRow);

  auto payload = makeRowVector({flags, properties});
  auto payloads = makeArrayVector({0, 1}, payload);
  auto expected = makeRowVector({meta, payloads});
  assertEqualVectors(expected, result);
}

TEST_F(AvroDatumDecoderTest, readsUnionData) {
  const auto filePath = writeUnionRecord();
  auto result = decodeRows(filePath, 2, 2);

  auto mixedUnion = makeRowVector({
      makeNullableFlatVector<int32_t>({7, std::nullopt}),
      makeNullableFlatVector<std::string>({std::nullopt, "mix-b"}),
  });
  auto nullableMixedUnion = makeRowVector(
      {
          makeNullableFlatVector<int32_t>({std::nullopt, std::nullopt}),
          makeNullableFlatVector<int64_t>({std::nullopt, std::nullopt}),
          makeNullableFlatVector<std::string>({"mix-a", std::nullopt}),
      },
      [](vector_size_t row) { return row == 1; });
  auto simpleNestedRecord = makeRowVector({
      makeFlatVector<int32_t>({1, 3}),
      makeFlatVector<std::string>({"simple-a", "simple-b"}),
  });
  auto nullableNestedRecord = makeRowVector(
      {
          makeNullableFlatVector<int32_t>({2, std::nullopt}),
          makeNullableFlatVector<std::string>({"nullable-a", std::nullopt}),
      },
      [](vector_size_t row) { return row == 1; });

  auto expected = makeRowVector({
      makeFlatVector<std::string>({"alpha", "beta"}),
      makeNullConstant(TypeKind::UNKNOWN, 2),
      makeNullableFlatVector<int32_t>({11, std::nullopt}),
      makeFlatVector<int64_t>({101L, 10000000000L}),
      makeFlatVector<double>({1.5, 9.25}),
      makeNullableFlatVector<int64_t>({1001, 20000000000L}),
      makeNullableFlatVector<double>({2.5, std::nullopt}),
      mixedUnion,
      nullableMixedUnion,
      simpleNestedRecord,
      nullableNestedRecord,
  });
  assertEqualVectors(expected, result);
}

TEST_F(AvroDatumDecoderTest, readsLogicalUnionData) {
  const auto filePath = writeLogicalUnionRecord();
  auto result = decodeRows(filePath, 3, 3);

  auto dateOrLong = makeRowVector(
      {"member0", "member1"},
      {makeNullableFlatVector<int32_t>({10, std::nullopt, 11}, DATE()),
       makeNullableFlatVector<int64_t>({std::nullopt, 100, std::nullopt})});
  auto intOrTimeMicros = makeRowVector(
      {"member0", "member1"},
      {makeNullableFlatVector<int32_t>({20, std::nullopt, 21}),
       makeNullableFlatVector<int64_t>(
           {std::nullopt, 200, std::nullopt}, TIME())});
  auto timeMillisOrLong = makeRowVector(
      {"member0", "member1"},
      {makeNullableFlatVector<int64_t>({30'000, std::nullopt, 31'000}, TIME()),
       makeNullableFlatVector<int64_t>({std::nullopt, 300, std::nullopt})});
  auto intOrTimestampMillis = makeRowVector(
      {"member0", "member1"},
      {makeNullableFlatVector<int32_t>({40, std::nullopt, 41}),
       makeNullableFlatVector<Timestamp>(
           {std::nullopt, Timestamp::fromMillis(400), std::nullopt})});
  auto dateOrTimestampMicros = makeRowVector(
      {"member0", "member1"},
      {makeNullableFlatVector<int32_t>({50, std::nullopt, 51}, DATE()),
       makeNullableFlatVector<Timestamp>(
           {std::nullopt, Timestamp::fromMicros(500), std::nullopt})});
  auto nullableTimeMillisOrTimestampNanos = makeRowVector(
      {"member0", "member1"},
      {makeNullableFlatVector<int64_t>(
           {60'000, std::nullopt, std::nullopt}, TIME()),
       makeNullableFlatVector<Timestamp>(
           {std::nullopt, std::nullopt, Timestamp::fromNanos(600)})},
      [](vector_size_t row) { return row == 1; });
  auto intOrLong = makeFlatVector<int64_t>({70, 700, 71});
  auto floatOrDouble = makeFlatVector<double>({1.5, 2.25, 3.5});

  auto expected = makeRowVector(
      {"dateOrLong",
       "intOrTimeMicros",
       "timeMillisOrLong",
       "intOrTimestampMillis",
       "dateOrTimestampMicros",
       "nullableTimeMillisOrTimestampNanos",
       "intOrLong",
       "floatOrDouble"},
      {dateOrLong,
       intOrTimeMicros,
       timeMillisOrLong,
       intOrTimestampMillis,
       dateOrTimestampMicros,
       nullableTimeMillisOrTimestampNanos,
       intOrLong,
       floatOrDouble});
  assertEqualVectors(expected, result);
}

} // namespace
} // namespace facebook::velox::avro
