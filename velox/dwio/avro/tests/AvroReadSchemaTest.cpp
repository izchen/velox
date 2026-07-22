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

#include <utility>

#include "velox/common/base/VeloxException.h"
#include "velox/dwio/common/Mutation.h"
#include "velox/type/Filter.h"
#include "velox/type/Type.h"

using namespace facebook::velox::test;

namespace facebook::velox::avro {
namespace {

// Provides a non-null delta updater for unsupported-feature tests.
class TestDeltaColumnUpdater : public dwio::common::DeltaColumnUpdater {
 public:
  void update(const RowSet&, VectorPtr&) override {}
};

// Tests requested type and ScanSpec planning for Avro reads.
class AvroReadSchemaTest : public AvroReaderTestBase {};

TEST_F(AvroReadSchemaTest, readsFileSchemaWithoutRequestedTypeOrScanSpec) {
  const auto filePath = writeRequestedTypeRecord();
  auto reader = createReader(filePath);

  auto rowReader = createRowReader(*reader, dwio::common::RowReaderOptions{});
  VectorPtr result;
  ASSERT_EQ(rowReader->next(5, result), 2);
  ASSERT_TRUE(*result->type() == *reader->rowType());

  auto expectedMeta = makeRowVector(
      {"tsMillis", "tsMicros", "label", "count"},
      {makeFlatVector<Timestamp>(
           {Timestamp::fromMillis(1'700), Timestamp::fromMillis(2'700)}),
       makeFlatVector<Timestamp>(
           {Timestamp::fromMicros(3'500), Timestamp::fromMicros(4'500)}),
       makeFlatVector<std::string>({"alpha", "beta"}),
       makeFlatVector<int32_t>({1, 2})});
  auto expected = makeRowVector(
      {"rawDate", "meta", "unused"},
      {makeFlatVector<int32_t>({11, 22}, DATE()),
       expectedMeta,
       makeFlatVector<std::string>({"unused-a", "unused-b"})});
  assertEqualVectors(expected, result);
}

TEST_F(AvroReadSchemaTest, requestedTypeWorksWithoutScanSpec) {
  const auto filePath = writeRequestedTypeRecord();
  auto reader = createReader(filePath);

  auto requestedType = ROW({"meta"}, {ROW({"label"}, {VARCHAR()})});
  dwio::common::RowReaderOptions rowOptions;
  rowOptions.setRequestedType(requestedType);

  auto rowReader = createRowReader(*reader, rowOptions);
  VectorPtr result;
  ASSERT_EQ(rowReader->next(5, result), 2);
  ASSERT_TRUE(*result->type() == *requestedType);

  auto expectedMeta = makeRowVector(
      {"label"}, {makeFlatVector<std::string>({"alpha", "beta"})});
  auto expected = makeRowVector({"meta"}, {expectedMeta});
  assertEqualVectors(expected, result);
}

TEST_F(AvroReadSchemaTest, requestedTypeRejectsMissingField) {
  const auto filePath = writeRequestedTypeRecord();
  expectRequestedTypeRejected(filePath, ROW({"missing"}, {VARCHAR()}));
  expectRequestedTypeRejected(
      filePath, ROW({"meta"}, {ROW({"missing"}, {VARCHAR()})}));
}

TEST_F(AvroReadSchemaTest, requestedTypeRejectsTypeMismatch) {
  const auto requestedTypeFilePath = writeRequestedTypeRecord();
  const auto pruningFilePath = writePruningRecord();
  const auto allTypesFilePath = writeAllTypesRecord();
  expectRequestedTypeRejected(
      requestedTypeFilePath, ROW({"unused"}, {BIGINT()}));
  expectRequestedTypeRejected(
      pruningFilePath, ROW({"nums"}, {ARRAY(BIGINT())}));
  expectRequestedTypeRejected(
      requestedTypeFilePath, ROW({"rawDate"}, {INTEGER()}));
  expectRequestedTypeRejected(
      allTypesFilePath, ROW({"timeMillisCol"}, {INTEGER()}));
  expectRequestedTypeRejected(
      requestedTypeFilePath, ROW({"meta"}, {ROW({"tsMillis"}, {BIGINT()})}));
  expectRequestedTypeRejected(
      allTypesFilePath, ROW({"decimalBytesCol"}, {VARBINARY()}));
}

TEST_F(AvroReadSchemaTest, requestedTypeValidatesDecimal) {
  const auto filePath = writeAllTypesRecord();

  dwio::common::RowReaderOptions rowOptions;
  rowOptions.setRequestedType(ROW({"decimalBytesCol"}, {DECIMAL(20, 2)}));

  auto reader = createReader(filePath);
  auto rowReader = createRowReader(*reader, rowOptions);
  VectorPtr result;
  ASSERT_EQ(rowReader->next(5, result), 2);

  auto expected = makeRowVector(
      {"decimalBytesCol"},
      {makeFlatVector<int128_t>(
          {static_cast<int128_t>(12345), static_cast<int128_t>(-4200)},
          DECIMAL(20, 2))});
  assertEqualVectors(expected, result);

  expectRequestedTypeRejected(
      filePath, ROW({"decimalBytesCol"}, {DECIMAL(5, 2)}));
  expectRequestedTypeRejected(
      filePath, ROW({"decimalBytesCol"}, {DECIMAL(9, 3)}));
}

TEST_F(AvroReadSchemaTest, scanSpecRejectsUnsupportedFeatures) {
  const auto filePath = writeRequestedTypeRecord();
  TestDeltaColumnUpdater updater;
  auto expectRejected = [&](std::shared_ptr<common::ScanSpec> spec) {
    auto reader = createReader(filePath);
    dwio::common::RowReaderOptions rowOptions;
    rowOptions.setScanSpec(std::move(spec));
    EXPECT_THROW(createRowReader(*reader, rowOptions), VeloxUserError);
  };

  auto rowIndexSpec = std::make_shared<common::ScanSpec>("root");
  rowIndexSpec->addField("rawDate", 0);
  rowIndexSpec->addField("$row_number", 1)
      ->setColumnType(common::ScanSpec::ColumnType::kRowIndex);
  expectRejected(std::move(rowIndexSpec));

  auto compositeSpec = std::make_shared<common::ScanSpec>("root");
  compositeSpec->getOrCreateChild("rawDate")->setFilter(
      common::createBigintValues({22}, false));
  compositeSpec->addField("$row_id", 0)
      ->setColumnType(common::ScanSpec::ColumnType::kComposite);
  expectRejected(std::move(compositeSpec));

  auto deltaUpdateSpec = std::make_shared<common::ScanSpec>("root");
  deltaUpdateSpec->addField("rawDate", 0)->setDeltaUpdate(&updater);
  expectRejected(std::move(deltaUpdateSpec));

  auto extractionWithoutTransformSpec =
      std::make_shared<common::ScanSpec>("root");
  extractionWithoutTransformSpec
      ->addFieldRecursively("mapCol", *MAP(VARCHAR(), BIGINT()), /*channel=*/0)
      ->setExtractionType(common::ScanSpec::ExtractionType::kKeys);
  auto extractionReader = createReader(writeAllTypesRecord());
  dwio::common::RowReaderOptions extractionRowOptions;
  extractionRowOptions.setScanSpec(std::move(extractionWithoutTransformSpec));
  EXPECT_THROW(
      createRowReader(*extractionReader, extractionRowOptions), VeloxUserError);
}

TEST_F(AvroReadSchemaTest, rejectsFilterOnlyFieldOutsideRequestedType) {
  const auto filePath = writeRequestedTypeRecord();
  auto reader = createReader(filePath);

  auto spec = std::make_shared<common::ScanSpec>("root");
  auto* meta = spec->addField("meta", 0);
  meta->addField("label", 0);
  spec->getOrCreateChild("rawDate")->setFilter(
      common::createBigintValues({22}, false));

  dwio::common::RowReaderOptions rowOptions;
  rowOptions.setRequestedType(ROW({"meta"}, {ROW({"label"}, {VARCHAR()})}));
  rowOptions.setScanSpec(spec);

  EXPECT_THROW(createRowReader(*reader, rowOptions), VeloxUserError);
}

TEST_F(AvroReadSchemaTest, rejectsProjectedFieldOutsideRequestedType) {
  const auto filePath = writeRequestedTypeRecord();
  auto reader = createReader(filePath);

  auto spec = std::make_shared<common::ScanSpec>("root");
  auto* meta = spec->addField("meta", 0);
  meta->addField("label", 0);
  spec->addField("unused", 1);

  dwio::common::RowReaderOptions rowOptions;
  rowOptions.setRequestedType(ROW({"meta"}, {ROW({"label"}, {VARCHAR()})}));
  rowOptions.setScanSpec(spec);

  EXPECT_THROW(createRowReader(*reader, rowOptions), VeloxUserError);
}

TEST_F(AvroReadSchemaTest, constantRejectsRequestedTypeMismatch) {
  const auto filePath = writeRequestedTypeRecord();
  auto reader = createReader(filePath);

  auto spec = std::make_shared<common::ScanSpec>("root");
  spec->addField("ds", 0)->setConstantValue(
      BaseVector::createConstant(VARCHAR(), "2026-05-06", 1, pool()));

  dwio::common::RowReaderOptions rowOptions;
  rowOptions.setRequestedType(ROW({"ds"}, {BIGINT()}));
  rowOptions.setScanSpec(spec);

  EXPECT_THROW(createRowReader(*reader, rowOptions), VeloxUserError);
}

} // namespace
} // namespace facebook::velox::avro
