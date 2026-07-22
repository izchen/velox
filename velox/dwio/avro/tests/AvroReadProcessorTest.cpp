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

#include <string_view>
#include <utility>

#include "velox/common/base/BitUtil.h"
#include "velox/dwio/common/Mutation.h"
#include "velox/type/Filter.h"
#include "velox/type/Type.h"

using namespace facebook::velox::test;

namespace facebook::velox::avro {
namespace {
using namespace facebook::velox::common::testutil;

// Tests ScanSpec processing and row-level mutation.
class AvroReadProcessorTest : public AvroReaderTestBase {};

TEST_F(AvroReadProcessorTest, scanSpecProjectsNestedRowFields) {
  const auto filePath = writeRequestedTypeRecord();
  auto reader = createReader(filePath);

  auto spec = std::make_shared<common::ScanSpec>("root");
  auto* meta = spec->addField("meta", 0);
  meta->addField("label", 0);

  dwio::common::RowReaderOptions rowOptions;
  rowOptions.setScanSpec(spec);
  auto result = readRows(*reader, 5, 2, rowOptions);

  auto expectedMeta = makeRowVector(
      {"label"}, {makeFlatVector<std::string>({"alpha", "beta"})});
  auto expected = makeRowVector({"meta"}, {expectedMeta});
  assertEqualVectors(expected, result);
}

TEST_F(AvroReadProcessorTest, scanSpecProjectsMissingConstantFields) {
  const auto filePath = writeRequestedTypeRecord();
  auto reader = createReader(filePath);

  auto spec = std::make_shared<common::ScanSpec>("root");
  spec->addField("ds", 0)->setConstantValue(
      BaseVector::createConstant(VARCHAR(), "2026-05-06", 1, pool()));
  auto* meta = spec->addField("meta", 1);
  meta->addField("label", 0);
  meta->addField("region", 1)
      ->setConstantValue(
          BaseVector::createConstant(VARCHAR(), "us", 1, pool()));

  dwio::common::RowReaderOptions rowOptions;
  rowOptions.setScanSpec(spec);
  auto result = readRows(*reader, 5, 2, rowOptions);

  auto expectedMeta = makeRowVector(
      {"label", "region"},
      {makeFlatVector<std::string>({"alpha", "beta"}),
       BaseVector::createConstant(VARCHAR(), "us", 2, pool())});
  auto expected = makeRowVector(
      {"ds", "meta"},
      {BaseVector::createConstant(VARCHAR(), "2026-05-06", 2, pool()),
       expectedMeta});
  assertEqualVectors(expected, result);
}

TEST_F(AvroReadProcessorTest, scanSpecAppliesNestedFilterBeforeProjection) {
  const auto filePath = writeRequestedTypeRecord();
  auto reader = createReader(filePath);

  auto spec = std::make_shared<common::ScanSpec>("root");
  auto* meta = spec->addField("meta", 0);
  meta->addField("label", 0);
  spec->getOrCreateChild(common::Subfield("meta.count"))
      ->setFilter(common::createBigintValues({2}, false));

  dwio::common::RowReaderOptions rowOptions;
  rowOptions.setScanSpec(spec);
  auto result = readRows(*reader, 5, 2, rowOptions);

  auto expectedMeta =
      makeRowVector({"label"}, {makeFlatVector<std::string>({"beta"})});
  auto expected = makeRowVector({"meta"}, {expectedMeta});
  assertEqualVectors(expected, result);
}

TEST_F(AvroReadProcessorTest, scanSpecProjectsNestedArrayAndMapFields) {
  auto filePath = writeComplexNestedRecord();
  auto reader = createReader(filePath);

  auto spec = std::make_shared<common::ScanSpec>("root");
  auto* payloads = spec->addField("payloads", 0);
  auto* payload = payloads->addArrayElementField();
  auto* properties = payload->addField("properties", 0);
  auto* property = properties->addMapValueField();
  property->addField("values", 0);

  dwio::common::RowReaderOptions rowOptions;
  rowOptions.setScanSpec(spec);
  auto result = readRows(*reader, 5, 1, rowOptions);

  const auto& rowType = result->type()->asRow();
  ASSERT_EQ(rowType.size(), 1);
  EXPECT_EQ(rowType.nameOf(0), "payloads");
  const auto& payloadsType = rowType.childAt(0)->asArray();
  const auto& payloadType = payloadsType.elementType()->asRow();
  ASSERT_EQ(payloadType.size(), 1);
  EXPECT_EQ(payloadType.nameOf(0), "properties");
  const auto& propertiesType = payloadType.childAt(0)->asMap();
  const auto& propertyType = propertiesType.valueType()->asRow();
  ASSERT_EQ(propertyType.size(), 1);
  EXPECT_EQ(propertyType.nameOf(0), "values");

  ASSERT_EQ(result->size(), 1);
  auto payloadsVector = result->as<RowVector>()->childAt(0)->as<ArrayVector>();
  ASSERT_EQ(payloadsVector->sizeAt(0), 1);
  auto payloadVector = payloadsVector->elements()->as<RowVector>();
  auto propertiesVector = payloadVector->childAt(0)->as<MapVector>();
  ASSERT_GT(propertiesVector->size(), 0);
  ASSERT_EQ(propertiesVector->sizeAt(0), 1);
  EXPECT_EQ(
      propertiesVector->mapKeys()->asFlatVector<StringView>()->valueAt(0).str(),
      "k1");
  auto propertyVector = propertiesVector->mapValues()->as<RowVector>();
  auto valuesVector = propertyVector->childAt(0)->as<ArrayVector>();
  ASSERT_GT(valuesVector->size(), 0);
  ASSERT_EQ(valuesVector->sizeAt(0), 2);
  auto values = valuesVector->elements()->asFlatVector<int32_t>();
  EXPECT_EQ(values->valueAt(0), 1);
  EXPECT_EQ(values->valueAt(1), 2);
}

TEST_F(AvroReadProcessorTest, scanSpecPrunesArrayAndMapEntries) {
  auto filePath = writePruningRecord();
  auto reader = createReader(filePath);

  auto spec = std::make_shared<common::ScanSpec>("root");
  auto* nums = spec->addFieldRecursively("nums", *ARRAY(INTEGER()), 0);
  nums->childByName(common::ScanSpec::kArrayElementsFieldName)
      ->setFilter(common::createBigintValues({2, 4}, false));

  spec->addFieldRecursively("limited", *ARRAY(INTEGER()), 1)
      ->setMaxArrayElementsCount(2);

  auto* props =
      spec->addFieldRecursively("props", *MAP(VARCHAR(), INTEGER()), 2);
  props->childByName(common::ScanSpec::kMapKeysFieldName)
      ->setFilter(
          std::make_unique<common::BytesValues>(
              std::vector<std::string>{"k2"}, false));

  auto* valueProps =
      spec->addFieldRecursively("valueProps", *MAP(VARCHAR(), INTEGER()), 3);
  valueProps->childByName(common::ScanSpec::kMapValuesFieldName)
      ->setFilter(common::createBigintValues({1, 3}, false));

  dwio::common::RowReaderOptions rowOptions;
  rowOptions.setScanSpec(spec);
  auto result = readRows(*reader, 5, 1, rowOptions);

  auto expected = makeRowVector(
      {"nums", "limited", "props", "valueProps"},
      {makeArrayVector<int32_t>({{2, 4}}),
       makeArrayVector<int32_t>({{10, 20}}),
       makeMapVector<std::string, int32_t>({{{"k2", 2}}}),
       makeMapVector<std::string, int32_t>({{{"k1", 1}, {"k3", 3}}})});
  assertEqualVectors(expected, result);
}

TEST_F(AvroReadProcessorTest, scanSpecProjectsUnionMember) {
  const auto filePath = writeUnionRecord();
  auto reader = createReader(filePath);

  auto spec = std::make_shared<common::ScanSpec>("root");
  auto* mixedUnion = spec->addField("mixedUnion", 0);
  mixedUnion->addField("member1", 0);

  dwio::common::RowReaderOptions rowOptions;
  rowOptions.setScanSpec(spec);
  auto result = readRows(*reader, 5, 2, rowOptions);

  auto expectedMixedUnion = makeRowVector(
      {"member1"},
      {makeNullableFlatVector<std::string>({std::nullopt, "mix-b"})});
  auto expected = makeRowVector({"mixedUnion"}, {expectedMixedUnion});
  assertEqualVectors(expected, result);
}

TEST_F(AvroReadProcessorTest, scanSpecFiltersUnionMember) {
  const auto filePath = writeUnionRecord();
  auto reader = createReader(filePath);

  auto spec = std::make_shared<common::ScanSpec>("root");
  auto* mixedUnion = spec->addField("mixedUnion", 0);
  auto* member = mixedUnion->addField("member1", 0);
  member->setFilter(
      std::make_unique<common::BytesValues>(
          std::vector<std::string>{"mix-b"}, false));

  dwio::common::RowReaderOptions rowOptions;
  rowOptions.setScanSpec(spec);
  auto result = readRows(*reader, 5, 2, rowOptions);

  auto expectedMixedUnion =
      makeRowVector({"member1"}, {makeFlatVector<std::string>({"mix-b"})});
  auto expected = makeRowVector({"mixedUnion"}, {expectedMixedUnion});
  assertEqualVectors(expected, result);
}

TEST_F(AvroReadProcessorTest, scanSpecAppliesTransformToConstantField) {
  const auto filePath = writeRequestedTypeRecord();
  auto reader = createReader(filePath);

  auto spec = std::make_shared<common::ScanSpec>("root");
  auto* constantSpec = spec->addField("ds", 0);
  constantSpec->setConstantValue(
      BaseVector::createConstant(VARCHAR(), "abc", 1, pool()));
  constantSpec->setTransform(
      [](const VectorPtr& input, memory::MemoryPool* pool) -> VectorPtr {
        VELOX_CHECK(input->isConstantEncoding());
        const auto* inputStrings = input->as<SimpleVector<StringView>>();
        VELOX_CHECK_NOT_NULL(inputStrings);

        auto output = BaseVector::create(BIGINT(), input->size(), pool);
        auto* rawLengths = output->asFlatVector<int64_t>()->mutableRawValues();
        for (vector_size_t row{0}; row < input->size(); ++row) {
          rawLengths[row] = inputStrings->valueAt(row).size();
        }
        return output;
      },
      BIGINT());

  dwio::common::RowReaderOptions rowOptions;
  rowOptions.setScanSpec(spec);
  auto result = readRows(*reader, 5, 2, rowOptions);

  auto expected = makeRowVector({"ds"}, {makeFlatVector<int64_t>({3, 3})});
  assertEqualVectors(expected, result);
}

TEST_F(AvroReadProcessorTest, scanSpecAppliesExtractionTransform) {
  const auto filePath = writeAllTypesRecord();
  auto reader = createReader(filePath);

  auto spec = std::make_shared<common::ScanSpec>("root");
  auto* mapSpec = spec->addFieldRecursively(
      "mapCol", *MAP(VARCHAR(), BIGINT()), /*channel=*/0);
  mapSpec->setExtractionType(common::ScanSpec::ExtractionType::kKeys);
  mapSpec->setTransform(
      [](const VectorPtr& input, memory::MemoryPool* pool) -> VectorPtr {
        auto* map = input->as<MapVector>();
        VELOX_CHECK_NOT_NULL(map);
        return std::make_shared<ArrayVector>(
            pool,
            ARRAY(map->mapKeys()->type()),
            map->nulls(),
            map->size(),
            map->offsets(),
            map->sizes(),
            map->mapKeys());
      },
      ARRAY(VARCHAR()));

  dwio::common::RowReaderOptions rowOptions;
  rowOptions.setScanSpec(spec);
  auto result = readRows(*reader, 5, 2, rowOptions);

  auto expected = makeRowVector(
      {"mapCol"}, {makeArrayVector<std::string>({{"a", "b"}, {"c"}})});
  assertEqualVectors(expected, result);
}

TEST_F(AvroReadProcessorTest, scanSpecAppliesNestedExtractionTransform) {
  const auto filePath = writeComplexNestedRecord();
  auto reader = createReader(filePath);

  const auto metaType =
      ROW({{"ids", ARRAY(BIGINT())}, {"attrs", MAP(VARCHAR(), VARCHAR())}});
  auto spec = std::make_shared<common::ScanSpec>("root");
  auto* metaSpec = spec->addFieldRecursively("meta", *metaType, /*channel=*/0);
  metaSpec->setExtractionType(common::ScanSpec::ExtractionType::kField);
  metaSpec->setExtractionFieldIndex(1);
  metaSpec->childByName("ids")->setConstantValue(
      BaseVector::createNullConstant(ARRAY(BIGINT()), 1, pool()));
  metaSpec->childByName("attrs")->setExtractionType(
      common::ScanSpec::ExtractionType::kKeys);
  metaSpec->setTransform(
      [](const VectorPtr& input, memory::MemoryPool* pool) -> VectorPtr {
        auto* meta = input->as<RowVector>();
        VELOX_CHECK_NOT_NULL(meta);
        auto* attrs = meta->childAt(input->type()->asRow().getChildIdx("attrs"))
                          ->as<MapVector>();
        VELOX_CHECK_NOT_NULL(attrs);
        return std::make_shared<ArrayVector>(
            pool,
            ARRAY(attrs->mapKeys()->type()),
            attrs->nulls(),
            attrs->size(),
            attrs->offsets(),
            attrs->sizes(),
            attrs->mapKeys());
      },
      ARRAY(VARCHAR()));

  dwio::common::RowReaderOptions rowOptions;
  rowOptions.setScanSpec(spec);
  auto result = readRows(*reader, 5, 1, rowOptions);

  auto expected = makeRowVector(
      {"meta"}, {makeArrayVector<std::string>({{"alpha", "beta"}})});
  assertEqualVectors(expected, result);
}

TEST_F(AvroReadProcessorTest, scanSpecAppliesMultipleExtractionTransform) {
  const auto filePath = writeAllTypesRecord();
  auto reader = createReader(filePath);

  auto spec = std::make_shared<common::ScanSpec>("root");
  auto* mapSpec = spec->addFieldRecursively(
      "mapCol", *MAP(VARCHAR(), BIGINT()), /*channel=*/0);
  const auto transformOutputType =
      ROW({{"keys", ARRAY(VARCHAR())}, {"size", BIGINT()}});
  mapSpec->setTransform(
      [transformOutputType](
          const VectorPtr& input, memory::MemoryPool* pool) -> VectorPtr {
        auto* map = input->as<MapVector>();
        VELOX_CHECK_NOT_NULL(map);
        auto keys = std::make_shared<ArrayVector>(
            pool,
            ARRAY(map->mapKeys()->type()),
            map->nulls(),
            map->size(),
            map->offsets(),
            map->sizes(),
            map->mapKeys());
        auto sizes = BaseVector::create(BIGINT(), map->size(), pool);
        auto* rawSizes = sizes->asFlatVector<int64_t>()->mutableRawValues();
        for (vector_size_t i = 0; i < map->size(); ++i) {
          rawSizes[i] = map->sizeAt(i);
        }
        return std::make_shared<RowVector>(
            pool,
            transformOutputType,
            nullptr,
            map->size(),
            std::vector<VectorPtr>{std::move(keys), std::move(sizes)});
      },
      transformOutputType);

  dwio::common::RowReaderOptions rowOptions;
  rowOptions.setScanSpec(spec);
  auto result = readRows(*reader, 5, 2, rowOptions);

  auto expectedExtractions = makeRowVector(
      {"keys", "size"},
      {makeArrayVector<std::string>({{"a", "b"}, {"c"}}),
       makeFlatVector<int64_t>({2, 1})});
  auto expected = makeRowVector({"mapCol"}, {expectedExtractions});
  assertEqualVectors(expected, result);
}

TEST_F(AvroReadProcessorTest, appliesDeletedRowsMutationWithoutScanSpec) {
  const auto filePath = writeRequestedTypeRecord();
  auto reader = createReader(filePath);
  auto rowReader = createRowReader(*reader);

  std::vector<uint64_t> deletedRows(bits::nwords(2));
  bits::setBit(deletedRows.data(), 0);
  dwio::common::Mutation mutation;
  mutation.deletedRows = deletedRows.data();

  VectorPtr result;
  ASSERT_EQ(rowReader->next(5, result, &mutation), 2);

  auto expectedMeta = makeRowVector(
      {"tsMillis", "tsMicros", "label", "count"},
      {makeFlatVector<Timestamp>({Timestamp::fromMillis(2'700)}),
       makeFlatVector<Timestamp>({Timestamp::fromMicros(4'500)}),
       makeFlatVector<std::string>({"beta"}),
       makeFlatVector<int32_t>({2})});
  auto expected = makeRowVector(
      {"rawDate", "meta", "unused"},
      {makeFlatVector<int32_t>({22}, DATE()),
       expectedMeta,
       makeFlatVector<std::string>({"unused-b"})});
  assertEqualVectors(expected, result);
}

TEST_F(AvroReadProcessorTest, scanSpecAppliesDeletedRowsMutation) {
  const auto filePath = writeRequestedTypeRecord();
  auto reader = createReader(filePath);

  auto spec = std::make_shared<common::ScanSpec>("root");
  auto* meta = spec->addField("meta", 0);
  meta->addField("label", 0);
  spec->getOrCreateChild(common::Subfield("meta.count"))
      ->setFilter(common::createBigintValues({1, 2}, false));

  dwio::common::RowReaderOptions rowOptions;
  rowOptions.setScanSpec(spec);
  auto rowReader = createRowReader(*reader, rowOptions);

  std::vector<uint64_t> deletedRows(bits::nwords(2));
  bits::setBit(deletedRows.data(), 0);
  dwio::common::Mutation mutation;
  mutation.deletedRows = deletedRows.data();

  VectorPtr result;
  ASSERT_EQ(rowReader->next(5, result, &mutation), 2);

  auto expectedMeta =
      makeRowVector({"label"}, {makeFlatVector<std::string>({"beta"})});
  auto expected = makeRowVector({"meta"}, {expectedMeta});
  assertEqualVectors(expected, result);
}

// requestedType present; ScanSpec present.

TEST_F(AvroReadProcessorTest, projectsFieldsInScanSpecOrder) {
  const auto filePath = writeRequestedTypeRecord();
  auto reader = createReader(filePath);

  auto requestedType =
      ROW({"rawDate", "meta"},
          {DATE(),
           ROW({"tsMillis", "tsMicros", "label"},
               {TIMESTAMP(), TIMESTAMP(), VARCHAR()})});
  auto expectedType =
      ROW({"meta", "rawDate"},
          {ROW({"label", "tsMicros", "tsMillis"},
               {VARCHAR(), TIMESTAMP(), TIMESTAMP()}),
           DATE()});
  auto spec = std::make_shared<common::ScanSpec>("root");
  auto* meta = spec->addField("meta", 0);
  meta->addField("label", 0);
  meta->addField("tsMicros", 1);
  meta->addField("tsMillis", 2);
  spec->addField("rawDate", 1);

  dwio::common::RowReaderOptions rowOptions;
  rowOptions.setRequestedType(requestedType);
  rowOptions.setScanSpec(spec);

  auto result = readRows(*reader, 5, 2, rowOptions);
  ASSERT_TRUE(*result->type() == *expectedType);

  auto expectedMeta = makeRowVector(
      {"label", "tsMicros", "tsMillis"},
      {makeFlatVector<std::string>({"alpha", "beta"}),
       makeFlatVector<Timestamp>(
           {Timestamp::fromMicros(3'500), Timestamp::fromMicros(4'500)}),
       makeFlatVector<Timestamp>(
           {Timestamp::fromMillis(1'700), Timestamp::fromMillis(2'700)})});
  auto expected = makeRowVector(
      {"meta", "rawDate"},
      {expectedMeta, makeFlatVector<int32_t>({11, 22}, DATE())});
  assertEqualVectors(expected, result);
}

TEST_F(AvroReadProcessorTest, filtersByNestedNonProjectedField) {
  const auto filePath = writeRequestedTypeRecord();
  auto reader = createReader(filePath);

  auto requestedType =
      ROW({"meta"}, {ROW({"label", "count"}, {VARCHAR(), INTEGER()})});
  auto spec = std::make_shared<common::ScanSpec>("root");
  auto* meta = spec->addField("meta", 0);
  meta->addField("label", 0);
  spec->getOrCreateChild(common::Subfield("meta.count"))
      ->setFilter(common::createBigintValues({2}, false));

  dwio::common::RowReaderOptions rowOptions;
  rowOptions.setRequestedType(requestedType);
  rowOptions.setScanSpec(spec);
  auto result = readRows(*reader, 5, 2, rowOptions);

  auto expectedMeta =
      makeRowVector({"label"}, {makeFlatVector<std::string>({"beta"})});
  auto expected = makeRowVector({"meta"}, {expectedMeta});
  assertEqualVectors(expected, result);
}

TEST_F(AvroReadProcessorTest, filtersByTopLevelNonProjectedField) {
  const auto filePath = writeRequestedTypeRecord();
  auto reader = createReader(filePath);

  auto requestedType =
      ROW({"meta", "rawDate"}, {ROW({"label"}, {VARCHAR()}), DATE()});
  auto spec = std::make_shared<common::ScanSpec>("root");
  auto* meta = spec->addField("meta", 0);
  meta->addField("label", 0);
  spec->getOrCreateChild("rawDate")->setFilter(
      common::createBigintValues({22}, false));

  dwio::common::RowReaderOptions rowOptions;
  rowOptions.setRequestedType(requestedType);
  rowOptions.setScanSpec(spec);
  auto result = readRows(*reader, 5, 2, rowOptions);

  auto expectedMeta =
      makeRowVector({"label"}, {makeFlatVector<std::string>({"beta"})});
  auto expected = makeRowVector({"meta"}, {expectedMeta});
  assertEqualVectors(expected, result);
}

TEST_F(AvroReadProcessorTest, constantProjectsMissingField) {
  const auto filePath = writeRequestedTypeRecord();
  auto reader = createReader(filePath);

  auto requestedType =
      ROW({"ds", "meta"}, {VARCHAR(), ROW({"label"}, {VARCHAR()})});
  auto spec = std::make_shared<common::ScanSpec>("root");
  spec->addField("ds", 0)->setConstantValue(
      BaseVector::createConstant(VARCHAR(), "2026-05-06", 1, pool()));
  auto* meta = spec->addField("meta", 1);
  meta->addField("label", 0);

  dwio::common::RowReaderOptions rowOptions;
  rowOptions.setRequestedType(requestedType);
  rowOptions.setScanSpec(spec);
  auto result = readRows(*reader, 5, 2, rowOptions);

  auto expectedMeta = makeRowVector(
      {"label"}, {makeFlatVector<std::string>({"alpha", "beta"})});
  auto expected = makeRowVector(
      {"ds", "meta"},
      {BaseVector::createConstant(VARCHAR(), "2026-05-06", 2, pool()),
       expectedMeta});
  assertEqualVectors(expected, result);
}

TEST_F(AvroReadProcessorTest, constantOverridesFileField) {
  const auto filePath = writeRequestedTypeRecord();
  auto reader = createReader(filePath);

  auto requestedType =
      ROW({"unused", "meta"}, {VARCHAR(), ROW({"label"}, {VARCHAR()})});
  auto spec = std::make_shared<common::ScanSpec>("root");
  spec->addField("unused", 0)
      ->setConstantValue(
          BaseVector::createConstant(VARCHAR(), "constant-unused", 1, pool()));
  auto* meta = spec->addField("meta", 1);
  meta->addField("label", 0);

  dwio::common::RowReaderOptions rowOptions;
  rowOptions.setRequestedType(requestedType);
  rowOptions.setScanSpec(spec);
  auto result = readRows(*reader, 5, 2, rowOptions);

  auto expectedMeta = makeRowVector(
      {"label"}, {makeFlatVector<std::string>({"alpha", "beta"})});
  auto expected = makeRowVector(
      {"unused", "meta"},
      {BaseVector::createConstant(VARCHAR(), "constant-unused", 2, pool()),
       expectedMeta});
  assertEqualVectors(expected, result);
}

TEST_F(AvroReadProcessorTest, constantCanBeOutsideRequestedType) {
  const auto filePath = writeRequestedTypeRecord();
  auto reader = createReader(filePath);

  auto requestedType = ROW({"meta"}, {ROW({"label"}, {VARCHAR()})});
  auto spec = std::make_shared<common::ScanSpec>("root");
  spec->addField("ds", 0)->setConstantValue(
      BaseVector::createConstant(VARCHAR(), "2026-05-06", 1, pool()));
  auto* meta = spec->addField("meta", 1);
  meta->addField("label", 0);

  dwio::common::RowReaderOptions rowOptions;
  rowOptions.setRequestedType(requestedType);
  rowOptions.setScanSpec(spec);
  auto result = readRows(*reader, 5, 2, rowOptions);

  auto expectedMeta = makeRowVector(
      {"label"}, {makeFlatVector<std::string>({"alpha", "beta"})});
  auto expected = makeRowVector(
      {"ds", "meta"},
      {BaseVector::createConstant(VARCHAR(), "2026-05-06", 2, pool()),
       expectedMeta});
  assertEqualVectors(expected, result);
}

} // namespace
} // namespace facebook::velox::avro
