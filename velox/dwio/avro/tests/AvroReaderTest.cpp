#include "velox/dwio/avro/reader/AvroReader.h"

#include <avro/Compiler.hh>
#include <avro/DataFile.hh>
#include <avro/Generic.hh>
#include <functional>
#include <gtest/gtest.h>
#include <sstream>
#include <string>
#include <vector>

#include "velox/common/file/File.h"
#include "velox/common/memory/Memory.h"
#include "velox/exec/tests/utils/TempFilePath.h"

using namespace facebook::velox;
using namespace facebook::velox::avro;

namespace {

::avro::ValidSchema parseSchema(const std::string& json) {
  ::avro::ValidSchema schema;
  std::istringstream schemaStream(json);
  ::avro::compileJsonSchema(schemaStream, schema);
  return schema;
}

std::unique_ptr<dwio::common::BufferedInput> createBufferedInput(
    const std::string& path,
    memory::MemoryPool& pool) {
  return std::make_unique<dwio::common::BufferedInput>(
      std::make_shared<LocalReadFile>(path), pool);
}

std::shared_ptr<exec::test::TempFilePath> writeFile(
    const std::string& schemaJson,
    const std::vector<std::function<void(::avro::GenericRecord&)>>& builders) {
  auto schema = parseSchema(schemaJson);
  auto file = exec::test::TempFilePath::create();

  ::avro::DataFileWriter<::avro::GenericDatum> writer(
      file->getPath().c_str(), schema);
  for (const auto& build : builders) {
    ::avro::GenericDatum datum(schema.root());
    auto& record = datum.value<::avro::GenericRecord>();
    build(record);
    writer.write(datum);
  }
  writer.close();
  return file;
}

} // namespace

class AvroReaderTest : public testing::Test {
 protected:
  void SetUp() override {
    pool_ = memory::memoryManager()->addLeafPool();
  }

  std::shared_ptr<memory::MemoryPool> pool_;
};

TEST_F(AvroReaderTest, invalidSchemaOverride) {
  const std::string schemaJson = R"({
    "type": "record",
    "name": "root",
    "fields": [{"name": "value", "type": "int"}]
  })";

  auto file = writeFile(
      schemaJson,
      {[](::avro::GenericRecord& record) {
        record.fieldAt(0).value<int32_t>() = 7;
      }});

  dwio::common::ReaderOptions options(pool_.get());
  options.serDeOptions().parameters["avro.schema.literal"] = "not a schema";

  auto input = createBufferedInput(file->getPath(), *pool_);
  EXPECT_THROW(AvroReader reader(std::move(input), options), VeloxUserError);
}

TEST_F(AvroReaderTest, invalidScanBatchBytes) {
  const std::string schemaJson = R"({
    "type": "record",
    "name": "root",
    "fields": [{"name": "value", "type": "int"}]
  })";

  auto file = writeFile(
      schemaJson,
      {[](::avro::GenericRecord& record) {
        record.fieldAt(0).value<int32_t>() = 1;
      }});

  dwio::common::ReaderOptions options(pool_.get());
  options.serDeOptions().parameters["avro.scan.batch.bytes"] = "0";

  auto input = createBufferedInput(file->getPath(), *pool_);
  EXPECT_THROW(AvroReader reader(std::move(input), options), VeloxUserError);
}

TEST_F(AvroReaderTest, numericPromotionUnion) {
  const std::string schemaJson = R"({
    "type": "record",
    "name": "root",
    "fields": [{"name": "number", "type": ["int", "long"]}]
  })";

  auto file = writeFile(
      schemaJson,
      {
          [](::avro::GenericRecord& record) {
            auto& unionField = record.fieldAt(0).value<::avro::GenericUnion>();
            unionField.selectBranch(0);
            unionField.datum().value<int32_t>() = 11;
          },
          [](::avro::GenericRecord& record) {
            auto& unionField = record.fieldAt(0).value<::avro::GenericUnion>();
            unionField.selectBranch(1);
            unionField.datum().value<int64_t>() = 12;
          },
      });

  dwio::common::ReaderOptions readerOptions(pool_.get());
  auto input = createBufferedInput(file->getPath(), *pool_);
  AvroReader reader(std::move(input), readerOptions);

  auto rowReader = reader.createRowReader();
  VectorPtr result;
  auto rowsRead = rowReader->next(10, result, nullptr);
  ASSERT_EQ(rowsRead, 2);

  auto rowVector = std::dynamic_pointer_cast<RowVector>(result);
  ASSERT_NE(rowVector, nullptr);
  auto numbers = rowVector->childAt(0)->as<FlatVector<int64_t>>();
  ASSERT_NE(numbers, nullptr);
  EXPECT_EQ(numbers->valueAt(0), 11);
  EXPECT_EQ(numbers->valueAt(1), 12);

  EXPECT_TRUE(rowReader->estimatedRowSize().has_value());
}

TEST_F(AvroReaderTest, structuredUnionWithNulls) {
  const std::string schemaJson = R"({
    "type": "record",
    "name": "root",
    "fields": [{"name": "mixed", "type": ["null", "string", "boolean"]}]
  })";

  auto file = writeFile(
      schemaJson,
      {
          [](::avro::GenericRecord& record) {
            auto& unionField = record.fieldAt(0).value<::avro::GenericUnion>();
            unionField.selectBranch(1);
            unionField.datum().value<std::string>() = "text";
          },
          [](::avro::GenericRecord& record) {
            auto& unionField = record.fieldAt(0).value<::avro::GenericUnion>();
            unionField.selectBranch(2);
            unionField.datum().value<bool>() = true;
          },
          [](::avro::GenericRecord& record) {
            auto& unionField = record.fieldAt(0).value<::avro::GenericUnion>();
            unionField.selectBranch(0);
          },
      });

  dwio::common::ReaderOptions readerOptions(pool_.get());
  auto input = createBufferedInput(file->getPath(), *pool_);
  AvroReader reader(std::move(input), readerOptions);

  auto rowReader = reader.createRowReader();
  VectorPtr result;
  auto rowsRead = rowReader->next(5, result, nullptr);
  ASSERT_EQ(rowsRead, 3);

  auto rowVector = std::dynamic_pointer_cast<RowVector>(result);
  ASSERT_NE(rowVector, nullptr);

  EXPECT_EQ(reader.rowType()->childAt(0)->kind(), TypeKind::ROW);
  auto unionRow = std::dynamic_pointer_cast<RowVector>(rowVector->childAt(0));
  ASSERT_NE(unionRow, nullptr);

  auto strings = unionRow->childAt(0)->as<FlatVector<StringView>>();
  auto booleans = unionRow->childAt(1)->as<FlatVector<bool>>();

  ASSERT_NE(strings, nullptr);
  ASSERT_NE(booleans, nullptr);

  EXPECT_FALSE(strings->isNullAt(0));
  EXPECT_EQ(strings->valueAt(0).str(), "text");
  EXPECT_TRUE(booleans->isNullAt(0));

  EXPECT_TRUE(strings->isNullAt(1));
  EXPECT_TRUE(booleans->valueAt(1));
  EXPECT_FALSE(rowVector->isNullAt(1));
  EXPECT_TRUE(rowVector->isNullAt(2));
}

