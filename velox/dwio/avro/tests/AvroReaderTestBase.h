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

#pragma once

#include <avro/DataFile.hh>
#include <avro/Generic.hh>

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include <gtest/gtest.h>

#include "velox/common/testutil/TempFilePath.h"
#include "velox/dwio/common/Reader.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::avro {

/// Provides shared Avro files and reader operations for Avro reader tests.
class AvroReaderTestBase : public testing::Test, public test::VectorTestBase {
 protected:
  // Initializes the memory manager shared by the test suite.
  static void SetUpTestSuite();

  // Registers the Avro reader factory before each test.
  void SetUp() override;

  // Unregisters the Avro reader factory after each test.
  void TearDown() override;

  // Adds all fields of 'type' to the ScanSpec in 'options'.
  void setScanSpec(const Type& type, dwio::common::RowReaderOptions& options)
      const;

  // Creates an Avro reader for 'filePath'.
  std::unique_ptr<dwio::common::Reader> createReader(
      const std::shared_ptr<common::testutil::TempFilePath>& filePath,
      std::optional<dwio::common::ReaderOptions> readerOptions =
          std::nullopt) const;

  // Creates a row reader using the supplied options.
  std::unique_ptr<dwio::common::RowReader> createRowReader(
      dwio::common::Reader& reader,
      std::optional<dwio::common::RowReaderOptions> rowOptions =
          std::nullopt) const;

  // Reads a batch and verifies the number of scanned rows.
  VectorPtr readRows(
      dwio::common::Reader& reader,
      uint64_t maxRows,
      uint64_t expectedScannedRows,
      std::optional<dwio::common::RowReaderOptions> rowOptions =
          std::nullopt) const;

  // Writes rows produced by 'writeRows' using the supplied Avro schema.
  static std::shared_ptr<common::testutil::TempFilePath> writeAvroFile(
      const std::string& schemaJson,
      const std::function<void(
          ::avro::DataFileWriter<::avro::GenericDatum>&,
          const ::avro::ValidSchema&)>& writeRows);

  // Writes values covering all supported Avro types.
  std::shared_ptr<common::testutil::TempFilePath> writeAllTypesRecord() const;

  // Writes nested record, array, and map values.
  std::shared_ptr<common::testutil::TempFilePath> writeComplexNestedRecord()
      const;

  // Writes rows used by requested-type and projection tests.
  std::shared_ptr<common::testutil::TempFilePath> writeRequestedTypeRecord()
      const;

  // Writes values covering supported Avro union representations.
  std::shared_ptr<common::testutil::TempFilePath> writeUnionRecord() const;

  // Writes logical types nested in Avro unions.
  std::shared_ptr<common::testutil::TempFilePath> writeLogicalUnionRecord()
      const;

};

} // namespace facebook::velox::avro
