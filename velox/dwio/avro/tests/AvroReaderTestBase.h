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
#include <string>

#include <gtest/gtest.h>

#include "velox/common/testutil/TempFilePath.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::avro {

/// Provides shared Avro files for Avro reader component tests.
class AvroReaderTestBase : public testing::Test, public test::VectorTestBase {
 protected:
  // Initializes the memory manager shared by the test suite.
  static void SetUpTestSuite();

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

  // Writes values covering supported Avro union representations.
  std::shared_ptr<common::testutil::TempFilePath> writeUnionRecord() const;

  // Writes logical types nested in Avro unions.
  std::shared_ptr<common::testutil::TempFilePath> writeLogicalUnionRecord()
      const;

};

} // namespace facebook::velox::avro
