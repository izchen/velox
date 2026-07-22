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

#include <avro/Node.hh>

#include "velox/dwio/avro/reader/AvroType.h"
#include "velox/dwio/common/Options.h"

namespace facebook::velox::avro {

/// See the Avro schema specification for details:
/// https://avro.apache.org/docs/1.12.0/specification/
///
/// Avro schema to Velox type mapping rules.
///
/// Primitive types (left: Avro physical type, Avro logical type is NONE):
///   null        -> UNKNOWN() (marks nullable)
///   boolean     -> BOOLEAN()
///   int         -> INTEGER()
///   long        -> BIGINT()
///   float       -> REAL()
///   double      -> DOUBLE()
///   string      -> VARCHAR()
///   bytes       -> VARBINARY()
///
/// Complex types (left: Avro physical type, Avro logical type is NONE):
///   enum           -> VARCHAR()
///   fixed          -> VARBINARY()
///   array<T>       -> ARRAY(veloxType(T))
///   map<string, V> -> MAP(VARCHAR(), veloxType(V))
///                       Avro spec mandates string keys
///   record{f1..fn} -> ROW(fieldNames, fieldTypes)
///   union          -> see "Union handling" below
///
/// Logical types (left: Avro physical type + logical type):
///   int  + date                    -> DATE()
///   int  + time-millis             -> TIME()
///   long + time-micros             -> TIME()
///   long + timestamp-millis        -> TIMESTAMP()
///   long + timestamp-micros        -> TIMESTAMP()
///   long + timestamp-nanos         -> TIMESTAMP()
///   string + uuid                  -> VARCHAR()
///   fixed  + uuid                  -> VARBINARY()
///   bytes/fixed + decimal          -> DECIMAL(precision, scale)
///   long + local-timestamp-millis  -> (no corresponding Velox type)
///   long + local-timestamp-micros  -> (no corresponding Velox type)
///   long + local-timestamp-nanos   -> (no corresponding Velox type)
///   fixed(size=12) + duration      -> (no corresponding Velox type)
///
/// Union handling:
///   - union with "null"          -> nullable = true
///   - single non-null branch     -> passthrough
///   - unions of {int, long} without logical types
///       -> BIGINT() (numeric promotion)
///   - unions of {float, double} without logical types
///       -> DOUBLE() (numeric promotion)
///   - other multi-branch unions
///       -> ROW(member0..memberN,
///              veloxType(branch0)..veloxType(branchN))
///          Field names are auto-generated as "member<i>" by branch index.
///
/// Notes:
///   - Symbolic nodes are resolved via avro::resolveSymbol() before mapping.
///   - Record field names can be lower-cased via
///     options.fileColumnNamesReadAsLowerCase(), duplicated field names
///     are rejected.
///   - Unsupported types fall through to VELOX_UNSUPPORTED().
std::shared_ptr<AvroTypeInfo> buildTypeInfo(
    const ::avro::NodePtr& node,
    const dwio::common::ReaderOptions& options);

} // namespace facebook::velox::avro
