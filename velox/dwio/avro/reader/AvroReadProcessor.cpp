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

#include "velox/dwio/avro/reader/AvroReadProcessor.h"

#include <algorithm>
#include <cstring>
#include <limits>
#include <vector>

#include "velox/common/base/BitUtil.h"
#include "velox/dwio/common/Mutation.h"
#include "velox/dwio/common/ScanSpec.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/ConstantVector.h"

namespace facebook::velox::avro {
namespace {

bool hasProjectedChildren(const common::ScanSpec& spec) {
  for (const auto& child : spec.children()) {
    if (child->projectOut()) {
      return true;
    }
  }
  return false;
}

column_index_t projectedColumnCount(const common::ScanSpec& spec) {
  column_index_t numColumns = 0;
  for (const auto& childSpec : spec.children()) {
    if (!childSpec->projectOut()) {
      continue;
    }

    VELOX_CHECK_NE(childSpec->channel(), common::ScanSpec::kNoChannel);
    numColumns = std::max(numColumns, childSpec->channel() + 1);
  }
  return numColumns;
}

VectorPtr projectComplexVector(
    const VectorPtr& input,
    const common::ScanSpec& spec);

VectorPtr applyScanSpecTransform(
    const VectorPtr& input,
    const common::ScanSpec& spec) {
  if (!spec.hasTransform()) {
    return input;
  }

  VELOX_CHECK_NOT_NULL(
      spec.transformOutputType(),
      "Avro ScanSpec transform output type is null for field: {}.",
      spec.fieldName());
  auto transformed = spec.transform()(input, input->pool());
  VELOX_CHECK_NOT_NULL(
      transformed,
      "Avro ScanSpec transform returned a null vector for field: {}.",
      spec.fieldName());
  VELOX_CHECK_EQ(
      transformed->size(),
      input->size(),
      "Avro ScanSpec transform changed the number of rows for field: {}.",
      spec.fieldName());
  return transformed;
}

struct NestedEntrySelection {
  BufferPtr offsets;
  BufferPtr sizes;
  BufferPtr indices;
  vector_size_t selectedCount{0};
  bool entriesChanged{false};
};

NestedEntrySelection selectNestedEntries(
    const ArrayVectorBase& collection,
    const uint64_t* passed,
    vector_size_t totalEntries,
    vector_size_t maxEntriesPerRow) {
  const auto numRows = collection.size();
  auto offsets = allocateOffsets(numRows, collection.pool());
  auto sizes = allocateSizes(numRows, collection.pool());
  auto indices = allocateIndices(totalEntries, collection.pool());
  auto* rawOffsets = offsets->asMutable<vector_size_t>();
  auto* rawSizes = sizes->asMutable<vector_size_t>();
  auto* rawIndices = indices->asMutable<vector_size_t>();

  vector_size_t numSelected = 0;
  bool changed = false;
  for (vector_size_t row = 0; row < numRows; ++row) {
    const auto begin = collection.offsetAt(row);
    const auto originalSize =
        collection.isNullAt(row) ? 0 : collection.sizeAt(row);
    const auto limit = std::min(originalSize, maxEntriesPerRow);

    rawOffsets[row] = numSelected;
    vector_size_t selectedInRow = 0;
    for (vector_size_t i = 0; i < limit; ++i) {
      const auto entry = begin + i;
      if (passed && !bits::isBitSet(passed, entry)) {
        changed = true;
        continue;
      }
      rawIndices[numSelected++] = entry;
      ++selectedInRow;
    }
    if (limit != originalSize) {
      changed = true;
    }

    rawSizes[row] = selectedInRow;
  }

  return {
      std::move(offsets),
      std::move(sizes),
      std::move(indices),
      numSelected,
      changed};
}

VectorPtr projectRowVector(
    const VectorPtr& input,
    const common::ScanSpec& spec) {
  const auto* inputRow = input->as<RowVector>();
  VELOX_CHECK_NOT_NULL(inputRow);
  const auto& inputRowType = input->type()->asRow();
  const auto numColumns = projectedColumnCount(spec);

  std::vector<std::string> names(numColumns);
  std::vector<TypePtr> types(numColumns);
  std::vector<VectorPtr> children(numColumns);
  for (const auto& childSpec : spec.children()) {
    if (!childSpec->projectOut()) {
      continue;
    }

    const auto channel = childSpec->channel();
    names[channel] = childSpec->fieldName();
    if (childSpec->isConstant()) {
      children[channel] = applyScanSpecTransform(
          BaseVector::wrapInConstant(
              input->size(), 0, childSpec->constantValue()),
          *childSpec);
      types[channel] = children[channel]->type();
      continue;
    }

    const auto childIdx = inputRowType.getChildIdx(childSpec->fieldName());
    auto child = inputRow->childAt(childIdx);
    VELOX_CHECK_NOT_NULL(
        child,
        "Avro projection expected child vector for '{}'.",
        childSpec->fieldName());
    child = projectComplexVector(child, *childSpec);
    types[channel] = child->type();
    children[channel] = std::move(child);
  }

  auto rowType = ROW(std::move(names), std::move(types));
  return std::make_shared<RowVector>(
      input->pool(),
      rowType,
      input->nulls(),
      input->size(),
      std::move(children),
      input->getNullCount());
}

VectorPtr projectArrayVector(
    const VectorPtr& input,
    const common::ScanSpec& spec) {
  auto* arrayVector = input->as<ArrayVector>();
  VELOX_CHECK_NOT_NULL(arrayVector);
  auto* elementSpec =
      spec.childByName(common::ScanSpec::kArrayElementsFieldName);
  VELOX_DCHECK(
      !elementSpec || !elementSpec->isConstant(),
      "Avro reader does not support constant array elements.");
  const auto maxEntries = spec.maxArrayElementsCount();
  if (!elementSpec && maxEntries == std::numeric_limits<vector_size_t>::max()) {
    return input;
  }

  auto elements = arrayVector->elements();

  std::vector<uint64_t> passedEntries;
  const auto hasElementFilter = elementSpec && elementSpec->hasFilter();
  if (hasElementFilter && elements->size() > 0) {
    passedEntries.assign(bits::nwords(elements->size()), -1);
    elementSpec->applyFilter(*elements, elements->size(), passedEntries.data());
  }

  auto projectedElements =
      elementSpec ? projectComplexVector(elements, *elementSpec) : elements;
  const auto hasMaxEntryLimit =
      maxEntries != std::numeric_limits<vector_size_t>::max();
  NestedEntrySelection selection;
  if (hasElementFilter || hasMaxEntryLimit) {
    selection = selectNestedEntries(
        *arrayVector,
        passedEntries.empty() ? nullptr : passedEntries.data(),
        elements->size(),
        maxEntries);
  }
  if (selection.entriesChanged) {
    projectedElements = BaseVector::wrapInDictionary(
        nullptr, selection.indices, selection.selectedCount, projectedElements);
    return std::make_shared<ArrayVector>(
        input->pool(),
        ARRAY(projectedElements->type()),
        input->nulls(),
        input->size(),
        std::move(selection.offsets),
        std::move(selection.sizes),
        std::move(projectedElements),
        input->getNullCount());
  }

  if (projectedElements == elements) {
    return input;
  }

  return std::make_shared<ArrayVector>(
      input->pool(),
      ARRAY(projectedElements->type()),
      input->nulls(),
      input->size(),
      arrayVector->offsets(),
      arrayVector->sizes(),
      std::move(projectedElements),
      input->getNullCount());
}

VectorPtr projectMapVector(
    const VectorPtr& input,
    const common::ScanSpec& spec) {
  auto* mapVector = input->as<MapVector>();
  VELOX_CHECK_NOT_NULL(mapVector);
  auto* keySpec = spec.childByName(common::ScanSpec::kMapKeysFieldName);
  auto* valueSpec = spec.childByName(common::ScanSpec::kMapValuesFieldName);
  VELOX_DCHECK(
      !keySpec || !keySpec->isConstant(),
      "Avro reader does not support constant map keys.");
  VELOX_DCHECK(
      !valueSpec || !valueSpec->isConstant(),
      "Avro reader does not support constant map values.");
  if (!keySpec && !valueSpec) {
    return input;
  }

  auto keys = mapVector->mapKeys();
  auto values = mapVector->mapValues();

  std::vector<uint64_t> passedEntries;
  const auto totalEntryCount = keys->size();
  const auto hasKeyFilter = keySpec && keySpec->hasFilter();
  const auto hasValueFilter = valueSpec && valueSpec->hasFilter();
  if ((hasKeyFilter || hasValueFilter) && totalEntryCount > 0) {
    passedEntries.assign(bits::nwords(totalEntryCount), -1);
    if (hasKeyFilter) {
      keySpec->applyFilter(*keys, totalEntryCount, passedEntries.data());
    }
    if (hasValueFilter) {
      valueSpec->applyFilter(*values, totalEntryCount, passedEntries.data());
    }
  }

  auto projectedKeys = keySpec ? projectComplexVector(keys, *keySpec) : keys;
  auto projectedValues =
      valueSpec ? projectComplexVector(values, *valueSpec) : values;
  NestedEntrySelection selection;
  if (hasKeyFilter || hasValueFilter) {
    selection = selectNestedEntries(
        *mapVector,
        passedEntries.empty() ? nullptr : passedEntries.data(),
        totalEntryCount,
        std::numeric_limits<vector_size_t>::max());
  }
  if (selection.entriesChanged) {
    projectedKeys = BaseVector::wrapInDictionary(
        nullptr, selection.indices, selection.selectedCount, projectedKeys);
    projectedValues = BaseVector::wrapInDictionary(
        nullptr, selection.indices, selection.selectedCount, projectedValues);
    return std::make_shared<MapVector>(
        input->pool(),
        MAP(projectedKeys->type(), projectedValues->type()),
        input->nulls(),
        input->size(),
        std::move(selection.offsets),
        std::move(selection.sizes),
        std::move(projectedKeys),
        std::move(projectedValues),
        input->getNullCount(),
        mapVector->hasSortedKeys());
  }

  if (projectedKeys == keys && projectedValues == values) {
    return input;
  }

  return std::make_shared<MapVector>(
      input->pool(),
      MAP(projectedKeys->type(), projectedValues->type()),
      input->nulls(),
      input->size(),
      mapVector->offsets(),
      mapVector->sizes(),
      std::move(projectedKeys),
      std::move(projectedValues),
      input->getNullCount(),
      mapVector->hasSortedKeys());
}

VectorPtr projectComplexVector(
    const VectorPtr& input,
    const common::ScanSpec& spec) {
  VectorPtr projected;
  switch (input->typeKind()) {
    case TypeKind::ROW:
      projected = projectRowVector(input, spec);
      break;
    case TypeKind::ARRAY:
      projected = projectArrayVector(input, spec);
      break;
    case TypeKind::MAP:
      projected = projectMapVector(input, spec);
      break;
    default:
      projected = input;
      break;
  }

  return applyScanSpecTransform(projected, spec);
}

void applyMutationToRows(
    const dwio::common::Mutation* mutation,
    vector_size_t rowCount,
    uint64_t* passed) {
  if (!mutation) {
    return;
  }
  if (mutation->deletedRows) {
    bits::andWithNegatedBits(passed, mutation->deletedRows, 0, rowCount);
  }
  if (mutation->randomSkip) {
    bits::forEachSetBit(passed, 0, rowCount, [&](auto i) {
      if (!mutation->randomSkip->testOne()) {
        bits::clearBit(passed, i);
      }
    });
  }
}

VectorPtr applyRowSelection(const VectorPtr& input, const uint64_t* passed) {
  const auto* inputRow = input->as<RowVector>();
  VELOX_CHECK_NOT_NULL(inputRow);
  const auto inputSize = input->size();
  const auto outputSize = bits::countBits(passed, 0, inputSize);
  if (outputSize == inputSize) {
    return input;
  }
  if (outputSize == 0) {
    return RowVector::createEmpty(input->type(), input->pool());
  }

  auto children = inputRow->children();
  auto indices = allocateIndices(outputSize, input->pool());
  auto* rawIndices = indices->asMutable<vector_size_t>();
  vector_size_t j = 0;
  bits::forEachSetBit(
      passed, 0, inputSize, [&](auto i) { rawIndices[j++] = i; });

  for (auto& child : children) {
    if (!child) {
      continue;
    }
    child->disableMemo();
    child = BaseVector::wrapInDictionary(
        nullptr, indices, outputSize, std::move(child));
  }

  BufferPtr outputNulls = input->nulls();
  if (input->nulls()) {
    outputNulls = AlignedBuffer::allocate<bool>(outputSize, input->pool());
    auto* rawOutputNulls = outputNulls->asMutable<uint64_t>();
    memset(rawOutputNulls, 0xFF, bits::nbytes(outputSize));

    const auto* rawInputNulls = input->rawNulls();
    for (vector_size_t i = 0; i < outputSize; ++i) {
      if (bits::isBitNull(rawInputNulls, rawIndices[i])) {
        bits::setNull(rawOutputNulls, i);
      }
    }
  }

  return std::make_shared<RowVector>(
      input->pool(),
      input->type(),
      outputNulls,
      outputSize,
      std::move(children));
}

} // namespace

VectorPtr processAvroRows(
    const VectorPtr& input,
    const common::ScanSpec* spec,
    const dwio::common::Mutation* mutation) {
  if (!spec && !dwio::common::hasDeletion(mutation)) {
    return input;
  }

  auto* inputRow = input->as<RowVector>();
  VELOX_CHECK_NOT_NULL(inputRow);
  std::vector<uint64_t> passed(bits::nwords(input->size()), -1);
  applyMutationToRows(mutation, input->size(), passed.data());

  VectorPtr projected = input;
  if (spec) {
    if (spec->hasFilter()) {
      spec->applyFilter(*inputRow, input->size(), passed.data());
    }
    projected = projectComplexVector(input, *spec);
  }

  return applyRowSelection(projected, passed.data());
}

} // namespace facebook::velox::avro
