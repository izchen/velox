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

#include <cstring>
#include <memory>
#include <type_traits>
#include "velox/buffer/Buffer.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/exception/Exception.h"

namespace facebook {
namespace velox {
namespace dwio {
namespace common {

template <typename T, typename = std::enable_if_t<std::is_trivial_v<T>>>
class DataBuffer {
 public:
  explicit DataBuffer(velox::memory::MemoryPool& pool, uint64_t size = 0)
      : pool_(&pool),
        // Initial allocation uses calloc, to avoid memset.
        buf_(reinterpret_cast<T*>(
            pool_->allocateZeroFilled(1, sizeInBytes(size)))),
        size_(size),
        capacity_(size) {
    VELOX_CHECK(buf_ != nullptr || size_ == 0);
  }

  DataBuffer(DataBuffer&& other) noexcept
      : pool_{other.pool_},
        veloxRef_{other.veloxRef_},
        buf_{other.buf_},
        size_{other.size_},
        capacity_{other.capacity_} {
    other.buf_ = nullptr;
    other.size_ = 0;
    other.capacity_ = 0;
  }

  DataBuffer(const DataBuffer&) = delete;
  DataBuffer& operator=(DataBuffer&) = delete;
  DataBuffer& operator=(DataBuffer&&) = delete;

  ~DataBuffer() {
    clear();
  }

  T* data() {
    return buf_;
  }

  const T* data() const {
    return buf_;
  }

  uint64_t size() const {
    return size_;
  }

  uint64_t capacity() const {
    return capacity_;
  }

  uint64_t capacityInBytes() const {
    return sizeInBytes(capacity_);
  }

  T& operator[](uint64_t i) {
    return data()[i];
  }

  // Get with range check introduces significant overhead. Use index operator[]
  // when possible
  const T& at(uint64_t i) const {
    VELOX_CHECK_LT(i, size_, "Accessing index out of range");
    return data()[i];
  }

  const T& operator[](uint64_t i) const {
    return data()[i];
  }

  void reserve(uint64_t capacity) {
    if (capacity <= capacity_) {
      // After resetting the buffer, capacity always resets to zero.
      VELOX_CHECK_NOT_NULL(buf_);
      return;
    }
    if (veloxRef_ != nullptr) {
      VELOX_FAIL("Can't reserve on a referenced buffer");
    }
    const auto newSize = sizeInBytes(capacity);
    if (buf_ == nullptr) {
      buf_ = reinterpret_cast<T*>(pool_->allocate(newSize));
    } else {
      buf_ = reinterpret_cast<T*>(
          pool_->reallocate(buf_, capacityInBytes(), newSize));
    }
    VELOX_CHECK(buf_ != nullptr || newSize == 0);
    capacity_ = capacity;
  }

  void extend(uint64_t size) {
    auto newSize = size_ + size;
    if (newSize > capacity_) {
      reserve(newSize + ((newSize + 1) / 2) + 1);
    }
  }

  void resize(uint64_t size) {
    reserve(size);
    if (size > size_) {
      std::memset(data() + size_, 0, sizeInBytes(size - size_));
    }
    size_ = size;
  }

  void append(
      uint64_t offset,
      const DataBuffer<T>& src,
      uint64_t srcOffset,
      uint64_t items) {
    // Does src have insufficient data
    VELOX_CHECK_GE(src.size(), srcOffset + items);
    append(offset, src.data() + srcOffset, items);
  }

  void append(uint64_t offset, const T* src, uint64_t items) {
    reserve(offset + items);
    unsafeAppend(offset, src, items);
  }

  /// Sets a value to the specified offset. If offset overflows current
  /// capacity, it safely allocates more space to meet the request.
  void safeSet(uint64_t offset, T value) {
    if (offset >= capacity_) {
      // Increase capacity by 50% or by offset value.
      const auto size =
          std::max(offset + 1, capacity_ + ((capacity_ + 1) / 2) + 1);
      reserve(size);
      VLOG(1) << "reserve size: " << size << " for offset set: " << offset;
    }

    buf_[offset] = value;
    if (offset >= size_) {
      size_ = offset + 1;
    }
  }

  void extendAppend(uint64_t offset, const T* src, uint64_t items) {
    auto newSize = offset + items;
    if (FOLLY_UNLIKELY(newSize > capacity_)) {
      reserve(newSize + ((newSize + 1) / 2) + 1);
    }
    unsafeAppend(offset, src, items);
  }

  void unsafeAppend(uint64_t offset, const T* src, uint64_t items) {
    if (FOLLY_LIKELY(items > 0)) {
      std::memcpy(data() + offset, src, sizeInBytes(items));
    }
    size_ = (offset + items);
  }

  void unsafeAppend(const T* src, uint64_t items) {
    if (FOLLY_LIKELY(items > 0)) {
      std::memcpy(data() + size_, src, sizeInBytes(items));
      size_ += items;
    }
  }

  void unsafeAppend(T value) {
    buf_[size_++] = value;
  }

  void append(T value) {
    if (size_ >= capacity_) {
      // Increase capacity by 50%.
      reserve(capacity_ + ((capacity_ + 1) / 2) + 1);
    }
    unsafeAppend(value);
  }

  void clear() {
    if ((veloxRef_ == nullptr) && (buf_ != nullptr)) {
      pool_->free(buf_, sizeInBytes(capacity_));
    }
    size_ = 0;
    capacity_ = 0;
    buf_ = nullptr;
  }

  static std::shared_ptr<const DataBuffer<T>> wrap(
      const velox::BufferPtr& buffer) {
    return std::shared_ptr<DataBuffer<T>>(new DataBuffer<T>(buffer));
  }

 private:
  explicit DataBuffer(const velox::BufferPtr& buffer)
      : pool_{nullptr}, veloxRef_{buffer} {
    buf_ = const_cast<T*>(veloxRef_->as<T>());
    size_ = veloxRef_->size() / sizeof(T);
    capacity_ = size_;
  }

  uint64_t sizeInBytes(uint64_t items) const {
    return sizeof(T) * items;
  }

  velox::memory::MemoryPool* const pool_;
  // The referenced velox buffer. 'buf_' owns the memory when 'veloxRef_' is
  // nullptr.
  const velox::BufferPtr veloxRef_{nullptr};
  // Buffer storing the items.
  T* buf_;
  // Current number of items of type T.
  uint64_t size_;
  // Maximum capacity of items of type T.
  uint64_t capacity_;
};
} // namespace common
} // namespace dwio
} // namespace velox
} // namespace facebook
