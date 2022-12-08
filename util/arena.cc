// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/arena.h"

namespace leveldb {

// 一个block = 4kb
static const int kBlockSize = 4096;

Arena::Arena()
    : alloc_ptr_(nullptr), alloc_bytes_remaining_(0), memory_usage_(0) {}

Arena::~Arena() {
  for (size_t i = 0; i < blocks_.size(); i++) {
    // 最终统一释放所有new出来的内存
    delete[] blocks_[i];
  }
}

char* Arena::AllocateFallback(size_t bytes) {
  // 根据待分配内存大小做不同策略
  // 1. 待分配内存bytes大于kBlockSize / 4 = 1kb
  //    直接分配bytes大小内存并返回，当前alloc_ptr_继续保持不动
  // 2. 待分配内存bytes小于等于kBlockSize / 4 = 1kb
  //    分配kBlockSize = 4kb大小内存，返回前bytes大小
  //    并将剩余的内存更新为当前的待返回内存，但会造成上一个block中alloc_bytes_remaining_大小的浪费
  //    当然这个浪费量应该是小于 kBlockSize / 4 = 1kb 的
  if (bytes > kBlockSize / 4) {
    // Object is more than a quarter of our block size.  Allocate it separately
    // to avoid wasting too much space in leftover bytes.
    char* result = AllocateNewBlock(bytes);
    return result;
  }

  // We waste the remaining space in the current block.
  // Note:直接更新，造成了浪费，浪费小于kBlockSize / 4 = 1kb
  alloc_ptr_ = AllocateNewBlock(kBlockSize);
  alloc_bytes_remaining_ = kBlockSize;

  char* result = alloc_ptr_;
  alloc_ptr_ += bytes;
  alloc_bytes_remaining_ -= bytes;
  return result;
}

char* Arena::AllocateAligned(size_t bytes) {
  // 内存对齐: 每个块的内存地址能够被align整除
  const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
  static_assert((align & (align - 1)) == 0,
                "Pointer size should be a power of 2");
  // 取当前内存首地址的低位地址
  size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align - 1);
  // 低位地址为0，说明已经是内存对齐的了
  // 低地址不为0，说明不是内存对齐的，距离下一个内存对齐的位置的距离为slop
  size_t slop = (current_mod == 0 ? 0 : align - current_mod);
  // 待分配内存大小，包含内存对齐的牺牲slop
  size_t needed = bytes + slop;
  char* result;
  if (needed <= alloc_bytes_remaining_) {
    // 为了内存对齐，跳过slop，浪费掉了
    result = alloc_ptr_ + slop;
    alloc_ptr_ += needed;
    alloc_bytes_remaining_ -= needed;
  } else {
    // AllocateFallback always returned aligned memory
    result = AllocateFallback(bytes);
  }
  assert((reinterpret_cast<uintptr_t>(result) & (align - 1)) == 0);
  return result;
}

char* Arena::AllocateNewBlock(size_t block_bytes) {
  char* result = new char[block_bytes];
  blocks_.push_back(result);
  // sizeof(char*) 是把bolck中存储首地址的内存算上了
  memory_usage_.fetch_add(block_bytes + sizeof(char*),
                          std::memory_order_relaxed);
  return result;
}

}  // namespace leveldb
