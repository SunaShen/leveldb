// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Decodes the blocks generated by block_builder.cc.

#include "table/block.h"

#include <algorithm>
#include <cstdint>
#include <vector>

#include "leveldb/comparator.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/logging.h"


// 重启点的作用
// 1. prev的时候分重启点区域倒退,不用回到起点在线性遍历
// 2. seek的时候方便二分查找，并且对应的位置就是各个数据的起点，不用害怕数据错位

namespace leveldb {

// 获取重启点的数量，block_data的最末尾存储的是重启点的数量，实现见BlockBuilder::Finish()
inline uint32_t Block::NumRestarts() const {
  assert(size_ >= sizeof(uint32_t));
  return DecodeFixed32(data_ + size_ - sizeof(uint32_t));
}

Block::Block(const BlockContents& contents)
    : data_(contents.data.data()),
      size_(contents.data.size()),
      owned_(contents.heap_allocated) {
  if (size_ < sizeof(uint32_t)) {
    // 错误数据
    size_ = 0;  // Error marker
  } else {
    // 重启点的最大数量，每个元素都是重启点
    size_t max_restarts_allowed = (size_ - sizeof(uint32_t)) / sizeof(uint32_t);
    if (NumRestarts() > max_restarts_allowed) {
      // The size is too small for NumRestarts()
      // 错误数据
      size_ = 0;
    } else {
      // 获得重启点数据的起点offset
      restart_offset_ = size_ - (1 + NumRestarts()) * sizeof(uint32_t);
    }
  }
}

Block::~Block() {
  // 负责释放从文件中读出的数据
  if (owned_) {
    delete[] data_;
  }
}

// Helper routine: decode the next block entry starting at "p",
// storing the number of shared key bytes, non_shared key bytes,
// and the length of the value in "*shared", "*non_shared", and
// "*value_length", respectively.  Will not dereference past "limit".
//
// If any errors are detected, returns nullptr.  Otherwise, returns a
// pointer to the key delta (just past the three decoded values).
static inline const char* DecodeEntry(const char* p, const char* limit,
                                      uint32_t* shared, uint32_t* non_shared,
                                      uint32_t* value_length) {
  // 每条数据的开头都有三个变长32位的值
  // 存储了1.key的共享前缀的长度 2.key的非共享前缀的长度 3.val的长度
  // 具体写入规则见BlockBuilder::Add
  if (limit - p < 3) return nullptr;
  *shared = reinterpret_cast<const uint8_t*>(p)[0];
  *non_shared = reinterpret_cast<const uint8_t*>(p)[1];
  *value_length = reinterpret_cast<const uint8_t*>(p)[2];
  // 先赌一把，认为三个值都是一个byte大小的，赌不对再老老实实解析
  if ((*shared | *non_shared | *value_length) < 128) {
    // Fast path: all three values are encoded in one byte each
    p += 3;
  } else {
    // 
    if ((p = GetVarint32Ptr(p, limit, shared)) == nullptr) return nullptr;
    if ((p = GetVarint32Ptr(p, limit, non_shared)) == nullptr) return nullptr;
    if ((p = GetVarint32Ptr(p, limit, value_length)) == nullptr) return nullptr;
  }

  if (static_cast<uint32_t>(limit - p) < (*non_shared + *value_length)) {
    return nullptr;
  }
  return p;
}

class Block::Iter : public Iterator {
 private:
  const Comparator* const comparator_;
  // block_content数据，有效数据size = 0 ~ restart_，因此无需单独指定size
  const char* const data_;       // underlying block contents
  // 重启点数据起点的offset
  uint32_t const restarts_;      // Offset of restart array (list of fixed32)
  // 重启点的数量
  uint32_t const num_restarts_;  // Number of uint32_t entries in restart array

  // current_ is offset in data_ of current entry.  >= restarts_ if !Valid
  // 当前数据位置
  uint32_t current_;
  // 当前数据对应的重启点位置
  uint32_t restart_index_;  // Index of restart block in which current_ falls
  std::string key_;
  Slice value_;
  Status status_;

  inline int Compare(const Slice& a, const Slice& b) const {
    return comparator_->Compare(a, b);
  }

  // Return the offset in data_ just past the end of the current entry.
  // 下一条数据的起点offset(相对data_)，通过char*指针相减得到
  inline uint32_t NextEntryOffset() const {
    return (value_.data() + value_.size()) - data_;
  }

  // 获取第index个重启点中存的数据(数据的offset)
  uint32_t GetRestartPoint(uint32_t index) {
    assert(index < num_restarts_);
    return DecodeFixed32(data_ + restarts_ + index * sizeof(uint32_t));
  }

  void SeekToRestartPoint(uint32_t index) {
    key_.clear();
    restart_index_ = index;
    // current_ will be fixed by ParseNextKey();

    // ParseNextKey() starts at the end of value_, so set value_ accordingly
    uint32_t offset = GetRestartPoint(index);
    // 将value_设置为第index个重启点对应的数据
    value_ = Slice(data_ + offset, 0);
  }

 public:
  Iter(const Comparator* comparator, const char* data, uint32_t restarts,
       uint32_t num_restarts)
      : comparator_(comparator),
        data_(data),
        restarts_(restarts),
        num_restarts_(num_restarts),
        // current_和restart_index_初始化就都设置为不能取到的最大值
        current_(restarts_),
        restart_index_(num_restarts_) {
    assert(num_restarts_ > 0);
  }

  bool Valid() const override { return current_ < restarts_; }
  Status status() const override { return status_; }
  Slice key() const override {
    assert(Valid());
    return key_;
  }
  Slice value() const override {
    assert(Valid());
    return value_;
  }

  void Next() override {
    assert(Valid());
    ParseNextKey();
  }

  void Prev() override {
    assert(Valid());

    // Scan backwards to a restart point before current_
    const uint32_t original = current_;
    // 当前重启点在当前数据的位置或者当前数据的后面，需要往前找到前一个重启点restart_index_--
    // 满足这个条件才是当前数据"前一个数据"所匹配的重启点
    while (GetRestartPoint(restart_index_) >= original) {
      if (restart_index_ == 0) {
        // No more entries
        current_ = restarts_;
        restart_index_ = num_restarts_;
        return;
      }
      restart_index_--;
    }

    // 移动到前一个数据匹配的数据点所对应的数据的位置
    SeekToRestartPoint(restart_index_);
    // 在当前重启点范围内，从前往后遍历，找到刚刚好小于当前值的数据，即得到了前一个数据
    do {
      // Loop until end of current entry hits the start of original entry
    } while (ParseNextKey() && NextEntryOffset() < original);
  }

  // 定位到要找的数据的位置,target为要查找的目标key
  // 使用二分查找找到包含目标值的重启点
  void Seek(const Slice& target) override {
    // Binary search in restart array to find the last restart point
    // with a key < target
    uint32_t left = 0;
    uint32_t right = num_restarts_ - 1;
    int current_key_compare = 0;

    // 先判断当前的key，刚好是target那就直接返回，否则，以此为基础判断是搜索前面的内容还是后面的内容
    // 注意此处二分查找的位置 mid, left, right都是重启点的索引
    // left 和 right 都是闭区间
    if (Valid()) {
      // If we're already scanning, use the current position as a starting
      // point. This is beneficial if the key we're seeking to is ahead of the
      // current position.
      // 注意当前的key_不一定刚好是重启点对应的数据，所以不能right = restart_index_ - 1;
      current_key_compare = Compare(key_, target);
      if (current_key_compare < 0) {
        // key_ is smaller than target
        left = restart_index_;
      } else if (current_key_compare > 0) {
        right = restart_index_;
      } else {
        // We're seeking to the key we're already at.
        return;
      }
    }

    while (left < right) {
      uint32_t mid = (left + right + 1) / 2;
      // 获得重启点mid对应的数据的位置
      uint32_t region_offset = GetRestartPoint(mid);
      // 解析数据
      uint32_t shared, non_shared, value_length;
      const char* key_ptr =
          DecodeEntry(data_ + region_offset, data_ + restarts_, &shared,
                      &non_shared, &value_length);
      // 重启点位置的数据不共享前缀，shared === 0
      if (key_ptr == nullptr || (shared != 0)) {
        CorruptionError();
        return;
      }
      Slice mid_key(key_ptr, non_shared);
      if (Compare(mid_key, target) < 0) {
        // Key at "mid" is smaller than "target".  Therefore all
        // blocks before "mid" are uninteresting.
        // 当前的mid_key一定刚好是重启点对应的数据(当前区域的最小值)
        // 但是保不准当前mid_key后面属于当前重启点范围的值(略大于mid_key)是否会包含目标值
        // 所以保守一点将 left = mid 而不是 left = mid + 1
        left = mid;
      } else {
        // Key at "mid" is >= "target".  Therefore all blocks at or
        // after "mid" are uninteresting.
        // 当前的mid_key一定刚好是重启点对应的数据(当前区域的最小值)
        // 所以当前重启点区域必定与目标key无缘了，直接right = mid - 1;
        right = mid - 1;
      }
    }

    // We might be able to use our current position within the restart block.
    // This is true if we determined the key we desire is in the current block
    // and is after than the current key.
    assert(current_key_compare == 0 || Valid());
    // 是否可以不用从重启点区域的起点开始搜索，需要满足以下条件
    // 1. 当前是同一个重启点区域 2. 当前的key小于目标值，这样后面线性所有才确保能找到
    // 性能优化，充分利用当前已存在的值
    bool skip_seek = left == restart_index_ && current_key_compare < 0;
    if (!skip_seek) {
      // 调整到重启点left对应的数据
      SeekToRestartPoint(left);
    }
    // Linear search (within restart block) for first key >= target
    // 通过二分已经锁定了数据一定在当前重启点区域内，现在线性搜索，直到找到大于等于目标值的key
    while (true) {
      if (!ParseNextKey()) {
        return;
      }
      if (Compare(key_, target) >= 0) {
        return;
      }
    }
  }

  void SeekToFirst() override {
    SeekToRestartPoint(0);
    ParseNextKey();
  }

  void SeekToLast() override {
    // 跳转到最后一个重启点区域之后线性遍历
    // TODO : 可以优化下，先判断下当前值是不是最后一个，是的话就直接return了
    // if (Valid() && NextEntryOffset() == restarts_) {
    //   return;
    // }
    SeekToRestartPoint(num_restarts_ - 1);
    while (ParseNextKey() && NextEntryOffset() < restarts_) {
      // Keep skipping
    }
  }

 private:
  void CorruptionError() {
    current_ = restarts_;
    restart_index_ = num_restarts_;
    status_ = Status::Corruption("bad entry in block");
    key_.clear();
    value_.clear();
  }

  bool ParseNextKey() {
    // 将current_更新为下一条数据的起点
    current_ = NextEntryOffset();
    const char* p = data_ + current_;
    // limit后面都是记录的重启点数据了
    const char* limit = data_ + restarts_;  // Restarts come right after data
    if (p >= limit) {
      // No more entries to return.  Mark as invalid.
      // 结束了，已经读取完了所有数据
      // 将current_和restart_index_都设为无效的数据
      current_ = restarts_;
      restart_index_ = num_restarts_;
      return false;
    }

    // Decode next entry
    // 在数据开头解析基础数据，key的共享前缀长度、key的非共享前缀长度、val的长度
    uint32_t shared, non_shared, value_length;
    p = DecodeEntry(p, limit, &shared, &non_shared, &value_length);
    if (p == nullptr || key_.size() < shared) {
      // 此时的key_还是前一个key呢，共享前缀不能大于前面key的长度
      CorruptionError();
      return false;
    } else {
      // 直接resize，直接复用，提高效率
      key_.resize(shared);
      key_.append(p, non_shared);
      value_ = Slice(p + non_shared, value_length);
      // 更新重启点，设置为当前数据的前一个重启点
      while (restart_index_ + 1 < num_restarts_ &&
             GetRestartPoint(restart_index_ + 1) < current_) {
        ++restart_index_;
      }
      return true;
    }
  }
};

Iterator* Block::NewIterator(const Comparator* comparator) {
  if (size_ < sizeof(uint32_t)) {
    return NewErrorIterator(Status::Corruption("bad block contents"));
  }
  const uint32_t num_restarts = NumRestarts();
  if (num_restarts == 0) {
    return NewEmptyIterator();
  } else {
    return new Iter(comparator, data_, restart_offset_, num_restarts);
  }
}

}  // namespace leveldb
