// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb {

// See doc/table_format.md for an explanation of the filter block format.

// Generate new filter every 2KB of data
static const size_t kFilterBaseLg = 11;
static const size_t kFilterBase = 1 << kFilterBaseLg;

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {}

// 每写入一个data_block同时也会相应的更新一下过滤器
// block_offset为当前data_block数据写入后在文件上的offset，指向新写入数据块末尾后一位
void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
  // 实现每kFilterBase个Byte数据，生成一个布隆过滤器
  uint64_t filter_index = (block_offset / kFilterBase);
  // filter_offsets_.size() 为当前生成的布隆过滤器的个数
  assert(filter_index >= filter_offsets_.size());
  // 循环？？？只有第一个是有key的，后面都是空荡荡的？
  // 考虑一种场景，当前data_block中的数据都特别大
  // 此时按照kFilterBase生成一个过滤器的原则，就需要执行多次GenerateFilter，直到生成的布隆过滤器数等于block_offset / kFilterBase的要求
  // 但是只有第一次循环是真正生成了过滤器的，第一次循环就把所有数据处理完了，后续再执行GenerateFilter时start_.empty()，仅仅是执行了一次filter_offsets_.push_back(result_.size());
  // 这样强制生成空的索引，严格保证kFilterBase个Byte一个过滤器的原因是什么？为了方便解析，FilterBlockReader::KeyMayMatch中使用block_offset >> base_lg_就可以得到对应位置的
  while (filter_index > filter_offsets_.size()) {
    GenerateFilter();
  }
}

void FilterBlockBuilder::AddKey(const Slice& key) {
  Slice k = key;
  // start_中记录各个key在keys_中的起始位置
  // keys_上直接拼接各个key在一个string字符串上
  start_.push_back(keys_.size());
  keys_.append(k.data(), k.size());
}

Slice FilterBlockBuilder::Finish() {
  if (!start_.empty()) {
    GenerateFilter();
  }

  // Append array of per-filter offsets
  // 将各个批次布隆过滤器位数组数据的offset数据记录到result_的末尾
  const uint32_t array_offset = result_.size();
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    PutFixed32(&result_, filter_offsets_[i]);
  }

  // 在result_末尾记录纯布隆过滤器有效数据大小以及kFilterBaseLg(每2kb真实数据生成一个布隆过滤器)
  PutFixed32(&result_, array_offset);
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result
  return Slice(result_);
}

void FilterBlockBuilder::GenerateFilter() {
  const size_t num_keys = start_.size();
  if (num_keys == 0) {
    // Fast path if there are no keys for this filter
    // 无用节点，只是为了方便后续直接通过data_block的offset查找
    filter_offsets_.push_back(result_.size());
    return;
  }

  // Make list of keys from flattened key structure
  // 将连续存储的在keys_上的key都解析出来放到tmp_keys_上，由policy_->CreateFilter来创建filter
  start_.push_back(keys_.size());  // Simplify length computation
  tmp_keys_.resize(num_keys);
  for (size_t i = 0; i < num_keys; i++) {
    const char* base = keys_.data() + start_[i];
    size_t length = start_[i + 1] - start_[i];
    tmp_keys_[i] = Slice(base, length);
  }

  // Generate filter for current set of keys and append to result_.
  // 记录当前批次布隆过滤器数据在result_上的offset
  filter_offsets_.push_back(result_.size());
  // 为本批次的num_kyes个key构建一个布隆过滤器，布隆过滤器的位数组信息追加在result_后面
  policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_);

  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}

FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,
                                     const Slice& contents)
    : policy_(policy), data_(nullptr), offset_(nullptr), num_(0), base_lg_(0) {
  size_t n = contents.size();

  // 参考过滤器写入数据的格式 FilterBlockBuilder::Finish
  // 最后面5Bytes包含了 1Byte的base_lg_和4Byte的纯布隆过滤器数据大小
  if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array
  base_lg_ = contents[n - 1];
  // 获取纯布隆过滤器数据大小，即前last_word位布隆过滤器数据，后面为各个批次过滤器的索引
  uint32_t last_word = DecodeFixed32(contents.data() + n - 5);
  if (last_word > n - 5) return;
  data_ = contents.data();
  // 过滤器数据索引起始点
  offset_ = data_ + last_word;
  // 过滤器批次数
  num_ = (n - 5 - last_word) / 4;
}

// 查询, 其中block_offset为数据块的offset
bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
  uint64_t index = block_offset >> base_lg_;
  // 获得过滤器批次index
  if (index < num_) {
    // 获得批次index对应的过滤数据的范围
    uint32_t start = DecodeFixed32(offset_ + index * 4);
    uint32_t limit = DecodeFixed32(offset_ + index * 4 + 4);
    if (start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
      // 从data中获取当前offset对应的过滤器数据
      Slice filter = Slice(data_ + start, limit - start);
      return policy_->KeyMayMatch(key, filter);
    } else if (start == limit) {
      // Empty filters do not match any keys
      return false;
    }
  }
  return true;  // Errors are treated as potential matches
}

}  // namespace leveldb
