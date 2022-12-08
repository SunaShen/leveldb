// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

struct TableBuilder::Rep {
  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == nullptr
                         ? nullptr
                         : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) {
    index_block_options.block_restart_interval = 1;
  }

  Options options;
  Options index_block_options;
  WritableFile* file;
  // 记录在文件file中的偏移offset
  uint64_t offset;
  Status status;
  // 数据block，包含key和val
  BlockBuilder data_block;
  // 索引block，记录key和数据block存储位置
  BlockBuilder index_block;
  // 上一个写入的key
  std::string last_key;
  // 数据条目
  int64_t num_entries;
  bool closed;  // Either Finish() or Abandon() has been called.
  // 过滤器，减少磁盘读，如bloom_filter
  FilterBlockBuilder* filter_block;

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  bool pending_index_entry;
  // pending_handle中记录offset和size
  BlockHandle pending_handle;  // Handle to add to index block

  // 在写入文件前，临时存放压缩数据
  std::string compressed_output;
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != nullptr) {
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}

// key的末尾包含了 (seq << 8) | type
void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->num_entries > 0) {
    // 严格保证升序
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }

  // 前一个data_block已经写入文件中时，pending_index_entry才为true
  // 在index_block中记一条记录
  if (r->pending_index_entry) {
    // data_block为空
    assert(r->data_block.empty());
    // 前面有保证key一定大于last_key
    // 找到[last_key, key）区间内的最短字符串，更新为last_key
    // 例如：
    // last_key = "comparator", key = "comparatxfj"
    // 得到结果为 "comparatp", 在区间范围内，且长度最短
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    std::string handle_encoding;
    // pending_handle中包含offset和size
    r->pending_handle.EncodeTo(&handle_encoding);
    // 新增一个索引记录,记录在文件中存储的块索引
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    r->pending_index_entry = false;
  }

  // 添加key,记录到过滤器中,减少不必要的读盘
  if (r->filter_block != nullptr) {
    r->filter_block->AddKey(key);
  }

  // 更新last_key
  r->last_key.assign(key.data(), key.size());
  r->num_entries++;
  // 添加key-val
  r->data_block.Add(key, value);

  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  if (estimated_block_size >= r->options.block_size) {
    Flush();
  }
}

void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  // 将data_block中的数据写入，并更新pending_handle，记录下一次写入的位置
  WriteBlock(&r->data_block, &r->pending_handle);
  if (ok()) {
    r->pending_index_entry = true;
    r->status = r->file->Flush();
  }
  // 每写入一个data_block就相应的更新下过滤器，使用当前数据块的数据创建一个布隆过滤器
  if (r->filter_block != nullptr) {
    r->filter_block->StartBlock(r->offset);
  }
}

void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  // block中所有数据整合成raw
  Slice raw = block->Finish();

  // 压缩数据
  Slice block_contents;
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        // 压缩后数据节省的空间小于原数据的12.5%时，不使用压缩，性能权衡？
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  // 将raw数据写入文件
  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();
  // 数据都已经写入文件了，重置block
  block->Reset();
}

void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type, BlockHandle* handle) {
  Rep* r = rep_;
  // handle用来记录offset和写入block数据的大小
  handle->set_offset(r->offset);
  handle->set_size(block_contents.size());
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    // 计算crc
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    // trailer = 1-byte compression_type + 32-bit crc
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      // 更新offset
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

Status TableBuilder::status() const { return rep_->status; }

// 数据已经都add完时执行Finish
Status TableBuilder::Finish() {
  Rep* r = rep_;
  // 将数据输入文件中
  Flush();
  assert(!r->closed);
  r->closed = true;

  // 最后等数据都写入完毕后，将其余的功能性block的内容也写入
  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Write filter block
  // 写入过滤器信息，使用非压缩形式，提高查询效率
  if (ok() && r->filter_block != nullptr) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
  }

  // Write metaindex block
  // 创建一个新的block，保存过滤器名字，以及过滤器block的offset信息，并写入文件中
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != nullptr) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): Add stats and other meta blocks
    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

  // Write index block
  // 写入索引block信息
  if (ok()) {
    // 由于前面执行了Flush此时pending_index_entry必定为true，在index末尾再增加前一个block的索引信息
    if (r->pending_index_entry) {
      // 函数功能：找到大于key的最短的字符串
      // 最后一个，为节省空间，所以随意找一个大于last_key的且最短的
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // Write footer
  // 写入footer信息，其中包含了metaindex_block和index_block的offset和size信息
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

uint64_t TableBuilder::FileSize() const { return rep_->offset; }

}  // namespace leveldb
