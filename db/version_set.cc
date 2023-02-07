// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include <algorithm>
#include <cstdio>

#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include "leveldb/env.h"
#include "leveldb/table_builder.h"
#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb {

static size_t TargetFileSize(const Options* options) {
  return options->max_file_size;
}

// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
// stop building a single file in a level->level+1 compaction.
static int64_t MaxGrandParentOverlapBytes(const Options* options) {
  return 10 * TargetFileSize(options);
}

// Maximum number of bytes in all compacted files.  We avoid expanding
// the lower level file set of a compaction if it would make the
// total compaction cover more than this many bytes.
static int64_t ExpandedCompactionByteSizeLimit(const Options* options) {
  return 25 * TargetFileSize(options);
}

static double MaxBytesForLevel(const Options* options, int level) {
  // Note: the result for level zero is not really used since we set
  // the level-0 compaction threshold based on number of files.

  // Result for both level-0 and level-1
  double result = 10. * 1048576.0;
  while (level > 1) {
    result *= 10;
    level--;
  }
  return result;
}

static uint64_t MaxFileSizeForLevel(const Options* options, int level) {
  // We could vary per level to reduce number of files?
  return TargetFileSize(options);
}

static int64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
  int64_t sum = 0;
  for (size_t i = 0; i < files.size(); i++) {
    sum += files[i]->file_size;
  }
  return sum;
}

Version::~Version() {
  assert(refs_ == 0);

  // Remove from linked list
  prev_->next_ = next_;
  next_->prev_ = prev_;

  // Drop references to files
  for (int level = 0; level < config::kNumLevels; level++) {
    for (size_t i = 0; i < files_[level].size(); i++) {
      FileMetaData* f = files_[level][i];
      assert(f->refs > 0);
      f->refs--;
      if (f->refs <= 0) {
        delete f;
      }
    }
  }
}

// 借助各个文件中记录的最大key，实现二分查找
// 找到某个sstable文件，这个文件满足 largest_key >= internal_key，若有多个文件满足，返回的是最大的index
int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files, const Slice& key) {
  uint32_t left = 0;
  uint32_t right = files.size();
  while (left < right) {
    uint32_t mid = (left + right) / 2;
    const FileMetaData* f = files[mid];
    // 与当前文件的最大key进行比较
    // 1. f.largest < key
    //    目标key在这个mid这个文件后面，且一定不在mid文件中，所以left = mid + 1
    // 2. f.largest >= key
    //    目标key在f文件的前面，且可能就是在f这种文件中，所以执行right = mid
    if (icmp.InternalKeyComparator::Compare(f->largest.Encode(), key) < 0) {
      // Key at "mid.largest" is < "target".  Therefore all
      // files at or before "mid" are uninteresting.
      left = mid + 1;
    } else {
      // Key at "mid.largest" is >= "target".  Therefore all files
      // after "mid" are uninteresting.
      right = mid;
    }
  }
  return right;
}

// user_key大于文件f的最大key
// 即user_key在文件f之后，且不包含文件f
static bool AfterFile(const Comparator* ucmp, const Slice* user_key,
                      const FileMetaData* f) {
  // null user_key occurs before all keys and is therefore never after *f
  return (user_key != nullptr &&
          ucmp->Compare(*user_key, f->largest.user_key()) > 0);
}

// user_key小于文件f的最小key
// 即user_key在文件f之前，且不包含文件f
static bool BeforeFile(const Comparator* ucmp, const Slice* user_key,
                       const FileMetaData* f) {
  // null user_key occurs after all keys and is therefore never before *f
  return (user_key != nullptr &&
          ucmp->Compare(*user_key, f->smallest.user_key()) < 0);
}

// 判断files中是否有文件和[smallest_user_key, largest_user_key]有交叉
// 交叉即可，不需要包含
bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const std::vector<FileMetaData*>& files,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key) {
  const Comparator* ucmp = icmp.user_comparator();
  if (!disjoint_sorted_files) {
    // Need to check against all files
    // 第0层文件各个key之间可能有交叉，无法使用FindFile来二分查找，只能线性遍历
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      if (AfterFile(ucmp, smallest_user_key, f) ||
          BeforeFile(ucmp, largest_user_key, f)) {
        // No overlap
        // smallest_user_key都在当前文件之后了，或者largest_user_key都在当前文件之前了，就一定不会有交叉
      } else {
        return true;  // Overlap
      }
    }
    return false;
  }

  // Binary search over file list
  uint32_t index = 0;
  if (smallest_user_key != nullptr) {
    // Find the earliest possible internal key for smallest_user_key
    // 通过smallest_key进行二分查找，找到文件index
    // 该文件满足 files[index].largest_key >= smallest_key
    // seq在查找时是否是有用的，这里seq设置为kMaxSequenceNumber，使得small_key作为当前user_key中最小的internal_key
    InternalKey small_key(*smallest_user_key, kMaxSequenceNumber,
                          kValueTypeForSeek);
    index = FindFile(icmp, files, small_key.Encode());
  }

  if (index >= files.size()) {
    // beginning of range is after all files, so no overlap.
    return false;
  }

  // largest_user_key不在files[index]文件之前
  // 即largest_user_key >= files[index].smallest_key
  return !BeforeFile(ucmp, largest_user_key, files[index]);
}

// An internal iterator.  For a given version/level pair, yields
// information about the files in the level.  For a given entry, key()
// is the largest key that occurs in the file, and value() is an
// 16-byte value containing the file number and file size, both
// encoded using EncodeFixed64.
// 内部使用的迭代器，用于遍历某个指定层级的的各个sstable文件
// 通过key()获得当前sstable文件的最大key，通过value()获得一个16bytes的数据(包含sstable文件的number和size)
class Version::LevelFileNumIterator : public Iterator {
 public:
  LevelFileNumIterator(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>* flist)
      : icmp_(icmp), flist_(flist), index_(flist->size()) {  // Marks as invalid
  }
  bool Valid() const override { return index_ < flist_->size(); }
  void Seek(const Slice& target) override {
    // 找到某个sstable文件，这个文件满足 largest_key >= internal_key，若有多个文件满足，返回的是最大的index
    index_ = FindFile(icmp_, *flist_, target);
  }
  void SeekToFirst() override { index_ = 0; }
  void SeekToLast() override {
    index_ = flist_->empty() ? 0 : flist_->size() - 1;
  }
  void Next() override {
    assert(Valid());
    index_++;
  }
  void Prev() override {
    assert(Valid());
    if (index_ == 0) {
      index_ = flist_->size();  // Marks as invalid
    } else {
      index_--;
    }
  }
  Slice key() const override {
    assert(Valid());
    return (*flist_)[index_]->largest.Encode();
  }
  Slice value() const override {
    assert(Valid());
    EncodeFixed64(value_buf_, (*flist_)[index_]->number);
    EncodeFixed64(value_buf_ + 8, (*flist_)[index_]->file_size);
    return Slice(value_buf_, sizeof(value_buf_));
  }
  Status status() const override { return Status::OK(); }

 private:
  const InternalKeyComparator icmp_;
  const std::vector<FileMetaData*>* const flist_;
  uint32_t index_;

  // Backing store for value().  Holds the file number and size.
  mutable char value_buf_[16];
};

static Iterator* GetFileIterator(void* arg, const ReadOptions& options,
                                 const Slice& file_value) {
  TableCache* cache = reinterpret_cast<TableCache*>(arg);
  if (file_value.size() != 16) {
    return NewErrorIterator(
        Status::Corruption("FileReader invoked with unexpected value"));
  } else {
    return cache->NewIterator(options, DecodeFixed64(file_value.data()),
                              DecodeFixed64(file_value.data() + 8));
  }
}

// 内部使用(AddIterators)
// 获得一个连续的迭代器(将当前层级的文件的iter拼在一起)
Iterator* Version::NewConcatenatingIterator(const ReadOptions& options,
                                            int level) const {
  // 创建一个二级迭代器
  // 第一级获得sstable文件的number和size
  // 第二级通过table_cache获得该文件的iter
  return NewTwoLevelIterator(
      new LevelFileNumIterator(vset_->icmp_, &files_[level]), &GetFileIterator,
      vset_->table_cache_, options);
}

// 外部使用
// 将当前版本中各个层文件的迭代器都填入iters中，作为整个db迭代器的一部分
void Version::AddIterators(const ReadOptions& options,
                           std::vector<Iterator*>* iters) {
  // Merge all level zero files together since they may overlap
  // merge第0层所有的文件，因为它们很容易重叠
  // 获取第0层所有的文件的迭代器
  for (size_t i = 0; i < files_[0].size(); i++) {
    iters->push_back(vset_->table_cache_->NewIterator(
        options, files_[0][i]->number, files_[0][i]->file_size));
  }

  // For levels > 0, we can use a concatenating iterator that sequentially
  // walks through the non-overlapping files in the level, opening them
  // lazily.
  // 遍历所有层，将每一层的文件作为一个连续的迭代器，只有真正的遍历到才会延迟的打开
  for (int level = 1; level < config::kNumLevels; level++) {
    if (!files_[level].empty()) {
      iters->push_back(NewConcatenatingIterator(options, level));
    }
  }
}

// Callback from TableCache::Get()
namespace {
enum SaverState {
  kNotFound,
  kFound,
  kDeleted,
  kCorrupt,
};
// 记录一次查找的最关键信息
// user_key和val，以及比较函数user_comparator
struct Saver {
  SaverState state;
  const Comparator* ucmp;
  Slice user_key;
  std::string* value;
};
}  // namespace
// 将从table_cache中搜索到的val信息存入saver中
static void SaveValue(void* arg, const Slice& ikey, const Slice& v) {
  Saver* s = reinterpret_cast<Saver*>(arg);
  ParsedInternalKey parsed_key;
  // 将internal_key解析为user_key + seq + type
  if (!ParseInternalKey(ikey, &parsed_key)) {
    s->state = kCorrupt;
  } else {
    // 判断是否是要找的key
    if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {
      // 判断是否是删除状态
      s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
      if (s->state == kFound) {
        s->value->assign(v.data(), v.size());
      }
    }
  }
}

static bool NewestFirst(FileMetaData* a, FileMetaData* b) {
  return a->number > b->number;
}

// 在全部的sstable文件中依次查找目标internal_key
// 按照level层数从小到大，层内根据文件的number按照文件生成的先后，先查找后生成的(即更新的数据)，找到数据即可直接返回
void Version::ForEachOverlapping(Slice user_key, Slice internal_key, void* arg,
                                 bool (*func)(void*, int, FileMetaData*)) {
  const Comparator* ucmp = vset_->icmp_.user_comparator();

  // Search level-0 in order from newest to oldest.
  // 从第0层的文件开始搜索，第0层文件互相有交叉且不保证有序，需要遍历所有文件最后决定从哪里取数据
  std::vector<FileMetaData*> tmp;
  tmp.reserve(files_[0].size());
  for (uint32_t i = 0; i < files_[0].size(); i++) {
    FileMetaData* f = files_[0][i];
    // 待查询的user_key落在当前sstable文件的范围内，将该文件记录下来
    if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
        ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
      tmp.push_back(f);
    }
  }
  if (!tmp.empty()) {
    // 将文件按照number降序排列，先读number大的文件(即更新的文件)
    std::sort(tmp.begin(), tmp.end(), NewestFirst);
    for (uint32_t i = 0; i < tmp.size(); i++) {
      if (!(*func)(arg, 0, tmp[i])) {
        return;
      }
    }
  }

  // Search other levels.
  // 第0层未找到，需要到更高层查找，高层的sstable文件都是按升序存储的，可以使用二分提高查找效率，且对应的val只会出现在一个文件中
  for (int level = 1; level < config::kNumLevels; level++) {
    size_t num_files = files_[level].size();
    if (num_files == 0) continue;

    // Binary search to find earliest index whose largest key >= internal_key.
    // 二分查找，找到某个sstable文件，这个文件满足 largest_key >= internal_key，若有多个文件满足，返回的是最大的index
    // TODO : FindFile使用internal_key,而下面compare使用user_key. 和使用的compare的比较器有关，使用的key需要和比较器对应
    // TODO : 除了0层，后面的层的各个sstable文件的key都是没有重叠的
    uint32_t index = FindFile(vset_->icmp_, files_[level], internal_key);
    if (index < num_files) {
      FileMetaData* f = files_[level][index];
      if (ucmp->Compare(user_key, f->smallest.user_key()) < 0) {
        // All of "f" is past any data for user_key
      } else {
        // 这个文件也满足smallest_key <= user_key.
        // 即 samllest_key <= user_key <= largest_key, 目标key在当前文件内
        if (!(*func)(arg, level, f)) {
          return;
        }
      }
    }
  }
}

// 外部使用，核心函数
// 在某个版本的sstable中查找key对应的数据
Status Version::Get(const ReadOptions& options, const LookupKey& k,
                    std::string* value, GetStats* stats) {
  // 用于记录无效查询，作为seek_compaction的判断条件
  stats->seek_file = nullptr;
  stats->seek_file_level = -1;

  // 记录查找过程中的状态信息
  struct State {
    // 本次查找的关键信息，包含user_key和val，以及比较函数user_comparator
    Saver saver;
    GetStats* stats;
    const ReadOptions* options;
    Slice ikey;
    FileMetaData* last_file_read;
    int last_file_read_level;

    VersionSet* vset;
    Status s;
    bool found;

    // Match函数会被多次调用!!
    // 当前层级的key范围内没有找到目标key需要进一步去更高层级去找
    // 正常的没有找到时返回true
    static bool Match(void* arg, int level, FileMetaData* f) {
      State* state = reinterpret_cast<State*>(arg);

      // Match函数多次调用时使用，记录第一次调用Match时查找的文件的FileMetaData信息和level
      // 作用：UpdateStats使用，用于记录无效查询，作为seek_compaction的判断条件
      if (state->stats->seek_file == nullptr &&
          state->last_file_read != nullptr) {
        // We have had more than one seek for this read.  Charge the 1st file.
        state->stats->seek_file = state->last_file_read;
        state->stats->seek_file_level = state->last_file_read_level;
      }

      state->last_file_read = f;
      state->last_file_read_level = level;

      // 从table_cache中获取指定sstable文件的指定key对一个的数据，并执行SaveValue函数，将目标值放入saver中
      state->s = state->vset->table_cache_->Get(*state->options, f->number,
                                                f->file_size, state->ikey,
                                                &state->saver, SaveValue);
      if (!state->s.ok()) {
        state->found = true;
        return false;
      }
      switch (state->saver.state) {
        case kNotFound:
          // 返回true还会处理其他文件
          return true;  // Keep searching in other files
        case kFound:
          state->found = true;
          return false;
        case kDeleted:
          return false;
        case kCorrupt:
          state->s =
              Status::Corruption("corrupted key for ", state->saver.user_key);
          state->found = true;
          return false;
      }

      // Not reached. Added to avoid false compilation warnings of
      // "control reaches end of non-void function".
      return false;
    }
  };

  State state;
  state.found = false;
  state.stats = stats;
  state.last_file_read = nullptr;
  state.last_file_read_level = -1;

  state.options = &options;
  state.ikey = k.internal_key();
  state.vset = vset_;

  state.saver.state = kNotFound;
  state.saver.ucmp = vset_->icmp_.user_comparator();
  state.saver.user_key = k.user_key();
  state.saver.value = value;

  ForEachOverlapping(state.saver.user_key, state.ikey, &state, &State::Match);

  return state.found ? state.s : Status::NotFound(Slice());
}

// 更新文件被查找的次数，以此决定是否需要合并，seek_compaction模式使用
bool Version::UpdateStats(const GetStats& stats) {
  FileMetaData* f = stats.seek_file;
  if (f != nullptr) {
    f->allowed_seeks--;
    // 该文件达到允许查找次数的上限，记录起来作为下一次合并的文件
    if (f->allowed_seeks <= 0 && file_to_compact_ == nullptr) {
      file_to_compact_ = f;
      file_to_compact_level_ = stats.seek_file_level;
      return true;
    }
  }
  return false;
}

// 外部使用，通过db_iter遍历数据时会有一定比例执行该函数
// 随机采样一些key调用该函数，来决定其对应的文件是否需要合并
bool Version::RecordReadSample(Slice internal_key) {
  ParsedInternalKey ikey;
  if (!ParseInternalKey(internal_key, &ikey)) {
    return false;
  }

  struct State {
    GetStats stats;  // Holds first matching file
    int matches;

    static bool Match(void* arg, int level, FileMetaData* f) {
      State* state = reinterpret_cast<State*>(arg);
      state->matches++;
      if (state->matches == 1) {
        // Remember first match.
        // 记录第一个匹配的文件信息
        state->stats.seek_file = f;
        state->stats.seek_file_level = level;
      }
      // We can stop iterating once we have a second match.
      // 得到两个文件就可以结束逐层逐文件的遍历了
      // 已经可以满足seek_compaction的第一次访问到文件但没有命中的条件
      return state->matches < 2;
    }
  };

  State state;
  state.matches = 0;
  ForEachOverlapping(ikey.user_key, internal_key, &state, &State::Match);

  // Must have at least two matches since we want to merge across
  // files. But what if we have a single file that contains many
  // overwrites and deletions?  Should we have another mechanism for
  // finding such files?
  // 查找这个key会至少处理两个文件，需要判断是否可以将低层级的文件合并掉
  if (state.matches >= 2) {
    // 1MB cost is about 1 seek (see comment in Builder::Apply).
    return UpdateStats(state.stats);
  }
  return false;
}

void Version::Ref() { ++refs_; }

void Version::Unref() {
  assert(this != &vset_->dummy_versions_);
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    delete this;
  }
}

// 判断当前层文件是否有包含[smallest_user_key, largest_user_key]的数据
// 用于MemTable数据生成sstable时的层数的选择(PickLevelForMemTableOutput)
bool Version::OverlapInLevel(int level, const Slice* smallest_user_key,
                             const Slice* largest_user_key) {
  return SomeFileOverlapsRange(vset_->icmp_, (level > 0), files_[level],
                               smallest_user_key, largest_user_key);
}

// 返回一个level，用于放置memtable中范围为[smallest_user_key,largest_user_key]的数据
// 其中有一定的优化策略：
// 同时满足如三个条件，可以将level提升一层
// 1. 与当前层level无重叠
// 2. 与当前层下一层level+1无重叠
// 3. 与当前层的下下一层level+2层的重叠文件大小小于阈值
// 目标：
// - 尽量将新的memtabel数据推到更高层级，以减少数据查询时io的次数
// - 不能处于太高层，某些key修改val数据比较频繁，太高层会导致compaction的io消耗过大
int Version::PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                        const Slice& largest_user_key) {
  int level = 0;
  // 第0层的sstable文件与[smallest_user_key,largest_user_key]有交叉，直接返回第0级别
  // 与第0层已有文件有交叉，跟随其他文件合并即可，无需推到高层
  if (!OverlapInLevel(0, &smallest_user_key, &largest_user_key)) {
    // Push to next level if there is no overlap in next level,
    // and the #bytes overlapping in the level after that are limited.
    // 第0层的所有sstable文件都与[smallest_user_key,largest_user_key]的数据没有任何交叉，继续判断条件1.2和1.3
    InternalKey start(smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey limit(largest_user_key, 0, static_cast<ValueType>(0));
    std::vector<FileMetaData*> overlaps;
    while (level < config::kMaxMemCompactLevel) {
      if (OverlapInLevel(level + 1, &smallest_user_key, &largest_user_key)) {
        // level + 1层级有与[smallest_user_key,largest_user_key]交叉的文件，则直接break，返回当前层级level
        break;
      }
      if (level + 2 < config::kNumLevels) {
        // Check that file does not overlap too many grandparent bytes.
        // 获取[smallest_user_key,largest_user_key]范围数据与level + 2层有交叉的文件列表
        GetOverlappingInputs(level + 2, &start, &limit, &overlaps);
        const int64_t sum = TotalFileSize(overlaps);
        // 有交叉文件大小超过10*max_file_size的大小，直接返回，而不执行level++;
        // TODO: 原因 : 当level + 1的数据向level + 2的数据文件合并时，开销比较大!
        if (sum > MaxGrandParentOverlapBytes(vset_->options_)) {
          break;
        }
      }
      level++;
    }
  }
  return level;
}

// Store in "*inputs" all files in "level" that overlap [begin,end]
// 获取指定层level的所有与key[begin, end]有交集的文件
void Version::GetOverlappingInputs(int level, const InternalKey* begin,
                                   const InternalKey* end,
                                   std::vector<FileMetaData*>* inputs) {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  inputs->clear();
  Slice user_begin, user_end;
  if (begin != nullptr) {
    user_begin = begin->user_key();
  }
  if (end != nullptr) {
    user_end = end->user_key();
  }
  const Comparator* user_cmp = vset_->icmp_.user_comparator();
  for (size_t i = 0; i < files_[level].size();) {
    FileMetaData* f = files_[level][i++];
    const Slice file_start = f->smallest.user_key();
    const Slice file_limit = f->largest.user_key();
    if (begin != nullptr && user_cmp->Compare(file_limit, user_begin) < 0) {
      // "f" is completely before specified range; skip it
      // 当前文件f完全在目标区间的前面，没有交集，直接跳过
    } else if (end != nullptr && user_cmp->Compare(file_start, user_end) > 0) {
      // "f" is completely after specified range; skip it
      // 当前文件f完全在目标区间的后面，没有交集，直接跳过
    } else {
      // 经过上面两个if的判断，当前分支满足条件： user_begin <= file_limit 并且 file_start <= user_end
      // 当前一定有交叉，交叉的范围为  max(user_begin, file_start) ~ min(user_end, file_limit)
      inputs->push_back(f);
      // TODO : ?????
      // 第0层的文件之间可能会有互相交叉，将搜索范围进一步扩充
      if (level == 0) {
        // Level-0 files may overlap each other.  So check if the newly
        // added file has expanded the range.  If so, restart search.
        if (begin != nullptr && user_cmp->Compare(file_start, user_begin) < 0) {
          // file_start < begin, 将待查找的范围扩大为 [file_start, end], 并重新查找
          user_begin = file_start;
          inputs->clear();
          i = 0;
        } else if (end != nullptr &&
                   user_cmp->Compare(file_limit, user_end) > 0) {
          // file_limit > end, 将待查找的范围扩大为 [begin, file_limit], 并重新查找
          user_end = file_limit;
          inputs->clear();
          i = 0;
        }
      }
    }
  }
}

// 输出当前版本各个sstable的meta信息
std::string Version::DebugString() const {
  std::string r;
  for (int level = 0; level < config::kNumLevels; level++) {
    // E.g.,
    //   --- level 1 ---
    //   17:123['a' .. 'd']
    //   20:43['e' .. 'g']
    r.append("--- level ");
    AppendNumberTo(&r, level);
    r.append(" ---\n");
    const std::vector<FileMetaData*>& files = files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      r.push_back(' ');
      AppendNumberTo(&r, files[i]->number);
      r.push_back(':');
      AppendNumberTo(&r, files[i]->file_size);
      r.append("[");
      r.append(files[i]->smallest.DebugString());
      r.append(" .. ");
      r.append(files[i]->largest.DebugString());
      r.append("]\n");
    }
  }
  return r;
}

// A helper class so we can efficiently apply a whole sequence
// of edits to a particular state without creating intermediate
// Versions that contain full copies of the intermediate state.
class VersionSet::Builder {
 private:
  // Helper to sort by v->files_[file_number].smallest
  // 用于座位std::set的排序函数，对于存储的文件排序
  struct BySmallestKey {
    const InternalKeyComparator* internal_comparator;

    bool operator()(FileMetaData* f1, FileMetaData* f2) const {
      int r = internal_comparator->Compare(f1->smallest, f2->smallest);
      if (r != 0) {
        return (r < 0);
      } else {
        // Break ties by file number
        return (f1->number < f2->number);
      }
    }
  };

  typedef std::set<FileMetaData*, BySmallestKey> FileSet;
  // 每一层的状态信息，包含待删除文件和待添加文件(按照各文件的最小key升序排列，最小key相同按照number升序排列)
  struct LevelState {
    std::set<uint64_t> deleted_files;
    FileSet* added_files;
  };

  VersionSet* vset_;
  // 极限版本
  Version* base_;
  LevelState levels_[config::kNumLevels];

 public:
  // Initialize a builder with the files from *base and other info from *vset
  Builder(VersionSet* vset, Version* base) : vset_(vset), base_(base) {
    base_->Ref();
    // 比较器
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < config::kNumLevels; level++) {
      levels_[level].added_files = new FileSet(cmp);
    }
  }

  ~Builder() {
    for (int level = 0; level < config::kNumLevels; level++) {
      const FileSet* added = levels_[level].added_files;
      // TODO : to_unref是否可以省略，直接两步合并???
      std::vector<FileMetaData*> to_unref;
      to_unref.reserve(added->size());
      for (FileSet::const_iterator it = added->begin(); it != added->end();
           ++it) {
        to_unref.push_back(*it);
      }
      delete added;
      for (uint32_t i = 0; i < to_unref.size(); i++) {
        FileMetaData* f = to_unref[i];
        f->refs--;
        if (f->refs <= 0) {
          delete f;
        }
      }
    }
    base_->Unref();
  }

  // Apply all of the edits in *edit to the current state.
  // 在当前基线版本上应用变更edit，并记录在LevelState中
  void Apply(const VersionEdit* edit) {
    // Update compaction pointers
    // 根据edit更新version_set中记录的各层级待合并的key的信息
    // TODO : edit->compact_pointers_的来源
    for (size_t i = 0; i < edit->compact_pointers_.size(); i++) {
      const int level = edit->compact_pointers_[i].first;
      vset_->compact_pointer_[level] =
          edit->compact_pointers_[i].second.Encode().ToString();
    }

    // Delete files
    // 记录待删除文件号
    for (const auto& deleted_file_set_kvp : edit->deleted_files_) {
      const int level = deleted_file_set_kvp.first;
      const uint64_t number = deleted_file_set_kvp.second;
      levels_[level].deleted_files.insert(number);
    }

    // Add new files
    // 记录新增的文件的metadata
    for (size_t i = 0; i < edit->new_files_.size(); i++) {
      const int level = edit->new_files_[i].first;
      FileMetaData* f = new FileMetaData(edit->new_files_[i].second);
      f->refs = 1;

      // We arrange to automatically compact this file after
      // a certain number of seeks.  Let's assume:
      //   (1) One seek costs 10ms
      //   (2) Writing or reading 1MB costs 10ms (100MB/s)
      //   (3) A compaction of 1MB does 25MB of IO:
      //         1MB read from this level
      //         10-12MB read from next level (boundaries may be misaligned)
      //         10-12MB written to next level
      // This implies that 25 seeks cost the same as the compaction
      // of 1MB of data.  I.e., one seek costs approximately the
      // same as the compaction of 40KB of data.  We are a little
      // conservative and allow approximately one seek for every 16KB
      // of data before triggering a compaction.
      // 记录允许查找的次数，超过这个次数就会触发文件合并
      f->allowed_seeks = static_cast<int>((f->file_size / 16384U));
      if (f->allowed_seeks < 100) f->allowed_seeks = 100;

      // 兼容文件number可以复用场景
      levels_[level].deleted_files.erase(f->number);
      levels_[level].added_files->insert(f);
    }
  }

  // Save the current state in *v.
  // 将版本变更信息存入新的version中
  // 只处理add_file,在准备添加文件时MaybeAddFile中再处理delete_file
  void SaveTo(Version* v) {
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < config::kNumLevels; level++) {
      // Merge the set of added files with the set of pre-existing files.
      // Drop any deleted files.  Store the result in *v.
      const std::vector<FileMetaData*>& base_files = base_->files_[level];
      std::vector<FileMetaData*>::const_iterator base_iter = base_files.begin();
      std::vector<FileMetaData*>::const_iterator base_end = base_files.end();
      const FileSet* added_files = levels_[level].added_files;
      // 新的version在原有基础上加上本次变更
      // 并将文件重新排序后加入
      // TODO : 哪个阶段保证的add_file与base中文件的key无交叉???
      // 理论上新增的文件added_files总会大于base的文件(文件number)，不一定!!有文件number重用的问题 ReuseFileNumber
      v->files_[level].reserve(base_files.size() + added_files->size());
      for (const auto& added_file : *added_files) {
        // Add all smaller files listed in base_
        // 先将base中小于当前added_file的文件加入version中
        for (std::vector<FileMetaData*>::const_iterator bpos =
                 std::upper_bound(base_iter, base_end, added_file, cmp);
             base_iter != bpos; ++base_iter) {
          MaybeAddFile(v, level, *base_iter);
        }

        // 将added_file加入version
        MaybeAddFile(v, level, added_file);
      }

      // Add remaining base files
      for (; base_iter != base_end; ++base_iter) {
        MaybeAddFile(v, level, *base_iter);
      }

#ifndef NDEBUG
      // Make sure there is no overlap in levels > 0
      if (level > 0) {
        for (uint32_t i = 1; i < v->files_[level].size(); i++) {
          const InternalKey& prev_end = v->files_[level][i - 1]->largest;
          const InternalKey& this_begin = v->files_[level][i]->smallest;
          if (vset_->icmp_.Compare(prev_end, this_begin) >= 0) {
            std::fprintf(stderr, "overlapping ranges in same level %s vs. %s\n",
                         prev_end.DebugString().c_str(),
                         this_begin.DebugString().c_str());
            std::abort();
          }
        }
      }
#endif
    }
  }

  void MaybeAddFile(Version* v, int level, FileMetaData* f) {
    if (levels_[level].deleted_files.count(f->number) > 0) {
      // File is deleted: do nothing
      // 要删除的文件，在删除列表中，就不用再加入新的版本中，实现新版本删除该文件的能力，但实际该文件并未被删除
    } else {
      std::vector<FileMetaData*>* files = &v->files_[level];
      // 0层以上的sstable文件之间是不允许有key交叉的
      if (level > 0 && !files->empty()) {
        // Must not overlap
        assert(vset_->icmp_.Compare((*files)[files->size() - 1]->largest,
                                    f->smallest) < 0);
      }
      f->refs++;
      files->push_back(f);
    }
  }
};

VersionSet::VersionSet(const std::string& dbname, const Options* options,
                       TableCache* table_cache,
                       const InternalKeyComparator* cmp)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      table_cache_(table_cache),
      icmp_(*cmp),
      next_file_number_(2),
      manifest_file_number_(0),  // Filled by Recover()
      last_sequence_(0),
      log_number_(0),
      prev_log_number_(0),
      descriptor_file_(nullptr),
      descriptor_log_(nullptr),
      dummy_versions_(this),
      current_(nullptr) {
  // 先创建第一个版本，作为version循环链表的头结点
  AppendVersion(new Version(this));
}

VersionSet::~VersionSet() {
  current_->Unref();
  assert(dummy_versions_.next_ == &dummy_versions_);  // List must be empty
  delete descriptor_log_;
  delete descriptor_file_;
}

// 添加一个新的版本
void VersionSet::AppendVersion(Version* v) {
  // Make "v" current
  assert(v->refs_ == 0);
  assert(v != current_);
  if (current_ != nullptr) {
    // 有新版本了，去掉对应当前版本的引用
    current_->Unref();
  }
  current_ = v;
  v->Ref();

  // Append to linked list
  // 将新创建的版本v添加到双向链表的尾部
  v->prev_ = dummy_versions_.prev_;
  v->next_ = &dummy_versions_;
  v->prev_->next_ = v;
  v->next_->prev_ = v;
}

// 外部调用
// 更具变更信息edit创建一个新版本，记录MANIFEST日志，并生效该版本
Status VersionSet::LogAndApply(VersionEdit* edit, port::Mutex* mu) {
  // 完善版本变更内容edit
  if (edit->has_log_number_) {
    assert(edit->log_number_ >= log_number_);
    assert(edit->log_number_ < next_file_number_);
  } else {
    edit->SetLogNumber(log_number_);
  }

  if (!edit->has_prev_log_number_) {
    edit->SetPrevLogNumber(prev_log_number_);
  }

  edit->SetNextFile(next_file_number_);
  edit->SetLastSequence(last_sequence_);

  // 创建一个新版本，并通过Builder将变更信息version_edit作用于该版本
  Version* v = new Version(this);
  {
    Builder builder(this, current_);
    builder.Apply(edit);
    builder.SaveTo(v);
  }
  // 更新compaction_level和compaction_score
  Finalize(v);

  // Initialize new descriptor log file if necessary by creating
  // a temporary file that contains a snapshot of the current version.
  std::string new_manifest_file;
  Status s;
  if (descriptor_log_ == nullptr) {
    // No reason to unlock *mu here since we only hit this path in the
    // first call to LogAndApply (when opening the database).
    // 只有服务启动第一次执行时会进入这里的逻辑
    assert(descriptor_file_ == nullptr);
    // 文件名 = dbname_/MANIFEST-manifest_file_number_
    new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_);
    s = env_->NewWritableFile(new_manifest_file, &descriptor_file_);
    if (s.ok()) {
      descriptor_log_ = new log::Writer(descriptor_file_);
      // 写入快照信息
      // TODO : 该快照是否包含本次新增的version???
      // compact_pointers_在builder.Apply时更新了，但是current_未更新?????
      s = WriteSnapshot(descriptor_log_);
    }
  }

  // Unlock during expensive MANIFEST log write
  {
    mu->Unlock();

    // Write new record to MANIFEST log
    if (s.ok()) {
      // 将当前新的变更信息version_edit追加到MANIFEST log文件中
      std::string record;
      edit->EncodeTo(&record);
      s = descriptor_log_->AddRecord(record);
      if (s.ok()) {
        s = descriptor_file_->Sync();
      }
      if (!s.ok()) {
        Log(options_->info_log, "MANIFEST write: %s\n", s.ToString().c_str());
      }
    }

    // If we just created a new descriptor file, install it by writing a
    // new CURRENT file that points to it.
    // 只有服务启动第一次会执行，记录当前使用的MANIFEST文件是哪一个
    // 创建一个 db_name/CURRENT 文件，其中内容为当前的MANIFEST-manifest_file_number_的文件名
    if (s.ok() && !new_manifest_file.empty()) {
      s = SetCurrentFile(env_, dbname_, manifest_file_number_);
    }

    mu->Lock();
  }

  // Install the new version
  if (s.ok()) {
    // 将新版本加入VersionSet的双向链表中，并使得current_ = v
    AppendVersion(v);
    log_number_ = edit->log_number_;
    prev_log_number_ = edit->prev_log_number_;
  } else {
    delete v;
    if (!new_manifest_file.empty()) {
      // 第一次启动时初始化就失败，需要释放资源，并移除新建的MANIFEST文件
      delete descriptor_log_;
      delete descriptor_file_;
      descriptor_log_ = nullptr;
      descriptor_file_ = nullptr;
      env_->RemoveFile(new_manifest_file);
    }
  }

  return s;
}

// DBImpl::Recover中调用
// 根据日志恢复出版本管理器(version_set)，并将所有版本信息合成一个新的版本
Status VersionSet::Recover(bool* save_manifest) {
  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    void Corruption(size_t bytes, const Status& s) override {
      if (this->status->ok()) *this->status = s;
    }
  };

  // Read "CURRENT" file, which contains a pointer to the current manifest file
  // 读取 db_name_/CURRENT文件，获取当前有效的manifest文件
  // 文件的内容为 "MANIFEST-manifest_file_number_"
  std::string current;
  Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current);
  if (!s.ok()) {
    return s;
  }
  if (current.empty() || current[current.size() - 1] != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  current.resize(current.size() - 1);

  std::string dscname = dbname_ + "/" + current;
  // 顺序读取manifest文件
  SequentialFile* file;
  s = env_->NewSequentialFile(dscname, &file);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      return Status::Corruption("CURRENT points to a non-existent file",
                                s.ToString());
    }
    return s;
  }

  bool have_log_number = false;
  bool have_prev_log_number = false;
  bool have_next_file = false;
  bool have_last_sequence = false;
  uint64_t next_file = 0;
  uint64_t last_sequence = 0;
  uint64_t log_number = 0;
  uint64_t prev_log_number = 0;
  Builder builder(this, current_);
  int read_records = 0;

  {
    LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(file, &reporter, true /*checksum*/,
                       0 /*initial_offset*/);
    Slice record;
    std::string scratch;
    // 从manifest文件中逐个record读取，并将内容解析至VersionEdit中
    // manifest文件按照版本先后顺序存储，因此顺序读取record，即一步步从恢复到当前最新版本
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
      ++read_records;
      VersionEdit edit;
      s = edit.DecodeFrom(record);
      if (s.ok()) {
        // 判断恢复出的comparator与当前的comparator是否相同
        if (edit.has_comparator_ &&
            edit.comparator_ != icmp_.user_comparator()->Name()) {
          s = Status::InvalidArgument(
              edit.comparator_ + " does not match existing comparator ",
              icmp_.user_comparator()->Name());
        }
      }

      if (s.ok()) {
        // 将VersionEdit中的内容暂存在builder中，后续调用builder.SaveTo()生成新的version
        // 此处通过while循环读取多个record，将这么多个版本的version_edit都存入builder中
        // 最终统一调用一次SaveTo，即恢复时将日志文件中的多个版本整合成一个新的版本
        builder.Apply(&edit);
      }

      if (edit.has_log_number_) {
        log_number = edit.log_number_;
        have_log_number = true;
      }

      if (edit.has_prev_log_number_) {
        prev_log_number = edit.prev_log_number_;
        have_prev_log_number = true;
      }

      if (edit.has_next_file_number_) {
        next_file = edit.next_file_number_;
        have_next_file = true;
      }

      if (edit.has_last_sequence_) {
        last_sequence = edit.last_sequence_;
        have_last_sequence = true;
      }
    }
  }
  delete file;
  file = nullptr;

  if (s.ok()) {
    if (!have_next_file) {
      s = Status::Corruption("no meta-nextfile entry in descriptor");
    } else if (!have_log_number) {
      s = Status::Corruption("no meta-lognumber entry in descriptor");
    } else if (!have_last_sequence) {
      s = Status::Corruption("no last-sequence-number entry in descriptor");
    }

    if (!have_prev_log_number) {
      prev_log_number = 0;
    }

    // 将next_file_number设置为 max(pre_log_number +1, log_number + 1, next_file_number)
    MarkFileNumberUsed(prev_log_number);
    MarkFileNumberUsed(log_number);
  }

  if (s.ok()) {
    // 生成新的版本，该版本为manifest文件中所有版本的结合
    Version* v = new Version(this);
    builder.SaveTo(v);
    // Install recovered version
    // 更新compaction_level和compaction_score
    Finalize(v);
    // 将版本v加入version_set中
    AppendVersion(v);
    manifest_file_number_ = next_file;
    next_file_number_ = next_file + 1;
    last_sequence_ = last_sequence;
    log_number_ = log_number;
    prev_log_number_ = prev_log_number;

    // See if we can reuse the existing MANIFEST file.

    if (ReuseManifest(dscname, current)) {
      // No need to save new manifest
    } else {
      *save_manifest = true;
    }
  } else {
    std::string error = s.ToString();
    Log(options_->info_log, "Error recovering version set with %d records: %s",
        read_records, error.c_str());
  }

  return s;
}

// dscname = db_name + "/" + dscbase
// dscbase = "MANIFEST-manifest_file_number_"
bool VersionSet::ReuseManifest(const std::string& dscname,
                               const std::string& dscbase) {
  if (!options_->reuse_logs) {
    return false;
  }
  FileType manifest_type;
  uint64_t manifest_number;
  uint64_t manifest_size;
  if (!ParseFileName(dscbase, &manifest_number, &manifest_type) ||
      manifest_type != kDescriptorFile ||
      !env_->GetFileSize(dscname, &manifest_size).ok() ||
      // Make new compacted MANIFEST if old one is too big
      // mainifest文件超过了2Mb，就不复用了
      manifest_size >= TargetFileSize(options_)) {
    return false;
  }

  assert(descriptor_file_ == nullptr);
  assert(descriptor_log_ == nullptr);
  // 将当前的descriptor_file_指向复用的文件dscname，后续manifest内容直接追加在该文件后面
  Status r = env_->NewAppendableFile(dscname, &descriptor_file_);
  if (!r.ok()) {
    Log(options_->info_log, "Reuse MANIFEST: %s\n", r.ToString().c_str());
    assert(descriptor_file_ == nullptr);
    return false;
  }

  Log(options_->info_log, "Reusing MANIFEST %s\n", dscname.c_str());
  descriptor_log_ = new log::Writer(descriptor_file_, manifest_size);
  manifest_file_number_ = manifest_number;
  return true;
}

void VersionSet::MarkFileNumberUsed(uint64_t number) {
  if (next_file_number_ <= number) {
    next_file_number_ = number + 1;
  }
}

// 设置当前版本的compaction相关的信息
// 下一次compact的level和score
void VersionSet::Finalize(Version* v) {
  // Precomputed best level for next compaction
  int best_level = -1;
  double best_score = -1;

  // 最顶层kNumLevels，没法继续合并了，无需处理
  for (int level = 0; level < config::kNumLevels - 1; level++) {
    double score;
    if (level == 0) {
      // We treat level-0 specially by bounding the number of files
      // instead of number of bytes for two reasons:
      //
      // (1) With larger write-buffer sizes, it is nice not to do too
      // many level-0 compactions.
      //
      // (2) The files in level-0 are merged on every read and
      // therefore we wish to avoid too many files when the individual
      // file size is small (perhaps because of a small write-buffer
      // setting, or very high compression ratios, or lots of
      // overwrites/deletions).
      // 第0层区别于其它层，将当前层文件的大小改为当前层文件的个数
      // 第0层的sstable文件是有交叉的，文件数量过多会影响查询效率
      score = v->files_[level].size() /
              static_cast<double>(config::kL0_CompactionTrigger);
    } else {
      // Compute the ratio of current size to size limit.
      // 第0层以上，关注当前层内文件的总大小
      const uint64_t level_bytes = TotalFileSize(v->files_[level]);
      score =
          static_cast<double>(level_bytes) / MaxBytesForLevel(options_, level);
    }

    // score越大，表示越需要合并
    if (score > best_score) {
      best_level = level;
      best_score = score;
    }
  }

  v->compaction_level_ = best_level;
  v->compaction_score_ = best_score;
}

// 生成快照信息文件
// 在当前version下，生成一个version_edit并存储
Status VersionSet::WriteSnapshot(log::Writer* log) {
  // TODO: Break up into multiple records to reduce memory usage on recovery?

  // Save metadata
  VersionEdit edit;
  edit.SetComparatorName(icmp_.user_comparator()->Name());

  // Save compaction pointers
  for (int level = 0; level < config::kNumLevels; level++) {
    if (!compact_pointer_[level].empty()) {
      InternalKey key;
      key.DecodeFrom(compact_pointer_[level]);
      edit.SetCompactPointer(level, key);
    }
  }

  // Save files
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = current_->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      edit.AddFile(level, f->number, f->file_size, f->smallest, f->largest);
    }
  }

  std::string record;
  edit.EncodeTo(&record);
  return log->AddRecord(record);
}

int VersionSet::NumLevelFiles(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return current_->files_[level].size();
}

// 获取每层文件的数量
const char* VersionSet::LevelSummary(LevelSummaryStorage* scratch) const {
  // Update code if kNumLevels changes
  static_assert(config::kNumLevels == 7, "");
  std::snprintf(
      scratch->buffer, sizeof(scratch->buffer), "files[ %d %d %d %d %d %d %d ]",
      int(current_->files_[0].size()), int(current_->files_[1].size()),
      int(current_->files_[2].size()), int(current_->files_[3].size()),
      int(current_->files_[4].size()), int(current_->files_[5].size()),
      int(current_->files_[6].size()));
  return scratch->buffer;
}

// 获取所有比ikey小的数据所占用的文件大小(近似值)
uint64_t VersionSet::ApproximateOffsetOf(Version* v, const InternalKey& ikey) {
  uint64_t result = 0;
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = v->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      if (icmp_.Compare(files[i]->largest, ikey) <= 0) {
        // Entire file is before "ikey", so just add the file size
        // 当前文件在待查询ikey之前，且无交集，记录当前文件的大小
        result += files[i]->file_size;
      } else if (icmp_.Compare(files[i]->smallest, ikey) > 0) {
        // Entire file is after "ikey", so ignore
        // 当前文件在待查询ikey之后，且无交集，直接忽略
        if (level > 0) {
          // Files other than level 0 are sorted by meta->smallest, so
          // no further files in this level will contain data for
          // "ikey".
          // 除了level0层之外，其余层的文件都是有序且不交叉排列的
          // 因此，当前层后续的文件一定也都在待查询ikey之后，且无交集
          break;
        }
      } else {
        // "ikey" falls in the range for this table.  Add the
        // approximate offset of "ikey" within the table.
        // 待查询ikey刚好落在当前sstable文件的范围内，获得ikey在当前文件中的近似的offset
        Table* tableptr;
        // 获取指定文件的迭代器iter,通过iter可以获得指定文件中一条一条的kv数据
        // *tableptr = table
        Iterator* iter = table_cache_->NewIterator(
            ReadOptions(), files[i]->number, files[i]->file_size, &tableptr);
        if (tableptr != nullptr) {
          // ApproximateOffsetOf 获取ikey在当前sstable文件中的近似offset，offset为block级别
          result += tableptr->ApproximateOffsetOf(ikey.Encode());
        }
        delete iter;
      }
    }
  }
  return result;
}

// 获取当前有效的文件编号，包含各个版本各个层级
void VersionSet::AddLiveFiles(std::set<uint64_t>* live) {
  // 遍历所有的版本version，从旧到新
  for (Version* v = dummy_versions_.next_; v != &dummy_versions_;
       v = v->next_) {
    for (int level = 0; level < config::kNumLevels; level++) {
      const std::vector<FileMetaData*>& files = v->files_[level];
      for (size_t i = 0; i < files.size(); i++) {
        live->insert(files[i]->number);
      }
    }
  }
}

// 获得当前版本的第level层文件的总大小
int64_t VersionSet::NumLevelBytes(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return TotalFileSize(current_->files_[level]);
}

// 当前未使用
// 获得各层中一个文件与更高一层文件所重叠文件的最大size
int64_t VersionSet::MaxNextLevelOverlappingBytes() {
  int64_t result = 0;
  std::vector<FileMetaData*> overlaps;
  for (int level = 1; level < config::kNumLevels - 1; level++) {
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      const FileMetaData* f = current_->files_[level][i];
      // 获得当前文件与其上一层有重叠的文件的列表
      current_->GetOverlappingInputs(level + 1, &f->smallest, &f->largest,
                                     &overlaps);
      // 获得与上一层有重叠的文件列表所对应的文件总大小
      const int64_t sum = TotalFileSize(overlaps);
      if (sum > result) {
        result = sum;
      }
    }
  }
  return result;
}

// Stores the minimal range that covers all entries in inputs in
// *smallest, *largest.
// REQUIRES: inputs is not empty
// 获得一个能够包含文件列表inputs中所有key的最小范围[smallest, largest]
void VersionSet::GetRange(const std::vector<FileMetaData*>& inputs,
                          InternalKey* smallest, InternalKey* largest) {
  assert(!inputs.empty());
  smallest->Clear();
  largest->Clear();
  for (size_t i = 0; i < inputs.size(); i++) {
    FileMetaData* f = inputs[i];
    if (i == 0) {
      *smallest = f->smallest;
      *largest = f->largest;
    } else {
      if (icmp_.Compare(f->smallest, *smallest) < 0) {
        *smallest = f->smallest;
      }
      if (icmp_.Compare(f->largest, *largest) > 0) {
        *largest = f->largest;
      }
    }
  }
}

// Stores the minimal range that covers all entries in inputs1 and inputs2
// in *smallest, *largest.
// REQUIRES: inputs is not empty
// 获得一个能够包含两个文件列表inputs1,inputs2中所有key的最小范围[smallest, largest]
void VersionSet::GetRange2(const std::vector<FileMetaData*>& inputs1,
                           const std::vector<FileMetaData*>& inputs2,
                           InternalKey* smallest, InternalKey* largest) {
  std::vector<FileMetaData*> all = inputs1;
  all.insert(all.end(), inputs2.begin(), inputs2.end());
  GetRange(all, smallest, largest);
}

// 外部调用
// 根据输入的Compaction中的合并文件列表(inputs_)获得kv的迭代器，用于生成新的合并后的sstable文件
// 返回的迭代器类型是MergingIterator类型
Iterator* VersionSet::MakeInputIterator(Compaction* c) {
  ReadOptions options;
  options.verify_checksums = options_->paranoid_checks;
  options.fill_cache = false;

  // Level-0 files have to be merged together.  For other levels,
  // we will make a concatenating iterator per level.
  // TODO(opt): use concatenating iterator for level-0 if there is no overlap
  // level0中各个文件之间有交叉，无法保证有序，所有不能使用concatenating_iterator级联迭代器将各个文件串联
  // 所以第0层需要为每一个文件创建一个迭代器
  const int space = (c->level() == 0 ? c->inputs_[0].size() + 1 : 2);
  Iterator** list = new Iterator*[space];
  int num = 0;
  // which = 0 表示当前层，which = 1 表示下一层
  for (int which = 0; which < 2; which++) {
    if (!c->inputs_[which].empty()) {
      if (c->level() + which == 0) {
        // 第0层需要特殊处理，为每一个文件创建一个迭代器
        const std::vector<FileMetaData*>& files = c->inputs_[which];
        for (size_t i = 0; i < files.size(); i++) {
          list[num++] = table_cache_->NewIterator(options, files[i]->number,
                                                  files[i]->file_size);
        }
      } else {
        // Create concatenating iterator for the files from this level
        // 将当前层级的所有待处理文件(inputs_[which])组合成一个连续的迭代器
        // 该迭代器是个二级迭代器(当前层的文件序列 -> 每个文件中的kv序列)，通过它直接可以获得一条条kv数据
        list[num++] = NewTwoLevelIterator(
            new Version::LevelFileNumIterator(icmp_, &c->inputs_[which]),
            &GetFileIterator, table_cache_, options);
      }
    }
  }
  assert(num <= space);
  // 将上述获得的迭代器组合在一起，形成一个新的迭代器result
  Iterator* result = NewMergingIterator(&icmp_, list, num);
  delete[] list;
  return result;
}

// 获取当前版本应该要执行compaction的信息，包含待合并的文件信息等
Compaction* VersionSet::PickCompaction() {
  Compaction* c;
  int level;

  // We prefer compactions triggered by too much data in a level over
  // the compactions triggered by seeks.
  // compaction的两种类型 优先级 size > seek
  // 1. 当前层文件总大小过大(0层是文件个数)
  // 2. 当前层文件被无效查询次数过多
  const bool size_compaction = (current_->compaction_score_ >= 1);
  const bool seek_compaction = (current_->file_to_compact_ != nullptr);
  if (size_compaction) {
    // sstable文件过大(0层是文件个数)，需要触发合并
    // 待合并的层为level
    level = current_->compaction_level_;
    assert(level >= 0);
    assert(level + 1 < config::kNumLevels);
    c = new Compaction(options_, level);

    // Pick the first file that comes after compact_pointer_[level]
    // 通过compact_pointer_实现本层文件从小到大轮流合并(Wrap-around)的效果
    // compact_pointer记录上次触发本层合并包含文件中的最大key，那么本次就会执行比compact_pointer更大的文件
    // 直到没有文件大于compact_pointer_时，落入c->inputs_[0].empty()的分支
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      FileMetaData* f = current_->files_[level][i];
      if (compact_pointer_[level].empty() ||
          icmp_.Compare(f->largest.Encode(), compact_pointer_[level]) > 0) {
        // 将大于compact_pointer_的第一个文件作为本次需要合并的文件
        // compact_pointer_保持递增
        c->inputs_[0].push_back(f);
        break;
      }
    }
    // 触发下一次循环，从本层第一个文件开始
    if (c->inputs_[0].empty()) {
      // Wrap-around to the beginning of the key space
      c->inputs_[0].push_back(current_->files_[level][0]);
    }
    // size_compaction模式下 input[0].size() === 1 , 每次合并只会处理本层的1个文件
    // 后续还有SetupOtherInputs还会根据此文件继续扩充
  } else if (seek_compaction) {
    // 已经精准的知道是哪个文件需要合并了，即file_to_compact_
    level = current_->file_to_compact_level_;
    c = new Compaction(options_, level);
    c->inputs_[0].push_back(current_->file_to_compact_);
  } else {
    return nullptr;
  }

  c->input_version_ = current_;
  c->input_version_->Ref();

  // Files in level 0 may overlap each other, so pick up all overlapping ones
  // 第0层各个文件之间可能有交叉，根据待合并文件c->inputs_[0][0]进行扩充范围(和当前有交叉的文件，直接一个批次处理)
  if (level == 0) {
    InternalKey smallest, largest;
    GetRange(c->inputs_[0], &smallest, &largest);
    // Note that the next call will discard the file we placed in
    // c->inputs_[0] earlier and replace it with an overlapping set
    // which will include the picked file.
    // 获取指定层level的所有与key[smallest, largest]有交集的文件
    current_->GetOverlappingInputs(0, &smallest, &largest, &c->inputs_[0]);
    assert(!c->inputs_[0].empty());
  }

  // 扩充合并文件列表
  // 执行流程：level层 -> level层扩充边界 -> 对应level+1层 -> level+1层扩充边界 -> 根据level+1层反查level层 -> level层扩充边界
  SetupOtherInputs(c);

  return c;
}

// Finds the largest key in a vector of files. Returns true if files is not
// empty.
// 内部使用
// 获取待合并文件列表中最大的internal_key，比较时包含对于同user_key中seq的判断
bool FindLargestKey(const InternalKeyComparator& icmp,
                    const std::vector<FileMetaData*>& files,
                    InternalKey* largest_key) {
  if (files.empty()) {
    return false;
  }
  *largest_key = files[0]->largest;
  for (size_t i = 1; i < files.size(); ++i) {
    FileMetaData* f = files[i];
    if (icmp.Compare(f->largest, *largest_key) > 0) {
      *largest_key = f->largest;
    }
  }
  return true;
}

// Finds minimum file b2=(l2, u2) in level file for which l2 > u1 and
// user_key(l2) = user_key(u1)
// 在本层文件列表中找到一个文件, 这个文件满足下面两个条件:
// 1.该文件最小key的user_key和largest_key的user_key相同
// 2.该文件最小key的版本号是小于largest_key版本号的最大值
FileMetaData* FindSmallestBoundaryFile(
    const InternalKeyComparator& icmp,
    const std::vector<FileMetaData*>& level_files,
    const InternalKey& largest_key) {
  const Comparator* user_cmp = icmp.user_comparator();
  FileMetaData* smallest_boundary_file = nullptr;
  for (size_t i = 0; i < level_files.size(); ++i) {
    FileMetaData* f = level_files[i];
    if (icmp.Compare(f->smallest, largest_key) > 0 &&
        user_cmp->Compare(f->smallest.user_key(), largest_key.user_key()) ==
            0) {
      // 当前文件最小的internal_key大于largest_key, 并且最小的internal_key的user_key等于largeset_key的user_key
      // 即当前文件的最小key和largest_key是同一个user_key,只不过版本seq更老
      if (smallest_boundary_file == nullptr ||
          icmp.Compare(f->smallest, smallest_boundary_file->smallest) < 0) {
        // 找到一个版本最新的数据，即seq最大的，对应internal_key更小
        smallest_boundary_file = f;
      }
    }
  }
  return smallest_boundary_file;
}

// Extracts the largest file b1 from |compaction_files| and then searches for a
// b2 in |level_files| for which user_key(u1) = user_key(l2). If it finds such a
// file b2 (known as a boundary file) it adds it to |compaction_files| and then
// searches again using this new upper bound.
//
// If there are two blocks, b1=(l1, u1) and b2=(l2, u2) and
// user_key(u1) = user_key(l2), and if we compact b1 but not b2 then a
// subsequent get operation will yield an incorrect result because it will
// return the record from b2 in level i rather than from b1 because it searches
// level by level for records matching the supplied user key.
//
// parameters:
//   in     level_files:      List of files to search for boundary files.
//   in/out compaction_files: List of files to extend by adding boundary files.
// 扩充文件合并列表[必须，非效率优化]
// 若本层存在一个文件b2的最小值与合并列表中最大值的user_key相同，那么该文件也需要加入合并列表，同时合并
// 说是同时合并，由于user_key相同，在合并过程中只会保留一个最新版本的。
// 必要性：文件b2的最小key与合并列表中最大值largest_key的user_key的相同，且比较internal_key是b2的最小key大于合并列表中最大值largest_key
//        那么只有一个可能性，b2文件中存着更老版本(seq更新)user_key的信息，若本次合并不一起处理，继续留在本level
//        那么后续查找这个user_key时会首先查找到b2这里更老版本的user_key，导致版本问题!!!!
void AddBoundaryInputs(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>& level_files,
                       std::vector<FileMetaData*>* compaction_files) {
  InternalKey largest_key;

  // Quick return if compaction_files is empty.
  // 获得待合并文件列表中最大的internal_key
  if (!FindLargestKey(icmp, *compaction_files, &largest_key)) {
    return;
  }

  // 遍历本层所有的文件列表，若存在下一个文件与当前largest_key是同一个user_key,只是版本更老，则该文件也纳入合并列表
  bool continue_searching = true;
  while (continue_searching) {
    // 在本层文件列表中找到一个文件, 这个文件满足下面两个条件:
    // 1.该文件最小key的user_key和largest_key的user_key相同
    // 2.该文件最小key的版本号是小于largest_key版本号的最大值，对应icmp(f->small_key, largest_key) > 0
    FileMetaData* smallest_boundary_file =
        FindSmallestBoundaryFile(icmp, level_files, largest_key);

    // If a boundary file was found advance largest_key, otherwise we're done.
    // 存在这种文件，则一起加入合并列表，并更新largest_key继续查找，否则直接结束。
    if (smallest_boundary_file != NULL) {
      compaction_files->push_back(smallest_boundary_file);
      largest_key = smallest_boundary_file->largest;
    } else {
      continue_searching = false;
    }
  }
}

// 设置其余需要加入本次合并的文件
void VersionSet::SetupOtherInputs(Compaction* c) {
  const int level = c->level();
  InternalKey smallest, largest;

  // 判断边界，边界user_key相同带上一起合并(必须，非效率优化)
  AddBoundaryInputs(icmp_, current_->files_[level], &c->inputs_[0]);
  // 获取当前待合并文件的internal_key区间
  GetRange(c->inputs_[0], &smallest, &largest);

  // 计算level+1层与待合并区间有交集的文件列表，放入input_[1]
  current_->GetOverlappingInputs(level + 1, &smallest, &largest,
                                 &c->inputs_[1]);
  // 判断边界，边界user_key相同带上一起合并
  // TODO : 合并完还在level+1层，非必要????
  AddBoundaryInputs(icmp_, current_->files_[level + 1], &c->inputs_[1]);

  // Get entire range covered by compaction
  InternalKey all_start, all_limit;
  // 获得level层和level+1层待合并文件包含key的范围
  GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);

  // See if we can grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up.
  // 根据预定的level和level+1层的文件的key的范围，再次反查level层与此有交叉的文件列表
  // (反正level+1层待合并的文件数确定，能多带一个level层的文件就多带一个)
  if (!c->inputs_[1].empty()) {
    std::vector<FileMetaData*> expanded0;
    // 再次反查level与合并区间有交叉的文件列表
    current_->GetOverlappingInputs(level, &all_start, &all_limit, &expanded0);
    AddBoundaryInputs(icmp_, current_->files_[level], &expanded0);
    const int64_t inputs0_size = TotalFileSize(c->inputs_[0]);
    const int64_t inputs1_size = TotalFileSize(c->inputs_[1]);
    const int64_t expanded0_size = TotalFileSize(expanded0);
    // 反查后包含的level文件数更多了，且此次合并文件的总大小在限制范围内(25Mb)，就key将level层的合并文件列表更新为expanded
    if (expanded0.size() > c->inputs_[0].size() &&
        inputs1_size + expanded0_size <
            ExpandedCompactionByteSizeLimit(options_)) {
      InternalKey new_start, new_limit;
      GetRange(expanded0, &new_start, &new_limit);
      std::vector<FileMetaData*> expanded1;
      // 根据新的level层合并列表expanded0，再次获取level+1层与之有交叉的文件列表
      current_->GetOverlappingInputs(level + 1, &new_start, &new_limit,
                                     &expanded1);
      AddBoundaryInputs(icmp_, current_->files_[level + 1], &expanded1);
      // 通过expanded0获取的level+1层的交叉的文件数和之前是一样的，即表明对于level+1层是没有多余开销的，此时可以放心使用expand0
      if (expanded1.size() == c->inputs_[1].size()) {
        Log(options_->info_log,
            "Expanding@%d %d+%d (%ld+%ld bytes) to %d+%d (%ld+%ld bytes)\n",
            level, int(c->inputs_[0].size()), int(c->inputs_[1].size()),
            long(inputs0_size), long(inputs1_size), int(expanded0.size()),
            int(expanded1.size()), long(expanded0_size), long(inputs1_size));
        smallest = new_start;
        largest = new_limit;
        // 更新合并文件列表
        c->inputs_[0] = expanded0;
        // TODO : inputs_[1] 和 expanded1 应该是一致的??? 会有相等的情况吗???
        c->inputs_[1] = expanded1;
        GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);
      }
    }
  }

  // Compute the set of grandparent files that overlap this compaction
  // (parent == level+1; grandparent == level+2)
  // 更新与grandparent层级有交叉的文件数
  // 通过该信息判断 1.是否可以直接移动 2.是否需要再重新生成新的合并文件(每个新的合并文件大小/重叠大小有限制)
  if (level + 2 < config::kNumLevels) {
    current_->GetOverlappingInputs(level + 2, &all_start, &all_limit,
                                   &c->grandparents_);
  }

  // Update the place where we will do the next compaction for this level.
  // We update this immediately instead of waiting for the VersionEdit
  // to be applied so that if the compaction fails, we will try a different
  // key range next time.
  // 更新记录本层合并的最大的internal_key，用于实现本层文件从小到大轮流合并(Wrap-around)的效果
  compact_pointer_[level] = largest.Encode().ToString();
  c->edit_.SetCompactPointer(level, largest);
}

// 根据指定的level层和internal_key的范围，生成待合并信息Compaction
Compaction* VersionSet::CompactRange(int level, const InternalKey* begin,
                                     const InternalKey* end) {
  std::vector<FileMetaData*> inputs;
  current_->GetOverlappingInputs(level, begin, end, &inputs);
  if (inputs.empty()) {
    return nullptr;
  }

  // Avoid compacting too much in one shot in case the range is large.
  // But we cannot do this for level-0 since level-0 files can overlap
  // and we must not pick one file and drop another older file if the
  // two files overlap.
  // 限制非0层合并文件总大小，避免一次合并太多
  if (level > 0) {
    const uint64_t limit = MaxFileSizeForLevel(options_, level);
    uint64_t total = 0;
    for (size_t i = 0; i < inputs.size(); i++) {
      uint64_t s = inputs[i]->file_size;
      total += s;
      if (total >= limit) {
        inputs.resize(i + 1);
        break;
      }
    }
  }

  Compaction* c = new Compaction(options_, level);
  c->input_version_ = current_;
  c->input_version_->Ref();
  c->inputs_[0] = inputs;
  // 扩充合并文件列表
  SetupOtherInputs(c);
  return c;
}

Compaction::Compaction(const Options* options, int level)
    : level_(level),
      // 配置的固定值max_file_size = 2 * 1024 * 1024;
      // TODO : 应该随着level的提升，逐渐增大合并后文件的大小，但当前是固定的大小???
      max_output_file_size_(MaxFileSizeForLevel(options, level)),
      input_version_(nullptr),
      grandparent_index_(0),
      seen_key_(false),
      overlapped_bytes_(0) {
  for (int i = 0; i < config::kNumLevels; i++) {
    level_ptrs_[i] = 0;
  }
}

Compaction::~Compaction() {
  if (input_version_ != nullptr) {
    input_version_->Unref();
  }
}

// 优化策略，特殊情况下合并操作可以简化为简单的移动
bool Compaction::IsTrivialMove() const {
  // 获得version_set
  const VersionSet* vset = input_version_->vset_;
  // Avoid a move if there is lots of overlapping grandparent data.
  // Otherwise, the move could create a parent file that will require
  // a very expensive merge later on.
  // 当前层只有一个待合并文件，上一层没有与该文件有交集的文件
  // 并且上上一层与该文件交集的总文件大小于10*max_file_size = 10 * 2 * 1024 * 1024 = 20Mb时才可以直接移动
  // 对于上上一层级有大量数据重叠的情况，应该避免移动，因为会对后续的合并会造成很大的开销
  return (num_input_files(0) == 1 && num_input_files(1) == 0 &&
          TotalFileSize(grandparents_) <=
              MaxGrandParentOverlapBytes(vset->options_));
}

// 加入删除文件列表
// 执行合并操作后，将合并前的文件都加入删除列表
void Compaction::AddInputDeletions(VersionEdit* edit) {
  for (int which = 0; which < 2; which++) {
    for (size_t i = 0; i < inputs_[which].size(); i++) {
      edit->RemoveFile(level_ + which, inputs_[which][i]->number);
    }
  }
}

// 比较user_key是否落在更高层[level + 2, max_level)的各个文件中，用于判断delete状态的key是否可以从sstable中删除
// 返回true表示未落在任何文件范围内
// 返回false表示落在了某一个文件范围内
bool Compaction::IsBaseLevelForKey(const Slice& user_key) {
  // Maybe use binary search to find right entry instead of linear search?
  // 获得比较器
  const Comparator* user_cmp = input_version_->vset_->icmp_.user_comparator();
  for (int lvl = level_ + 2; lvl < config::kNumLevels; lvl++) {
    const std::vector<FileMetaData*>& files = input_version_->files_[lvl];
    // level_ptrs_为优化策略
    // 由于IsBaseLevelForKey函数调用时，传入的user_key是保证升序的，而且每一层文件sstable文件也是升序排列的
    // 因此，可以使用level_ptrs_记录上一个user_key遍历到的文件的序号，下一个user_key直接根据level_ptrs_的位置开始判断
    while (level_ptrs_[lvl] < files.size()) {
      FileMetaData* f = files[level_ptrs_[lvl]];
      if (user_cmp->Compare(user_key, f->largest.user_key()) <= 0) {
        // We've advanced far enough
        if (user_cmp->Compare(user_key, f->smallest.user_key()) >= 0) {
          // Key falls in this file's range, so definitely not base level
          // user_key落在了第lvl层级的第level_ptrs_[lvl]个文件的范围内
          return false;
        }
        // user_key小于第lvl层级的第level_ptrs_[lvl]个文件的最小值，未落在当前层的任何一个文件中，直接break，去比较更高层级
        break;
      }
      // user_key大于第lvl层级的第level_ptrs_[lvl]个文件的最大值，不会落在当前文件
      // 继续与当前层级的下一个文件做判断
      level_ptrs_[lvl]++;
    }
  }
  return true;
}

// 当新合并的文件(level+1层)与level+2层重叠的文件大小超过20Mb，则需要重新生成一个新合并的文件了
// 以防止下一次level+1和level+2合并时牵扯的文件过多
bool Compaction::ShouldStopBefore(const Slice& internal_key) {
  const VersionSet* vset = input_version_->vset_;
  // Scan to find earliest grandparent file that contains key.
  const InternalKeyComparator* icmp = &vset->icmp_;
  // grandparents_为第level_+2层与当前待合并key范围有交集的文件列表
  // 统计比internal_key小的文件的总大小，这些文件和internal_key无交集
  // 由于调用函数ShouldStopBefore的internal_key必定是升序的
  // 因此使用grandparent_index_，overlapped_bytes_记录前面已经计算过的部分，进行优化，减少重复计算
  while (grandparent_index_ < grandparents_.size() &&
         icmp->Compare(internal_key,
                       grandparents_[grandparent_index_]->largest.Encode()) >
             0) {
    if (seen_key_) {
      overlapped_bytes_ += grandparents_[grandparent_index_]->file_size;
    }
    grandparent_index_++;
  }
  seen_key_ = true;

  if (overlapped_bytes_ > MaxGrandParentOverlapBytes(vset->options_)) {
    // Too much overlap for current output; start new output
    overlapped_bytes_ = 0;
    return true;
  } else {
    return false;
  }
}

void Compaction::ReleaseInputs() {
  if (input_version_ != nullptr) {
    input_version_->Unref();
    input_version_ = nullptr;
  }
}

}  // namespace leveldb
