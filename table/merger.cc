// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/merger.h"

#include "leveldb/comparator.h"
#include "leveldb/iterator.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {
class MergingIterator : public Iterator {
 public:
  MergingIterator(const Comparator* comparator, Iterator** children, int n)
      : comparator_(comparator),
        children_(new IteratorWrapper[n]),
        n_(n),
        current_(nullptr),
        direction_(kForward) {
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
    }
  }

  ~MergingIterator() override { delete[] children_; }

  bool Valid() const override { return (current_ != nullptr); }

  void SeekToFirst() override {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToFirst();
    }
    FindSmallest();
    direction_ = kForward;
  }

  void SeekToLast() override {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToLast();
    }
    FindLargest();
    direction_ = kReverse;
  }

  void Seek(const Slice& target) override {
    // 每个迭代器都移动到最接近target的位置(刚好大于等于target)
    // 然后从中找到一个最小的即为最接近target位置
    for (int i = 0; i < n_; i++) {
      children_[i].Seek(target);
    }
    FindSmallest();
    direction_ = kForward;
  }

  // 对于存在同一个user_key的情况，优先返回seq更大的!!!详见FindSmallest的实现。
  void Next() override {
    assert(Valid());

    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all of the non-current_ children since current_ is
    // the smallest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.

    // 查找key后的节点转化为: 
    // 1.所有迭代器定位到刚好大于key的位置 2.并从中选取最小值
    // 当 direction_ == kReverse 时，迭代器定位到刚好小于key的位置，所以需要特殊处理，将迭代器位置移到刚好大于key的位置
    // 当 direction_ == kForward 时，迭代器的位置都是大于等于key的，所以无需特殊处理，
    // direction_只是为了效率优化，其实可以不考虑direction_，每次执行下述逻辑。
    if (direction_ != kForward) {
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key());
          // TODO : 为何跳过等于key的
          if (child->Valid() &&
              comparator_->Compare(key(), child->key()) == 0) {
            child->Next();
          }
        }
      }
      direction_ = kForward;
    }

    // 当前值已经使用过，执行Next，将当前位置排除
    current_->Next();
    // 将当前值排除的情况下，再找出一个最小的值，即为要找的Next
    FindSmallest();
  }

  void Prev() override {
    assert(Valid());

    // Ensure that all children are positioned before key().
    // If we are moving in the reverse direction, it is already
    // true for all of the non-current_ children since current_ is
    // the largest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.

    // 查找key前的节点转化为: 
    // 1.所有迭代器定位到刚好小于key的位置 2.并从中选取最大值
    // 当 direction_ == kReverse 时，天然满足上述条件(所有迭代器定位到刚好小于key)，所以不需要进一步处理
    // 当 direction_ == kForward 时，迭代器的位置都是大于等于key的，所以需要特殊处理，将迭代器位置退到刚好小于key的位置
    // direction_只是为了效率优化，其实可以不考虑direction_，每次执行下述逻辑。
    if (direction_ != kReverse) {
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key());
          if (child->Valid()) {
            // Child is at first entry >= key().  Step back one to be < key()
            // 使得所有的child中的位置都退到刚好小于key的位置
            child->Prev();
          } else {
            // Child has no entries >= key().  Position at last entry.
            child->SeekToLast();
          }
        }
      }
      direction_ = kReverse;
    }

    current_->Prev();
    // 在都小于key的值里面找到一个最大的key，即刚好得到current->key前紧挨着的key
    FindLargest();
  }

  Slice key() const override {
    assert(Valid());
    return current_->key();
  }

  Slice value() const override {
    assert(Valid());
    return current_->value();
  }

  Status status() const override {
    Status status;
    for (int i = 0; i < n_; i++) {
      status = children_[i].status();
      if (!status.ok()) {
        break;
      }
    }
    return status;
  }

 private:
  // Which direction is the iterator moving?
  enum Direction { kForward, kReverse };

  void FindSmallest();
  void FindLargest();

  // We might want to use a heap in case there are lots of children.
  // For now we use a simple array since we expect a very small number
  // of children in leveldb.
  const Comparator* comparator_;
  // 指向长度为n的数组，数组类型为IteratorWrapper
  IteratorWrapper* children_;
  int n_;
  IteratorWrapper* current_;
  // 遍历的方向
  Direction direction_;
};

// 在各个迭代器(level0:每个文件一个迭代器，level1~7:每层一个迭代器)当前位置中找到最小的key，作为MergingIterator的当前位置
// 每个迭代器内部都是升序的，因此直接比较每个迭代器的当前值，即可得到后续待遍历数据中的最小值
// 当多个迭代器对应的user_key是相同的，优先返回seq更大的
void MergingIterator::FindSmallest() {
  IteratorWrapper* smallest = nullptr;
  // 从低层级到高层级，当多个层级的当前key相等时，优先取低层级的
  // 此处的key为internal_key, 不可能完全相等!!!
  for (int i = 0; i < n_; i++) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (smallest == nullptr) {
        smallest = child;
      } else if (comparator_->Compare(child->key(), smallest->key()) < 0) {
        // 此处的key为internal_key，比较时会把seq考虑进去
        // 上述执行的Compare的实现为InternalKeyComparator::Compare
        // 1. 先比较user_key
        // 2. user_key相等之后比较 (seq << 8 | type) , 且seq的值越大对应的internal_key值越小
        smallest = child;
      }
    }
  }
  current_ = smallest;
}

void MergingIterator::FindLargest() {
  IteratorWrapper* largest = nullptr;
  // TODO : 等价于 for (int i = 0; i < n_; i++) ???
  for (int i = n_ - 1; i >= 0; i--) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (largest == nullptr) {
        largest = child;
      } else if (comparator_->Compare(child->key(), largest->key()) > 0) {
        largest = child;
      }
    }
  }
  current_ = largest;
}
}  // namespace

// 将多个迭代器拼成一个迭代器
Iterator* NewMergingIterator(const Comparator* comparator, Iterator** children,
                             int n) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyIterator();
  } else if (n == 1) {
    return children[0];
  } else {
    return new MergingIterator(comparator, children, n);
  }
}

}  // namespace leveldb
