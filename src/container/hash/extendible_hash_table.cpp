//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  this->dir_.emplace_back(std::make_shared<Bucket>(bucket_size, 0));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(this->latch_);
  size_t index = this->IndexOf(key);
  return this->dir_[index]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(this->latch_);
  size_t index = this->IndexOf(key);
  return this->dir_[index]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(this->latch_);
  size_t index = this->IndexOf(key);

  // key 已存在，覆盖 value 即可
  if (this->dir_[index]->Insert(key, value)) {
    return;
  }

  // 进行桶分裂
  while (this->dir_[index]->IsFull()) {
    auto old_b = this->dir_[index];
    int local_depth = old_b->GetDepth();
    // 需要扩容，否则只需要新增一个 bucket
    if (local_depth == this->GetGlobalDepthInternal()) {
      // dir_num *= 2; global_depth ++
      size_t old_dir_num = this->dir_.size();
      this->dir_.reserve(old_dir_num * 2);
      std::copy_n(this->dir_.begin(), old_dir_num, std::back_inserter(this->dir_));
      this->global_depth_++;

      // 新 dir 指向低 local_depth 位 dir 指向的 bucket
      for (size_t i = old_dir_num; i < old_dir_num * 2; i++) {
        this->dir_[i] = this->dir_[i - old_dir_num];
      }
    }

    // 新增一个 bucket，并重新分配 old_bucket 中的 item
    auto new_b = std::make_shared<Bucket>(bucket_size_, local_depth + 1);
    old_b->IncrementDepth();
    
    int new_high_bit_mask = 1 << local_depth;
    for (const auto &[k, v] : old_b->GetItems()) {
      // 只需要对比 local_depth 增长的那一位即可
      // 将增长的那一位为 1 的 item 转移到新 bucket 中
      if (static_cast<bool>(IndexOf(k) & new_high_bit_mask)) {
        new_b->Insert(k, v);
        // 不能在遍历过程中 old_b->Remove()
      }
    }
    // 在旧 bucket 中移除转移的 item
    for (const auto &[k, v] : new_b->GetItems()) {
      old_b->Remove(k);
    }

    // 将原先指向 old_bucket 的 dir 根据新的 local_depth 重新分配指向
    // 如果 dir.index 在增长的那一位为 1，则指向新的 bucket
    for (size_t i = index; i < this->dir_.size(); i += new_high_bit_mask) {
      if (static_cast<bool>(i & new_high_bit_mask)) {
        this->dir_[i] = new_b;
      }
    }
    index = this->IndexOf(key);
  }

  this->dir_[index]->Insert(key, value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  for (const auto &[k, v] : this->list_) {
    if (k == key) {
      value = v;
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  for (auto it = this->list_.begin(); it != this->list_.end(); it++) {
    if (it->first == key) {
      this->list_.erase(it);
      return true;
    }
  }
  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  for (auto &[k, v] : this->list_) {
    if (k == key) {
      v = value;
      return true;
    }
  }
  if (!this->IsFull()) {
    this->list_.emplace_back(key, value);
    return true;
  }
  return false;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
