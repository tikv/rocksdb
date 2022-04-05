//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#include "db/memtable.h"
#include "memory/arena.h"
#include "memtable/art.h"
#include "rocksdb/memtablerep.h"

namespace rocksdb {
namespace {

class AdaptiveRadixTreeRep : public MemTableRep {
  AdaptiveRadixTree skip_list_;

public:
 explicit AdaptiveRadixTreeRep(Allocator* allocator)
     : MemTableRep(allocator), skip_list_(allocator) {}

 KeyHandle Allocate(const size_t len, char** buf) override {
   *buf = allocator_->Allocate(len);
   return static_cast<KeyHandle>(*buf);
 }

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
 void Insert(KeyHandle handle) override {
   char* buf = static_cast<char*>(handle);
   uint32_t len = 0;
   // +5: we assume "data" is not corrupted
   // unsigned char is 7 bits, uint32_t is 32 bits, need 5 unsigned char
   auto p = GetVarint32Ptr(buf, buf + 5 /* limit */, &len);
   skip_list_.Insert(p, len - 8, buf);
 }

 bool InsertKey(KeyHandle handle) override {
   Insert(handle);
   return true;
 }

  // Returns true iff an entry that compares equal to key is in the list.
 bool Contains(const char* key) const override {
   return skip_list_.Get(key, static_cast<int>(strlen(key))) != nullptr;
 }

 size_t ApproximateMemoryUsage() override {
   // All memory is allocated through allocator_; nothing to report here
   return 0;
 }

 void Get(const LookupKey& k, void* callback_args,
          bool (*callback_func)(void* arg, const char* entry)) override {
   const char* value = skip_list_.Get(k.user_key().data(),
                                      static_cast<int>(k.user_key().size()));
   if (value != nullptr) {
     callback_func(callback_args, value);
   }
 }

 uint64_t ApproximateNumEntries(const Slice& /* start_ikey */,
                                const Slice& /* end_ikey */) override {
   return 0;
 }

  ~AdaptiveRadixTreeRep() override {}

  // Iteration over the contents of a skip list
  class Iterator : public MemTableRep::Iterator {
   public:
    // Initialize an iterator over the specified list.
    // The returned iterator is not valid.
    explicit Iterator(AdaptiveRadixTree* list) : iter_(list) {}

    ~Iterator() override {}

    // Returns true iff the iterator is positioned at a valid node.
    bool Valid() const override { return iter_.Valid(); }

    // Returns the key at the current position.
    // REQUIRES: Valid()
    const char* key() const override { return iter_.Value(); }

    // Advances to the next position.
    // REQUIRES: Valid()
    void Next() override { iter_.Next(); }

    // Advances to the previous position.
    // REQUIRES: Valid()
    void Prev() override { assert(false); }

    // Advance to the first entry with a key >= target
    void Seek(const Slice& user_key, const char* memtable_key) override {
      if (memtable_key != nullptr) {
        uint32_t l = 0;
        const char* k = GetVarint32Ptr(memtable_key, memtable_key + 5, &l);
        iter_.Seek(k, static_cast<int>(l) - 8);
      } else {
        iter_.Seek(user_key.data(), static_cast<int>(user_key.size()) - 8);
      }
    }

    // Retreat to the last entry with a key <= target
    void SeekForPrev(const Slice& user_key, const char* memtable_key) override {
      if (memtable_key != nullptr) {
        uint32_t l = 0;
        const char* k = GetVarint32Ptr(memtable_key, memtable_key + 5, &l);
        iter_.SeekForPrev(k, l - 8);
      } else {
        iter_.SeekForPrev(user_key.data(),
                          static_cast<int>(user_key.size()) - 8);
      }
    }

    // Position at the first entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToFirst() override { iter_.SeekToFirst(); }

    // Position at the last entry in list.
    // Final state of iterator is Valid() iff list is not empty.
    void SeekToLast() override { iter_.SeekToLast(); }

   protected:
    std::string tmp_;  // For passing to EncodeKey
    AdaptiveRadixTree::Iterator iter_;
  };

  MemTableRep::Iterator* GetIterator(Arena* arena = nullptr) override {
    void* mem =
        arena ? arena->AllocateAligned(sizeof(AdaptiveRadixTreeRep::Iterator)) :
              operator new(sizeof(AdaptiveRadixTreeRep::Iterator));
    return new (mem) AdaptiveRadixTreeRep::Iterator(&skip_list_);
  }
};
}

MemTableRep* AdaptiveRadixTreeFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator& /* compare */, Allocator* allocator,
    const SliceTransform* /* transform */, Logger* /*logger*/) {
  return new AdaptiveRadixTreeRep(allocator);
}

} // namespace rocksdb
