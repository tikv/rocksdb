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
  const MemTableRep::KeyComparator& cmp_;
  const SliceTransform* transform_;
  const size_t lookahead_;

  friend class LookaheadIterator;
public:
 explicit AdaptiveRadixTreeRep(const MemTableRep::KeyComparator& compare,
                      Allocator* allocator, const SliceTransform* transform,
                      const size_t lookahead)
     : MemTableRep(allocator),
       skip_list_(allocator),
       cmp_(compare),
       transform_(transform),
       lookahead_(lookahead) {}

 KeyHandle Allocate(const size_t len, char** buf) override {
   // *buf = skip_list_.AllocateKey(len);
   return static_cast<KeyHandle>(nullptr);
 }

  // Insert key into the list.
  // REQUIRES: nothing that compares equal to key is currently in the list.
 void Insert(KeyHandle handle) override {
   skip_list_.Insert(static_cast<char*>(handle), 0, nullptr);
 }

 bool InsertKey(KeyHandle handle) override {
   return skip_list_.Insert(static_cast<char*>(handle), 0, nullptr);
 }

  // Returns true iff an entry that compares equal to key is in the list.
 bool Contains(const char* key) const override {
   return skip_list_.Get(key) != nullptr;
 }

 size_t ApproximateMemoryUsage() override {
   // All memory is allocated through allocator_; nothing to report here
   return 0;
 }

 void Get(const LookupKey& k, void* callback_args,
          bool (*callback_func)(void* arg, const char* entry)) override {
   callback_func(callback_args, skip_list_.Get(k.user_key().data()));
 }

  uint64_t ApproximateNumEntries(const Slice& start_ikey,
                                 const Slice& end_ikey) override {
//    std::string tmp;
//    uint64_t start_count =
//        skip_list_.EstimateCount(EncodeKey(&tmp, start_ikey));
//    uint64_t end_count = skip_list_.EstimateCount(EncodeKey(&tmp, end_ikey));
//    return (end_count >= start_count) ? (end_count - start_count) : 0;
    return 0;
  }

  ~AdaptiveRadixTreeRep() override {}

//  // Iteration over the contents of a skip list
//  class Iterator : public MemTableRep::Iterator {
//   public:
//    // Initialize an iterator over the specified list.
//    // The returned iterator is not valid.
//    explicit Iterator(
//        const AdaptiveRadixTreeRep* list)
//        : iter_(list) {}
//
//    ~Iterator() override {}
//
//    // Returns true iff the iterator is positioned at a valid node.
//    bool Valid() const override { return iter_.Valid(); }
//
//    // Returns the key at the current position.
//    // REQUIRES: Valid()
//    const char* key() const override { return iter_.key(); }
//
//    // Advances to the next position.
//    // REQUIRES: Valid()
//    void Next() override { iter_.Next(); }
//
//    // Advances to the previous position.
//    // REQUIRES: Valid()
//    void Prev() override { iter_.Prev(); }
//
//    // Advance to the first entry with a key >= target
//    void Seek(const Slice& user_key, const char* memtable_key) override {
//      if (memtable_key != nullptr) {
//        iter_.Seek(memtable_key);
//      } else {
//        iter_.Seek(EncodeKey(&tmp_, user_key));
//      }
//    }
//
//    // Retreat to the last entry with a key <= target
//    void SeekForPrev(const Slice& user_key, const char* memtable_key) override {
//      if (memtable_key != nullptr) {
//        iter_.SeekForPrev(memtable_key);
//      } else {
//        iter_.SeekForPrev(EncodeKey(&tmp_, user_key));
//      }
//    }
//
//    // Position at the first entry in list.
//    // Final state of iterator is Valid() iff list is not empty.
//    void SeekToFirst() override { iter_.SeekToFirst(); }
//
//    // Position at the last entry in list.
//    // Final state of iterator is Valid() iff list is not empty.
//    void SeekToLast() override { iter_.SeekToLast(); }
//
//   protected:
//    std::string tmp_;       // For passing to EncodeKey
//  };


  MemTableRep::Iterator* GetIterator(Arena* arena = nullptr) override {
//      void *mem =
//        arena ? arena->AllocateAligned(sizeof(AdaptiveRadixTreeRep::Iterator))
//              : operator new(sizeof(AdaptiveRadixTreeRep::Iterator));
//      return new (mem) AdaptiveRadixTreeRep::Iterator(&skip_list_);
    return nullptr;
  }
};
}


} // namespace rocksdb
