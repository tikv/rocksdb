//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "memtable/art.h"

#include <set>

#include "memory/arena.h"
#include "rocksdb/env.h"
#include "test_util/testharness.h"
#include "util/hash.h"
#include "util/random.h"

namespace rocksdb {

typedef uint64_t Key;

static const char* Encode(const uint64_t* key) {
  return reinterpret_cast<const char*>(key);
}

static Key Decode(const char* key) {
  Key rv;
  memcpy(&rv, key, sizeof(Key));
  return rv;
}

class ArtTest : public testing::Test {
 public:
  void Insert(AdaptiveRadixTree* list, Key key) {
    char* buf = list->AllocateKey(sizeof(Key));
    memcpy(buf, &key, sizeof(Key));
    list->Insert(buf, 8, buf);
    keys_.insert(key);
  }

  void Validate(AdaptiveRadixTree* list) {
    // Check keys exist.
    for (Key key : keys_) {
      ASSERT_TRUE(list->Get(Encode(&key)) != nullptr);
    }
    // Iterate over the list, make sure keys appears in order and no extra
    // keys exist.
    AdaptiveRadixTree::Iterator iter(list);
    ASSERT_FALSE(iter.Valid());
    Key zero = 0;
    iter.Seek(Encode(&zero), 8);
    for (Key key : keys_) {
      ASSERT_TRUE(iter.Valid());
      ASSERT_EQ(key, Decode(iter.Value()));
      iter.Next();
    }
    ASSERT_FALSE(iter.Valid());
  }

 private:
  std::set<Key> keys_;
};

TEST_F(ArtTest, Empty) {
  Arena arena;
  AdaptiveRadixTree list(&arena);

  AdaptiveRadixTree::Iterator iter(&list);
  ASSERT_TRUE(!iter.Valid());
  iter.SeekToFirst();
  ASSERT_TRUE(!iter.Valid());
  iter.Seek("ancd", 4);
  ASSERT_TRUE(!iter.Valid());
  // iter.SeekForPrev(100);
  // ASSERT_TRUE(!iter.Valid());
  // iter.SeekToLast();
  // ASSERT_TRUE(!iter.Valid());
}

TEST_F(ArtTest, InsertAndLookup) {
  const int N = 2000;
  const int R = 5000;
  Random rnd(1000);
  std::set<Key> keys;
  Arena arena;
  AdaptiveRadixTree list(&arena);
  const char* v = "abc";
  for (int i = 0; i < N; i++) {
    Key key = rnd.Next() % R;
    if (keys.insert(key).second) {
      char* buf = arena.AllocateAligned(sizeof(Key));
      memcpy(buf, &key, sizeof(Key));
      list.Insert(buf, sizeof(key), v);
    }
  }

  for (Key i = 0; i < R; i++) {
    if (list.Get(Encode(&i))) {
      ASSERT_EQ(keys.count(i), 1U);
    } else {
      ASSERT_EQ(keys.count(i), 0U);
    }
  }

  // Simple iterator tests
  {
    AdaptiveRadixTree::Iterator iter(&list);
    ASSERT_TRUE(!iter.Valid());

    uint64_t zero = 0;
    iter.Seek(Encode(&zero), 8);
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(*(keys.begin()), Decode(iter.Value()));

    uint64_t max_key = R - 1;
    iter.SeekForPrev(Encode(&max_key), 8);
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(*(keys.rbegin()), Decode(iter.Value()));

    iter.SeekToFirst();
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(*(keys.begin()), Decode(iter.Value()));

    iter.SeekToLast();
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(*(keys.rbegin()), Decode(iter.Value()));
  }

  // Forward iteration test
  for (Key i = 0; i < R; i++) {
    AdaptiveRadixTree::Iterator iter(&list);
    iter.Seek(Encode(&i), 8);

    // Compare against model iterator
    std::set<Key>::iterator model_iter = keys.lower_bound(i);
    for (int j = 0; j < 3; j++) {
      if (model_iter == keys.end()) {
        ASSERT_TRUE(!iter.Valid());
        break;
      } else {
        ASSERT_TRUE(iter.Valid());
        ASSERT_EQ(*model_iter, Decode(iter.Value()));
        ++model_iter;
        iter.Next();
      }
    }
  }

  // Backward iteration test
  for (Key i = 0; i < R; i++) {
    AdaptiveRadixTree::Iterator iter(&list);
    iter.SeekForPrev(Encode(&i), 8);

    // Compare against model iterator
    std::set<Key>::iterator model_iter = keys.upper_bound(i);
    for (int j = 0; j < 3; j++) {
      if (model_iter == keys.begin()) {
        ASSERT_TRUE(!iter.Valid());
        break;
      } else {
        ASSERT_TRUE(iter.Valid());
        ASSERT_EQ(*--model_iter, Decode(iter.Value()));
        iter.Prev();
      }
    }
  }
}

#ifndef ROCKSDB_VALGRIND_RUN
// We want to make sure that with a single writer and multiple
// concurrent readers (with no synchronization other than when a
// reader's iterator is created), the reader always observes all the
// data that was present in the skip list when the iterator was
// constructor.  Because insertions are happening concurrently, we may
// also observe new values that were inserted since the iterator was
// constructed, but we should never miss any values that were present
// at iterator construction time.
//
// We generate multi-part keys:
//     <key,gen,hash>
// where:
//     key is in range [0..K-1]
//     gen is a generation number for key
//     hash is hash(key,gen)
//
// The insertion code picks a random key, sets gen to be 1 + the last
// generation number inserted for that key, and sets hash to Hash(key,gen).
//
// At the beginning of a read, we snapshot the last inserted
// generation number for each key.  We then iterate, including random
// calls to Next() and Seek().  For every key we encounter, we
// check that it is either expected given the initial snapshot or has
// been concurrently added since the iterator started.
class ConcurrentTest {
 public:
  static const uint32_t K = 8;

 private:
  static uint64_t key(Key key) { return (key >> 40); }
  static uint64_t gen(Key key) { return (key >> 8) & 0xffffffffu; }
  static uint64_t hash(Key key) { return key & 0xff; }

  static uint64_t HashNumbers(uint64_t k, uint64_t g) {
    uint64_t data[2] = {k, g};
    return Hash(reinterpret_cast<char*>(data), sizeof(data), 0);
  }

  static Key MakeKey(uint64_t k, uint64_t g) {
    assert(sizeof(Key) == sizeof(uint64_t));
    assert(k <= K);  // We sometimes pass K to seek to the end of the skiplist
    assert(g <= 0xffffffffu);
    return ((k << 40) | (g << 8) | (HashNumbers(k, g) & 0xff));
  }

  static bool IsValidKey(Key k) {
    return hash(k) == (HashNumbers(key(k), gen(k)) & 0xff);
  }

  static Key RandomTarget(Random* rnd) {
    switch (rnd->Next() % 10) {
      case 0:
        // Seek to beginning
        return MakeKey(0, 0);
      case 1:
        // Seek to end
        return MakeKey(K, 0);
      default:
        // Seek to middle
        return MakeKey(rnd->Next() % K, 0);
    }
  }

  // Per-key generation
  struct State {
    std::atomic<int> generation[K];
    void Set(int k, int v) {
      generation[k].store(v, std::memory_order_release);
    }
    int Get(int k) { return generation[k].load(std::memory_order_acquire); }

    State() {
      for (unsigned int k = 0; k < K; k++) {
        Set(k, 0);
      }
    }
  };

  // Current state of the test
  State current_;

  Arena arena_;

  // InlineSkipList is not protected by mu_.  We just use a single writer
  // thread to modify it.
  AdaptiveRadixTree list_;

 public:
  ConcurrentTest() : list_(&arena_) {}

  // REQUIRES: No concurrent calls to WriteStep or ConcurrentWriteStep
  void WriteStep(Random* rnd) {
    const uint32_t k = rnd->Next() % K;
    const int g = current_.Get(k) + 1;
    const Key new_key = MakeKey(k, g);
    char* buf = list_.AllocateKey(sizeof(Key));
    memcpy(buf, &new_key, sizeof(Key));
    list_.Insert(buf, 8, buf);
    current_.Set(k, g);
  }

  void ReadStep(Random* rnd) {
    // Remember the initial committed state of the skiplist.
    State initial_state;
    for (unsigned int k = 0; k < K; k++) {
      initial_state.Set(k, current_.Get(k));
    }

    Key pos = RandomTarget(rnd);
    typename AdaptiveRadixTree::Iterator iter(&list_);
    iter.Seek(Encode(&pos), 8);
    while (true) {
      Key current;
      if (!iter.Valid()) {
        current = MakeKey(K, 0);
      } else {
        current = Decode(iter.Value());
        ASSERT_TRUE(IsValidKey(current)) << current;
      }
      ASSERT_LE(pos, current) << "should not go backwards";

      // Verify that everything in [pos,current) was not present in
      // initial_state.
      while (pos < current) {
        ASSERT_LT(key(pos), K) << pos;

        // Note that generation 0 is never inserted, so it is ok if
        // <*,0,*> is missing.
        ASSERT_TRUE((gen(pos) == 0U) ||
                    (gen(pos) > static_cast<uint64_t>(initial_state.Get(
                                    static_cast<int>(key(pos))))))
            << "key: " << key(pos) << "; gen: " << gen(pos)
            << "; initgen: " << initial_state.Get(static_cast<int>(key(pos)));

        // Advance to next key in the valid key space
        if (key(pos) < key(current)) {
          pos = MakeKey(key(pos) + 1, 0);
        } else {
          pos = MakeKey(key(pos), gen(pos) + 1);
        }
      }

      if (!iter.Valid()) {
        break;
      }

      if (rnd->Next() % 2) {
        iter.Next();
        pos = MakeKey(key(pos), gen(pos) + 1);
      } else {
        Key new_target = RandomTarget(rnd);
        if (new_target > pos) {
          pos = new_target;
          iter.Seek(Encode(&new_target), 8);
        }
      }
    }
  }
};

const uint32_t ConcurrentTest::K;

// Simple test that does single-threaded testing of the ConcurrentTest
// scaffolding.
TEST_F(ArtTest, ConcurrentReadWithoutThreads) {
  {
    ConcurrentTest test;
    Random rnd(test::RandomSeed());
    for (int i = 0; i < 10000; i++) {
      test.ReadStep(&rnd);
      test.WriteStep(&rnd);
    }
  }
  {
    ConcurrentTest test;
    Random rnd(test::RandomSeed());
    for (int i = 0; i < 10000; i++) {
      test.ReadStep(&rnd);
      test.WriteStep(&rnd);
    }
  }
}

class TestState {
 public:
  TestState(int s) : seed_(s), quit_flag_(false) {}

  enum ReaderState { STARTING, RUNNING, DONE };
  virtual ~TestState() {}
  virtual void Wait(ReaderState s) = 0;
  virtual void Change(ReaderState s) = 0;
  virtual void AdjustPendingWriters(int delta) = 0;
  virtual void WaitForPendingWriters() = 0;
  // REQUIRES: No concurrent calls for the same k
  virtual void ReadStep(Random* rnd) = 0;

 public:
  int seed_;
  std::atomic<bool> quit_flag_;
  std::atomic<uint32_t> next_writer_;
};

class TestStateImpl : public TestState {
 public:
  ConcurrentTest t_;

  explicit TestStateImpl(int s)
      : TestState(s), state_(STARTING), pending_writers_(0), state_cv_(&mu_) {}

  void Wait(ReaderState s) override {
    mu_.Lock();
    while (state_ != s) {
      state_cv_.Wait();
    }
    mu_.Unlock();
  }

  void Change(ReaderState s) override {
    mu_.Lock();
    state_ = s;
    state_cv_.Signal();
    mu_.Unlock();
  }

  void AdjustPendingWriters(int delta) override {
    mu_.Lock();
    pending_writers_ += delta;
    if (pending_writers_ == 0) {
      state_cv_.Signal();
    }
    mu_.Unlock();
  }

  void WaitForPendingWriters() override {
    mu_.Lock();
    while (pending_writers_ != 0) {
      state_cv_.Wait();
    }
    mu_.Unlock();
  }

  void ReadStep(Random* rnd) override { t_.ReadStep(rnd); }

 private:
  port::Mutex mu_;
  ReaderState state_;
  int pending_writers_;
  port::CondVar state_cv_;
};

static void ConcurrentReader(void* arg) {
  TestState* state = reinterpret_cast<TestState*>(arg);
  Random rnd(state->seed_);
  int64_t reads = 0;
  state->Change(TestState::RUNNING);
  while (!state->quit_flag_.load(std::memory_order_acquire)) {
    state->ReadStep(&rnd);
    ++reads;
  }
  state->Change(TestState::DONE);
}

static void RunConcurrentRead(int run) {
  const int seed = test::RandomSeed() + (run * 100);
  Random rnd(seed);
  const int N = 1000;
  const int kSize = 1000;
  for (int i = 0; i < N; i++) {
    if ((i % 100) == 0) {
      fprintf(stderr, "Run %d of %d\n", i, N);
    }
    TestStateImpl state(seed + 1);
    Env::Default()->SetBackgroundThreads(1);
    Env::Default()->Schedule(ConcurrentReader, &state);
    state.Wait(TestState::RUNNING);
    for (int k = 0; k < kSize; ++k) {
      state.t_.WriteStep(&rnd);
    }
    state.quit_flag_.store(true, std::memory_order_release);
    state.Wait(TestState::DONE);
  }
}

TEST_F(ArtTest, ConcurrentRead1) {
  RunConcurrentRead(1);
  RunConcurrentRead(1);
}
TEST_F(ArtTest, ConcurrentRead2) {
  RunConcurrentRead(2);
  RunConcurrentRead(2);
}
TEST_F(ArtTest, ConcurrentRead3) {
  RunConcurrentRead(3);
  RunConcurrentRead(3);
}
TEST_F(ArtTest, ConcurrentRead4) {
  RunConcurrentRead(4);
  RunConcurrentRead(4);
}
TEST_F(ArtTest, ConcurrentRead5) {
  RunConcurrentRead(5);
  RunConcurrentRead(5);
}

#endif  // ROCKSDB_VALGRIND_RUN
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
