//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include "db/db_test_util.h"
#include "db/write_batch_internal.h"
#include "db/write_thread.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "test_util/fault_injection_test_env.h"
#include "test_util/sync_point.h"
#include "util/string_util.h"

namespace rocksdb {

// Test variations of WriteImpl.
class DBMultiBatchWriteTest : public DBTestBase {
 public:
  DBMultiBatchWriteTest() : DBTestBase("/db_multi_batch_write_test") {}

  void WaitTimeout(std::function<bool()> func, uint32_t timeout) {
    auto start_time = std::chrono::steady_clock::now();
    while (!func()) {
      auto now = std::chrono::steady_clock::now();
      ASSERT_LT(now - start_time, std::chrono::microseconds(timeout));
    }
  }

  // Write one version of "value" to DB.
  static void WriteOneBatch(DB* db, uint32_t index, const uint32_t version) {
    WriteOptions opt;
    std::vector<WriteBatch> data(kNumBatch);
    std::vector<WriteBatch*> batches;
    std::string value = "value" + ToString(version);
    for (uint32_t j = 0; j < kNumBatch; j++) {
      WriteBatch* batch = &data[j];
      std::string key_prefix =
          "key_" + ToString(index) + "_" + ToString(j) + "_";
      for (uint32_t k = 0; k < kBatchSize; k++) {
        batch->Put(key_prefix + ToString(k), value);
      }
      batches.push_back(batch);
    }
    *GetThreadLocalVersion() = version;
    db->MultiBatchWrite(opt, std::move(batches));
  }

  // Check whether the version of value in DB is in line with expectations. -1 represent that the keys do not existed.
  void CheckValue(const Snapshot* snap, uint32_t index, const int version) {
    ReadOptions opt;
    if (snap) {
      opt.snapshot = snap;
    }
    std::string expected_value;
    if (version >= 0) {
      expected_value = "value" + ToString(version);
    }
    for (uint32_t j = 0; j < kNumBatch; j++) {
      std::string key_prefix =
          "key_" + ToString(index) + "_" + ToString(j) + "_";
      for (uint32_t k = 0; k < kBatchSize; k++) {
        std::string value;
        auto s = dbfull()->Get(opt, key_prefix + ToString(k), &value);
        if (!s.ok()) {
          ASSERT_EQ(Status::NotFound(), s);
          ASSERT_EQ(-1, version);
        } else {
          ASSERT_EQ(expected_value, value);
        }
      }
    }
  }

  static uint32_t* GetThreadLocalVersion() {
    thread_local uint32_t version = 0;
    return &version;
  }

 protected:
  static const uint32_t kNumThreads = 4;
  static const uint32_t kBatchSize = 16;
  static const uint32_t kNumBatch = 4;
  static const uint32_t kNumWrite = 4;
};

TEST_F(DBMultiBatchWriteTest, BasicWrite) {
  Options options;
  options.enable_multi_thread_write = true;
  options.write_buffer_size = 1024 * 128;
  Reopen(options);
  std::vector<port::Thread> threads;
  auto db = dbfull();
  for (uint32_t t = 0; t < kNumThreads; t++) {
    threads.push_back(port::Thread(
        [&](uint32_t index) {
          WriteOptions opt;
          std::vector<WriteBatch> data(kNumBatch);
          for (uint32_t j = 0; j < kNumWrite; j++) {
            DBMultiBatchWriteTest::WriteOneBatch(db, index * kNumWrite + j, 0);
          }
        },
        t));
  }
  for (uint32_t i = 0; i < kNumThreads; i++) {
    threads[i].join();
  }
  ReadOptions opt;
  for (uint32_t t = 0; t < kNumThreads; t++) {
    std::string value;
    for (uint32_t i = 0; i < kNumWrite; i++) {
      CheckValue(nullptr, t * kNumWrite + i, 0);
    }
  }
}

TEST_F(DBMultiBatchWriteTest, MultiBatchWriteDoneByOtherThread) {
  Options options;
  options.write_buffer_size = 1024 * 128;
  options.enable_multi_thread_write = true;
  Reopen(options);
  std::vector<port::Thread> threads;
  auto db = dbfull();

  std::atomic<bool> follower_start(false);
  std::atomic<bool> leader_continue(false);
  std::atomic<uint32_t> leader_start(0);
  std::atomic<uint32_t> follower_wait(0);
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::MultiBatchWriteImpl:Wait1", [&](void* arg) {
        auto* w = reinterpret_cast<WriteThread::Writer*>(arg);
        if (w->state == WriteThread::STATE_GROUP_LEADER) {
          leader_start = w->write_group->running;
        }
        while (!leader_continue.load()) {
          leader_start = w->write_group->running;
        }
      });

  SyncPoint::GetInstance()->EnableProcessing();

  threads.emplace_back(DBMultiBatchWriteTest::WriteOneBatch, db, 0, 0);
  WaitTimeout([&]() -> bool { return leader_start.load() > 0; }, 1000 * 1000);

  // leader-thread will be blocking at `DBImpl::MultiBatchWriteImpl:Wait1`.
  // Because it finished the last sub-task by self, there are (kNumBatch - 1)
  // task left.
  assert((kNumBatch - 1) == leader_start.load());

  SyncPoint::GetInstance()->SetCallBack(
      "WriteThread::AwaitState:BlockingWaitingMultiThread", [&](void* arg) {
        auto* w = reinterpret_cast<WriteThread::Writer*>(arg);
        if (w->state != WriteThread::STATE_GROUP_LEADER) {
          return;
        }
        follower_wait = 1;
        while (!follower_start.load()) {
        }
      });
  threads.emplace_back(DBMultiBatchWriteTest::WriteOneBatch, db, 0, 1);

  // Wait follower-thread meet SyncPoint, which means that it has finished all
  // tasks in queue.
  WaitTimeout([&]() -> bool { return follower_wait.load() > 0; }, 1000 * 1000);

  // follower-thread will keep doing sub-task from queue until there is no task
  // left. So it will finish all sub-task of produced by leader-thread.
  ASSERT_EQ(0, leader_start.load());

  // leader-thread has not refresh last-sequence, so we can not read keys
  // inserted by leader-thread, although they has existed in memtable.
  CheckValue(nullptr, 0, -1);

  // Let leader-thread refresh last-sequence and wait it till done.
  leader_continue = true;
  threads[0].join();
  CheckValue(nullptr, 0, 0);

  // Let follower-thread start inserting into memtable and wait it till done.
  follower_start = true;
  threads[1].join();
  CheckValue(nullptr, 0, 1);
}

TEST_F(DBMultiBatchWriteTest, MultiBatchWriteParallelDoneByOtherThread) {
  Options options;
  options.write_buffer_size = 1024 * 128;
  options.enable_multi_thread_write = true;
  Reopen(options);
  std::vector<port::Thread> threads;
  auto db = dbfull();

  std::atomic<bool> block_wait(true);
  std::atomic<bool> leader_start(false);
  std::atomic<bool> leader_waiting(false);
  std::atomic<uint32_t> leader_exit_count(0);
  std::atomic<uint32_t> follower_wait(0);
  std::vector<const Snapshot*> snapshots(2, nullptr);
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::MultiBatchWriteImpl:BeforeLeaderEnters", [&](void* arg) {
        // Wait all follower-threads starting so that this WriteGroup could pick
        // up all writers of follower-threads.
        while (follower_wait.load() < DBMultiBatchWriteTest::kNumThreads) {
        }
        leader_start = true;
      });

  // Notify leader-thread
  SyncPoint::GetInstance()->SetCallBack(
      "WriteThread::JoinBatchGroup:Wait",
      [&](void* arg) { follower_wait.fetch_add(1); });

  // Wait other-thread to finish our writes.
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::MultiBatchWriteImpl:Wait2", [&](void* arg) {
        bool is_leader_thread = *reinterpret_cast<bool*>(arg);
        if (is_leader_thread) {
          leader_waiting = true;
        }
        while (block_wait.load()) {
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  for (uint32_t i = 0; i < kNumThreads; i++) {
    threads.emplace_back(DBMultiBatchWriteTest::WriteOneBatch, db, i, 0);
  }
  // Wait leader-thread to finish calling `LaunchParallelMemTableWriters`.
  WaitTimeout([&]() -> bool { return leader_waiting.load(); }, 1000 * 1000);

  // `BlockingWaitingMultiThread` means that this thread has finished all writes
  // left in queue.
  SyncPoint::GetInstance()->SetCallBack(
      "WriteThread::AwaitState:BlockingWaitingMultiThread",
      [&](void* arg) { block_wait = false; });
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::MultiBatchWriteImpl:BeforeSetLastSequence", [&](void* arg) {
        // Because we have not updated last-sequence, we can not get new value
        // from this snapshot. even if they have existed in memtable
        snapshots[0] = db->GetSnapshot();
      });

  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::MultiBatchWriteImpl:BeforeLeaderExit", [&](void* arg) {
        // Make sure that there is only one thread could enter here.
        auto count = leader_exit_count.fetch_add(1);
        ASSERT_EQ(0, count);
        auto* w = reinterpret_cast<WriteThread::Writer*>(arg);
        ASSERT_EQ(1, w->write_group->running);

        // Because we have refresh last-sequence, we can get new value.
        snapshots[1] = db->GetSnapshot();
      });

  // Add an new thread which will help all other threads finish their writes.
  threads.emplace_back(DBMultiBatchWriteTest::WriteOneBatch, db, 0, 1);
  for (auto& t : threads) {
    t.join();
  }
  CheckValue(nullptr, 0, 1);
  ASSERT_TRUE(snapshots[0]);
  ASSERT_TRUE(snapshots[1]);
  CheckValue(snapshots[0], 0, -1);
  CheckValue(snapshots[1], 0, 0);
  for (auto s : snapshots) {
    dbfull()->ReleaseSnapshot(s);
  }
}

TEST_F(DBMultiBatchWriteTest, MultiBatchWriteLeaderCoverByOtherThread) {
  // This test we will make other thread finish their writes before
  // leader-thread, but their writes will still cover that of leader-thread.
  Options options;
  options.write_buffer_size = 1024 * 128;
  options.enable_multi_thread_write = true;
  Reopen(options);
  std::vector<port::Thread> threads;
  auto db = dbfull();
  std::atomic<uint32_t> follower_wait(0);
  std::atomic<uint32_t> follower_complete(0);
  std::atomic<uint32_t> leader_thread_version(0);
  std::vector<const Snapshot*> snapshots(2, nullptr);

  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::MultiBatchWriteImpl:BeforeLeaderEnters", [&](void* arg) {
        // Wait all follower-threads starting so that this WriteGroup could pick
        // up all writers of follower-threads.
        while (follower_wait.load() < 2) {
        }
      });

  // Notify leader-thread
  SyncPoint::GetInstance()->SetCallBack(
      "WriteThread::JoinBatchGroup:Wait",
      [&](void* arg) { follower_wait.fetch_add(1); });

  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::MultiBatchWriteImpl:BeforeSetLastSequence",
      [&](void* arg) { follower_complete.fetch_add(1); });

  // Wait other-thread to finish our writes.
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::MultiBatchWriteImpl:BeforeInsert", [&](void* arg) {
        auto is_leader_thread = *reinterpret_cast<bool*>(arg);
        if (is_leader_thread) {
          leader_thread_version =
              *DBMultiBatchWriteTest::GetThreadLocalVersion();
          while (follower_complete.load() < 1) {
          }
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  threads.emplace_back(DBMultiBatchWriteTest::WriteOneBatch, db, 0, 0);
  threads.emplace_back(DBMultiBatchWriteTest::WriteOneBatch, db, 0, 1);
  for (auto& t : threads) {
    t.join();
  }
  // Make sure that value written by leader-thread is covered by other thread.
  CheckValue(nullptr, 0, 1 - leader_thread_version.load());
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
