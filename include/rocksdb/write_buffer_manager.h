//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBufferManager is for managing memory allocation for one or more
// MemTables.

#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <list>
#include <mutex>

#include "rocksdb/cache.h"

namespace ROCKSDB_NAMESPACE {
class CacheReservationManager;
class DB;
class ColumnFamilyHandle;

// Interface to block and signal DB instances, intended for RocksDB
// internal use only. Each DB instance contains ptr to StallInterface.
class StallInterface {
 public:
  virtual ~StallInterface() {}

  virtual void Block() = 0;

  virtual void Signal() = 0;
};

class WriteBufferManager final {
 public:
  // Parameters:
  // - flush_size: When the total size of mutable memtables exceeds this limit,
  // the largest one will be frozen and scheduled for flush. Disabled when 0.
  //
  // Immutable memtables are excluded for this reason: RocksDB always schedule
  // a flush for newly created immutable memtable. We can consider them evicted
  // from memory if flush bandwidth is sufficient.
  //
  // It's an undefined behavior to enable/disable flush limit after the manager
  // has been used by a DB instance.
  //
  // - stall_ratio: When the total size of memtables exceeds ratio*flush_size,
  // user writes will be delayed. Disabled when smaller than 1.
  //
  // - flush_oldest_first: By default we freeze the largest mutable memtable
  // when `flush_size` is triggered. By enabling this flag, the oldest mutable
  // memtable will be frozen instead.
  //
  // - cache: if `cache` is provided, memtable memory will be charged as a
  // dummy entry This is useful to keep the memory sum of both memtable and
  // block cache under control.
  explicit WriteBufferManager(size_t flush_size,
                              std::shared_ptr<Cache> cache = {},
                              float stall_ratio = 0.0,
                              bool flush_oldest_first = false);
  // No copying allowed
  WriteBufferManager(const WriteBufferManager&) = delete;
  WriteBufferManager& operator=(const WriteBufferManager&) = delete;

  ~WriteBufferManager();

  // Returns true if a non-zero buffer_limit is passed to limit the total
  // memory usage or cache is provided to charge write buffer memory.
  bool enabled() const { return flush_size() > 0 || cache_res_mgr_ != nullptr; }

  // Returns the total memory used by memtables.
  // Only valid if enabled().
  size_t memory_usage() const {
    return memory_used_.load(std::memory_order_relaxed);
  }

  // Returns the total memory used by active memtables.
  size_t mutable_memtable_memory_usage() const {
    return memory_active_.load(std::memory_order_relaxed);
  }

  size_t dummy_entries_in_cache_usage() const;

  // Returns the flush_size.
  size_t flush_size() const {
    return flush_size_.load(std::memory_order_relaxed);
  }

  size_t stall_size() const {
    return static_cast<size_t>(flush_size() * stall_ratio_);
  }

  void SetFlushSize(size_t new_size);

  void SetFlushOldestFirst(bool v) {
    flush_oldest_first_.store(v, std::memory_order_relaxed);
  }

  // Below functions should be called by RocksDB internally.

  // This handle is the same as the one created by `DB::Open` or
  // `DB::CreateColumnFamily`.
  // Must be called not holding db mutex and not inside write thread.
  // `UnregisterColumnFamily()` must be called by DB before the handle is
  // destroyed.
  void RegisterColumnFamily(DB* db, ColumnFamilyHandle* cf);

  // Called during `DB::Close`.
  // Must be called not holding db mutex and not inside write thread.
  void UnregisterDB(DB* db);

  // Called during `DestroyColumnFamilyHandle`.
  // Must be called not holding db mutex and not inside write thread.
  void UnregisterColumnFamily(ColumnFamilyHandle* cf);

  void ReserveMem(size_t mem);

  // We are in the process of freeing `mem` bytes, so it is not considered
  // when checking the soft limit.
  void ScheduleFreeMem(size_t mem);

  void FreeMem(size_t mem);

  // Whether the DB writer should call `MaybeFlush` before write.
  bool ShouldFlush() {
    size_t local_size = flush_size();
    return local_size > 0 && mutable_memtable_memory_usage() >= local_size;
  }

  // Must be called without holding db mutex. When called in write thread, must
  // pass in the pointer to the db.
  void MaybeFlush(DB* this_db) {
    if (sentinels_mu_.try_lock()) {
      MaybeFlushLocked(this_db);
      sentinels_mu_.unlock();
    }
  }

  void MaybeFlushLocked(DB* this_db = nullptr);

  // Returns true if total memory usage exceeded buffer_size.
  // We stall the writes untill memory_usage drops below buffer_size. When the
  // function returns true, all writer threads (including one checking this
  // condition) across all DBs will be stalled. Stall is allowed only if user
  // pass allow_stall = true during WriteBufferManager instance creation.
  //
  // Should only be called by RocksDB internally .
  bool ShouldStall() const {
    if (!allow_stall_ || flush_size() == 0) {
      return false;
    }
    return is_stall_active() || is_stall_threshold_exceeded();
  }

  // Returns true if stall is active.
  bool is_stall_active() const {
    return stall_active_.load(std::memory_order_relaxed);
  }

  // Returns true if stalling condition is met. Only valid if buffer_size_ is
  // non-zero.
  bool is_stall_threshold_exceeded() const {
    return memory_usage() >= stall_size();
  }

  // Add the DB instance to the queue and block the DB.
  // Should only be called by RocksDB internally.
  void BeginWriteStall(StallInterface* wbm_stall);

  // If stall conditions have resolved, remove DB instances from queue and
  // signal them to continue.
  void MaybeEndWriteStall();

  // Called when DB instance is closed.
  void RemoveFromStallQueue(StallInterface* wbm_stall);

 private:
  struct WriteBufferSentinel {
    DB* db;
    ColumnFamilyHandle* cf;
  };
  // Protected by `sentinels_mu_`.
  std::list<std::shared_ptr<WriteBufferSentinel>> sentinels_;
  std::mutex sentinels_mu_;

  // Shared by flush_size limit and cache charging.
  // When cache charging is enabled, this is updated under cache_res_mgr_mu_.
  std::atomic<size_t> memory_used_;

  std::atomic<size_t> flush_size_;
  // Only used when flush_size is non-zero.
  std::atomic<size_t> memory_active_;
  std::atomic<bool> flush_oldest_first_;

  const bool allow_stall_;
  const float stall_ratio_;
  std::list<StallInterface*> queue_;
  // Protects the queue_ and stall_active_.
  std::mutex stall_mu_;
  // Value should only be changed by BeginWriteStall() and MaybeEndWriteStall()
  // while holding mu_, but it can be read without a lock.
  // It is a cached value of `ShouldStall()`.
  std::atomic<bool> stall_active_;

  std::unique_ptr<CacheReservationManager> cache_res_mgr_;
  // Protects cache_res_mgr_
  std::mutex cache_res_mgr_mu_;

  void ReserveMemWithCache(size_t mem);
  void FreeMemWithCache(size_t mem);
};
}  // namespace ROCKSDB_NAMESPACE
