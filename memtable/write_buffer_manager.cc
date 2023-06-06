//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/write_buffer_manager.h"

#include "cache/cache_entry_roles.h"
#include "cache/cache_reservation_manager.h"
#include "db/db_impl/db_impl.h"
#include "logging/logging.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {
WriteBufferManager::WriteBufferManager(size_t _flush_size,
                                       std::shared_ptr<Cache> cache,
                                       float stall_ratio,
                                       bool flush_oldest_first)
    : memory_used_(0),
      flush_size_(_flush_size),
      memory_active_(0),
      flush_oldest_first_(flush_oldest_first),
      allow_stall_(stall_ratio >= 1.0),
      stall_ratio_(stall_ratio),
      stall_active_(false),
      cache_res_mgr_(nullptr) {
#ifndef ROCKSDB_LITE
  if (cache) {
    // Memtable's memory usage tends to fluctuate frequently
    // therefore we set delayed_decrease = true to save some dummy entry
    // insertion on memory increase right after memory decrease
    cache_res_mgr_.reset(
        new CacheReservationManager(cache, true /* delayed_decrease */));
  }
#else
  (void)cache;
#endif  // ROCKSDB_LITE
}

WriteBufferManager::~WriteBufferManager() {
#ifndef NDEBUG
  std::unique_lock<std::mutex> lock(stall_mu_);
  assert(queue_.empty());
#endif
}

std::size_t WriteBufferManager::dummy_entries_in_cache_usage() const {
  if (cache_res_mgr_ != nullptr) {
    return cache_res_mgr_->GetTotalReservedCacheSize();
  } else {
    return 0;
  }
}

void WriteBufferManager::SetFlushSize(size_t new_size) {
  if (flush_size_.exchange(new_size, std::memory_order_relaxed) > new_size) {
    // Threshold is decreased. We must make sure all outstanding memtables
    // are flushed.
    std::lock_guard<std::mutex> lock(sentinels_mu_);
    auto max_retry = sentinels_.size();
    while ((max_retry--) && ShouldFlush()) {
      MaybeFlushLocked();
    }
  } else {
    // Check if stall is active and can be ended.
    MaybeEndWriteStall();
  }
}

void WriteBufferManager::RegisterColumnFamily(DB* db, ColumnFamilyHandle* cf) {
  assert(db != nullptr);
  auto sentinel = std::make_shared<WriteBufferSentinel>();
  sentinel->db = db;
  sentinel->cf = cf;
  std::lock_guard<std::mutex> lock(sentinels_mu_);
  MaybeFlushLocked();
  sentinels_.push_back(sentinel);
}

void WriteBufferManager::UnregisterDB(DB* db) {
  std::lock_guard<std::mutex> lock(sentinels_mu_);
  sentinels_.remove_if([=](const std::shared_ptr<WriteBufferSentinel>& s) {
    return s->db == db;
  });
  MaybeFlushLocked();
}

void WriteBufferManager::UnregisterColumnFamily(ColumnFamilyHandle* cf) {
  std::lock_guard<std::mutex> lock(sentinels_mu_);
  sentinels_.remove_if([=](const std::shared_ptr<WriteBufferSentinel>& s) {
    return s->cf == cf;
  });
  MaybeFlushLocked();
}

void WriteBufferManager::ReserveMem(size_t mem) {
  size_t local_size = flush_size();
  if (cache_res_mgr_ != nullptr) {
    ReserveMemWithCache(mem);
  } else if (local_size > 0) {
    memory_used_.fetch_add(mem, std::memory_order_relaxed);
  }
  if (local_size > 0) {
    memory_active_.fetch_add(mem, std::memory_order_relaxed);
  }
}

// Should only be called from write thread
void WriteBufferManager::ReserveMemWithCache(size_t mem) {
#ifndef ROCKSDB_LITE
  assert(cache_res_mgr_ != nullptr);
  // Use a mutex to protect various data structures. Can be optimized to a
  // lock-free solution if it ends up with a performance bottleneck.
  std::lock_guard<std::mutex> lock(cache_res_mgr_mu_);

  size_t new_mem_used = memory_used_.load(std::memory_order_relaxed) + mem;
  memory_used_.store(new_mem_used, std::memory_order_relaxed);
  Status s =
      cache_res_mgr_->UpdateCacheReservation<CacheEntryRole::kWriteBuffer>(
          new_mem_used);

  // We absorb the error since WriteBufferManager is not able to handle
  // this failure properly. Ideallly we should prevent this allocation
  // from happening if this cache reservation fails.
  // [TODO] We'll need to improve it in the future and figure out what to do on
  // error
  s.PermitUncheckedError();
#else
  (void)mem;
#endif  // ROCKSDB_LITE
}

void WriteBufferManager::ScheduleFreeMem(size_t mem) {
  if (flush_size() > 0) {
    memory_active_.fetch_sub(mem, std::memory_order_relaxed);
  }
}

void WriteBufferManager::FreeMem(size_t mem) {
  if (cache_res_mgr_ != nullptr) {
    FreeMemWithCache(mem);
  } else if (flush_size() > 0) {
    memory_used_.fetch_sub(mem, std::memory_order_relaxed);
  }
  // Check if stall is active and can be ended.
  MaybeEndWriteStall();
}

void WriteBufferManager::FreeMemWithCache(size_t mem) {
#ifndef ROCKSDB_LITE
  assert(cache_res_mgr_ != nullptr);
  // Use a mutex to protect various data structures. Can be optimized to a
  // lock-free solution if it ends up with a performance bottleneck.
  std::lock_guard<std::mutex> lock(cache_res_mgr_mu_);
  size_t new_mem_used = memory_used_.load(std::memory_order_relaxed) - mem;
  memory_used_.store(new_mem_used, std::memory_order_relaxed);
  Status s =
      cache_res_mgr_->UpdateCacheReservation<CacheEntryRole::kWriteBuffer>(
          new_mem_used);

  // We absorb the error since WriteBufferManager is not able to handle
  // this failure properly.
  // [TODO] We'll need to improve it in the future and figure out what to do on
  // error
  s.PermitUncheckedError();
#else
  (void)mem;
#endif  // ROCKSDB_LITE
}

void WriteBufferManager::MaybeFlushLocked(DB* this_db) {
  if (!ShouldFlush()) {
    return;
  }
  // Have at least one candidate to flush with
  // check_if_compaction_disabled=false when all others failed.
  constexpr size_t kCandidateSize = 2;
  // (score, age).
  using Candidate = std::tuple<WriteBufferSentinel*, uint64_t, uint64_t>;
  auto cmp = [](const Candidate& a, const Candidate& b) {
    return std::get<1>(a) <= std::get<1>(b);
  };
  std::set<Candidate, decltype(cmp)> candidates(cmp);

  for (auto& s : sentinels_) {
    // TODO: move this calculation to a callback.
    uint64_t current_score = 0;
    uint64_t current_memory_bytes = std::numeric_limits<uint64_t>::max();
    uint64_t oldest_time = std::numeric_limits<uint64_t>::max();
    s->db->GetApproximateActiveMemTableStats(s->cf, &current_memory_bytes,
                                             &oldest_time);
    if (flush_oldest_first_.load(std::memory_order_relaxed)) {
      // Convert oldest to highest score.
      current_score = std::numeric_limits<uint64_t>::max() - oldest_time;
    } else {
      current_score = current_memory_bytes;
    }
    // A very mild penalty for too many L0 files.
    uint64_t level0;
    // 3 is to optimize the frequency of getting options, which uses mutex.
    if (s->db->GetIntProperty(DB::Properties::kNumFilesAtLevelPrefix + "0",
                              &level0) &&
        level0 >= 3) {
      auto opts = s->db->GetOptions(s->cf);
      if (opts.level0_file_num_compaction_trigger > 0 &&
          level0 >=
              static_cast<uint64_t>(opts.level0_file_num_compaction_trigger)) {
        auto diff = level0 - static_cast<uint64_t>(
                                 opts.level0_file_num_compaction_trigger);
        // 0->2, +1->4, +2->8, +3->12, +4->18
        uint64_t factor = (diff + 2) * (diff + 2) / 2;
        if (factor > 100) {
          factor = 100;
        }
        current_score = current_score * (100 - factor) / factor;
      }
    }
    candidates.insert({s.get(), current_score, oldest_time});
    if (candidates.size() > kCandidateSize) {
      candidates.erase(candidates.begin());
    }
  }

  // We only flush at most one column family at a time.
  // This is enough to keep size under control except when flush_size is
  // dynamically decreased. That case is managed in `SetFlushSize`.
  auto candidate = candidates.rbegin();
  while (candidate != candidates.rend()) {
    auto sentinel = std::get<0>(*candidate);
    FlushOptions flush_opts;
    flush_opts.allow_write_stall = true;
    flush_opts.wait = false;
    flush_opts._write_stopped = (sentinel->db == this_db);
    flush_opts.expected_oldest_key_time = std::get<2>(*candidate);
    candidate++;
    if (candidate != candidates.rend()) {
      // Don't check it for the last candidate. Otherwise we could end up
      // never progressing.
      flush_opts.check_if_compaction_disabled = true;
    }
    auto s = sentinel->db->Flush(flush_opts, sentinel->cf);
    if (s.ok()) {
      return;
    }
    auto opts = sentinel->db->GetDBOptions();
    ROCKS_LOG_WARN(opts.info_log, "WriteBufferManager fails to flush: %s",
                   s.ToString().c_str());
    // Fallback to the next best candidate.
  }
}

void WriteBufferManager::BeginWriteStall(StallInterface* wbm_stall) {
  assert(wbm_stall != nullptr);
  assert(allow_stall_);

  // Allocate outside of the lock.
  std::list<StallInterface*> new_node = {wbm_stall};

  {
    std::unique_lock<std::mutex> lock(stall_mu_);
    // Verify if the stall conditions are stil active.
    if (ShouldStall()) {
      stall_active_.store(true, std::memory_order_relaxed);
      queue_.splice(queue_.end(), std::move(new_node));
    }
  }

  // If the node was not consumed, the stall has ended already and we can signal
  // the caller.
  if (!new_node.empty()) {
    new_node.front()->Signal();
  }
}

// Called when memory is freed in FreeMem or the buffer size has changed.
void WriteBufferManager::MaybeEndWriteStall() {
  // Cannot early-exit on !enabled() because SetFlushSize(0) needs to unblock
  // the writers.
  if (!allow_stall_) {
    return;
  }

  if (is_stall_threshold_exceeded()) {
    return;  // Stall conditions have not resolved.
  }

  // Perform all deallocations outside of the lock.
  std::list<StallInterface*> cleanup;

  std::unique_lock<std::mutex> lock(stall_mu_);
  if (!stall_active_.load(std::memory_order_relaxed)) {
    return;  // Nothing to do.
  }

  // Unblock new writers.
  stall_active_.store(false, std::memory_order_relaxed);

  // Unblock the writers in the queue.
  for (StallInterface* wbm_stall : queue_) {
    wbm_stall->Signal();
  }
  cleanup = std::move(queue_);
}

void WriteBufferManager::RemoveFromStallQueue(StallInterface* wbm_stall) {
  assert(wbm_stall != nullptr);

  // Deallocate the removed nodes outside of the lock.
  std::list<StallInterface*> cleanup;

  if (allow_stall_) {
    std::unique_lock<std::mutex> lock(stall_mu_);
    for (auto it = queue_.begin(); it != queue_.end();) {
      auto next = std::next(it);
      if (*it == wbm_stall) {
        cleanup.splice(cleanup.end(), queue_, std::move(it));
      }
      it = next;
    }
  }
  wbm_stall->Signal();
}

}  // namespace ROCKSDB_NAMESPACE
