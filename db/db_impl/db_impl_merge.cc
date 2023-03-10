//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_impl/db_impl.h"

namespace ROCKSDB_NAMESPACE {

/// A RAII-style helper used to block DB writes.
class WriteBlocker {
 public:
  WriteBlocker(DBImpl* db) : db_(db), writer_(new WriteThread::Writer()) {
    db_->mutex_.Lock();
    db_->write_thread_.EnterUnbatched(writer_.get(), &db_->mutex_);
    db_->WaitForPendingWrites();
  }

  ~WriteBlocker() {
    db_->write_thread_.ExitUnbatched(writer_.get());
    db_->mutex_.Unlock();
  }

 private:
  DBImpl* db_;
  std::unique_ptr<WriteThread::Writer> writer_;
};

Status DBImpl::ValidateForMerge(const MergeInstanceOptions& mopts,
                                DBImpl* rhs) {
  if (rhs->two_write_queues_) {
    return Status::NotSupported("two_write_queues == true");
  }
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    auto rhs_cfd =
        rhs->versions_->GetColumnFamilySet()->GetColumnFamily(cfd->GetName());
    if (rhs_cfd != nullptr) {
      if (strcmp(cfd->ioptions()->table_factory->Name(),
                 rhs_cfd->ioptions()->table_factory->Name()) != 0) {
        return Status::InvalidArgument(
            "table_factory must be of the same type");
      }
    }
  }
  if (mopts.merge_memtable) {
    if (rhs->total_log_size_ > 0) {
      return Status::InvalidArgument("DB WAL is not empty");
    }
  }
  if (rhs->table_cache_ == table_cache_) {
    return Status::InvalidArgument("table_cache must not be shared");
  }
  return Status::OK();
}

Status DBImpl::MergeDisjointInstances(const MergeInstanceOptions& merge_options,
                                      const std::vector<DB*>& instances) {
  Status s;
  autovector<ColumnFamilyData*> this_cfds;
  for (auto cfd : *versions_->GetColumnFamilySet()) {
    assert(cfd != nullptr);
    if (!cfd->IsDropped()) {
      this_cfds.push_back(cfd);
    }
  }
  const size_t num_cfs = this_cfds.size();

  // # Sanity checks
  // Check target instance (`this`).
  if (two_write_queues_) {
    return Status::NotSupported("target instance two_write_queues == true");
  }
  autovector<DBImpl*> db_impls;
  autovector<DBImpl*> all_db_impls{this};
  // A list of source db super versions grouped by cf. nullptr if the cf is
  // missing.
  autovector<autovector<SuperVersion*>> cf_db_super_versions;
  std::shared_ptr<void> _defer(nullptr, [&](...) {
    for (auto& db_super_versions : cf_db_super_versions) {
      for (auto* super_version : db_super_versions) {
        if (super_version != nullptr && super_version->Unref()) {
          super_version->Cleanup();
        }
      }
    }
  });
  // Check source instances.
  for (size_t i = 0; i < instances.size(); i++) {
    auto* db_impl = static_cast<DBImpl*>(instances[i]);
    s = ValidateForMerge(merge_options, db_impl);
    if (s.ok()) {
      db_impls.push_back(db_impl);
      all_db_impls.push_back(db_impl);
    } else {
      return s;
    }
  }

  // Block all writes.
  autovector<std::unique_ptr<WriteBlocker>> write_blockers;
  for (auto* db : all_db_impls) {
    write_blockers.emplace_back(new WriteBlocker(db));
  }

  // # Internal key range check
  assert(s.ok());
  for (auto* this_cfd : this_cfds) {
    auto& name = this_cfd->GetName();
    auto* comparator = this_cfd->user_comparator();
    using CfRange = std::pair<PinnableSlice, PinnableSlice>;
    std::vector<CfRange> db_ranges;
    auto process_cf = [&](ColumnFamilyData* cfd) {
      assert(cfd && s.ok());
      PinnableSlice smallest, largest;
      bool found = false;
      s = cfd->GetUserKeyRange(&smallest, &largest, &found);
      if (s.ok() && found) {
        db_ranges.emplace_back(
            std::make_pair(std::move(smallest), std::move(largest)));
      }
    };
    process_cf(this_cfd);
    if (!s.ok()) {
      return s;
    }
    for (auto* db : db_impls) {
      auto cfd = db->versions_->GetColumnFamilySet()->GetColumnFamily(name);
      if (cfd && !cfd->IsDropped()) {
        process_cf(cfd);
        if (!s.ok()) {
          return s;
        }
      }
    }
    std::sort(db_ranges.begin(), db_ranges.end(),
              [=](const CfRange& a, const CfRange& b) {
                return comparator->Compare(a.first, b.first) < 0;
              });
    Slice last_largest;
    for (auto& range : db_ranges) {
      if (last_largest.size() == 0 ||
          comparator->Compare(last_largest, range.first) < 0) {
        last_largest = range.second;
      } else {
        return Status::InvalidArgument("Source DBs have overlapping range");
      }
    }
  }

  // # Handle transient states
  //
  // - Acquire snapshots of table files (`SuperVersion`).
  //
  // - Do memtable merge if needed. We do this together with acquiring snapshot
  //   to avoid the case where a memtable is flushed shortly after being
  //   merged, and the resulting L0 data is merged again as a table file.
  assert(s.ok());
  autovector<MemTable*> to_delete;  // not used.
  // Key-value freshness is determined by its sequence number. To avoid
  // incoming writes being shadowed by history data from other instances, we
  // must increment target instance's sequence number to be larger than all
  // source data. See [A].
  uint64_t max_seq_number = 0;
  // RocksDB's recovery is heavily dependent on the one-on-one mapping between
  // memtable and WAL (even when WAL is empty). Each memtable keeps a record of
  // `next_log_number` to mark its position within a series of WALs. This
  // counter must be monotonic. We work around this issue by setting the
  // counters of all involved memtables to the same maximum value. See [B].
  uint64_t max_log_number = 0;
  for (auto* db : all_db_impls) {
    max_seq_number = std::max(max_seq_number, db->versions_->LastSequence());
    max_log_number = std::max(max_log_number, db->logfile_number_);
  }
  // [A] Bump sequence number.
  versions_->SetLastAllocatedSequence(max_seq_number);
  versions_->SetLastSequence(max_seq_number);
  cf_db_super_versions.resize(num_cfs);
  for (size_t cf_i = 0; cf_i < num_cfs; cf_i++) {
    cf_db_super_versions[cf_i].resize(db_impls.size());
    auto* this_cfd = this_cfds[cf_i];
    auto& cf_name = this_cfd->GetName();
    autovector<MemTable*> mems;
    for (size_t db_i = 0; db_i < db_impls.size(); db_i++) {
      auto& db = db_impls[db_i];
      auto cfd = db->versions_->GetColumnFamilySet()->GetColumnFamily(cf_name);
      if (cfd == nullptr || cfd->IsDropped()) {
        cf_db_super_versions[cf_i][db_i] = nullptr;
        continue;
      }

      if (merge_options.merge_memtable) {
        if (!cfd->mem()->IsEmpty()) {
          WriteContext write_context;
          assert(log_empty_);
          s = SwitchMemtable(cfd, &write_context);
          if (!s.ok()) {
            return s;
          }
        }
        assert(cfd->mem()->IsEmpty());

        // [B] Bump log number for active memtable. Even though it's not
        // shared, it must still be larger than other shared immutable
        // memtables.
        cfd->mem()->SetNextLogNumber(max_log_number);
        cfd->imm()->ExportMemtables(&mems);
      }

      // Acquire super version.
      cf_db_super_versions[cf_i][db_i] = cfd->GetSuperVersion()->Ref();
    }
    for (auto mem : mems) {
      assert(mem != nullptr);
      mem->Ref();
      // [B] Bump log number for shared memtables.
      mem->SetNextLogNumber(max_log_number);
      this_cfd->imm()->Add(mem, &to_delete);
    }
    this_cfd->mem()->SetNextLogNumber(max_log_number);
  }
  for (size_t i = 0; i < all_db_impls.size(); i++) {
    auto* db = all_db_impls[i];
    bool check_log_number = (i == 0 || merge_options.allow_source_write) &&
                            merge_options.merge_memtable;
    if (check_log_number && max_log_number != db->logfile_number_) {
      assert(max_log_number > db->logfile_number_);
      // [B] Create a new WAL so that future memtable will use the correct log
      // number as well.
      log::Writer* new_log = nullptr;
      s = db->CreateWAL(max_log_number, 0 /*recycle_log_number*/,
                        0 /*preallocate_block_size*/, &new_log);
      if (!s.ok()) {
        return s;
      }
      db->logfile_number_ = max_log_number;
      assert(new_log != nullptr);
      db->logs_.emplace_back(max_log_number, new_log);
      auto current = db->versions_->current_next_file_number();
      if (current <= max_log_number) {
        db->versions_->FetchAddFileNumber(max_log_number - current + 1);
      }
    }
  }

  // Unblock writes.
  write_blockers.clear();

  TEST_SYNC_POINT("DBImpl::MergeDisjointInstances:AfterMergeMemtable:1");

  // # Merge table files
  assert(s.ok());
  autovector<VersionEdit> cf_edits;
  cf_edits.resize(num_cfs);
  for (size_t cf_i = 0; cf_i < num_cfs; cf_i++) {
    auto* this_cfd = this_cfds[cf_i];
    auto& edit = cf_edits[cf_i];
    edit.SetColumnFamily(this_cfd->GetID());
    for (size_t db_i = 0; db_i < db_impls.size(); db_i++) {
      auto* super_version = cf_db_super_versions[cf_i][db_i];
      if (super_version == nullptr) {
        continue;
      }
      VersionStorageInfo& vsi = *super_version->current->storage_info();
      auto& cf_paths = super_version->cfd->ioptions()->cf_paths;
      auto SourcePath = [&](size_t path_id) {
        // Matching `TableFileName()`.
        if (path_id >= cf_paths.size()) {
          assert(false);
          return cf_paths.back().path;
        } else {
          return cf_paths[path_id].path;
        }
      };
      const auto& target_path = this_cfd->ioptions()->cf_paths.front().path;
      const uint64_t target_path_id = 0;
      for (int level = 0; level < vsi.num_levels(); ++level) {
        for (const auto& f : vsi.LevelFiles(level)) {
          assert(f != nullptr);
          const uint64_t source_file_number = f->fd.GetNumber();
          const uint64_t target_file_number = versions_->FetchAddFileNumber(1);
          std::string src = MakeTableFileName(SourcePath(f->fd.GetPathId()),
                                              source_file_number);
          std::string target =
              MakeTableFileName(target_path, target_file_number);
          s = GetEnv()->LinkFile(src, target);
          if (!s.ok()) {
            return s;
          }
          edit.AddFile(
              level, target_file_number, target_path_id, f->fd.GetFileSize(),
              f->smallest, f->largest, f->fd.smallest_seqno,
              f->fd.largest_seqno, f->marked_for_compaction, f->temperature,
              f->oldest_blob_file_number, f->oldest_ancester_time,
              f->file_creation_time, f->file_checksum,
              f->file_checksum_func_name, f->min_timestamp, f->max_timestamp);
        }
      }
    }
  }

  // # Apply version edits
  assert(s.ok());
  {
    autovector<autovector<VersionEdit*>> edit_ptrs;
    autovector<const MutableCFOptions*> cf_mopts;
    for (size_t i = 0; i < num_cfs; i++) {
      edit_ptrs.push_back({&cf_edits[i]});
      cf_mopts.push_back(this_cfds[i]->GetLatestMutableCFOptions());
    }

    auto old_capacity = table_cache_->GetCapacity();
    if (merge_options.max_preload_files >= 0) {
      // Refer to `LoadTableHandlers` for calculation details.
      // This trick will be wrong if table_cache is shared.
      table_cache_->SetCapacity(
          (table_cache_->GetUsage() + merge_options.max_preload_files) * 4);
    }

    InstrumentedMutexLock lock(&mutex_);
    s = versions_->LogAndApply(this_cfds, cf_mopts, edit_ptrs, &mutex_,
                               directories_.GetDbDir(), false);
    if (!s.ok()) {
      return s;
    }
    for (size_t i = 0; i < num_cfs; i++) {
      SuperVersionContext sv_context(/* create_superversion */ true);
      InstallSuperVersionAndScheduleWork(this_cfds[i], &sv_context,
                                         *cf_mopts[i]);
      sv_context.Clean();
    }

    if (immutable_db_options_.atomic_flush) {
      AssignAtomicFlushSeq(this_cfds);
    }
    for (auto cfd : this_cfds) {
      cfd->imm()->FlushRequested();
      if (!immutable_db_options_.atomic_flush) {
        FlushRequest flush_req;
        GenerateFlushRequest({cfd}, &flush_req);
        SchedulePendingFlush(flush_req, FlushReason::kWriteBufferFull);
      }
    }
    if (immutable_db_options_.atomic_flush) {
      FlushRequest flush_req;
      GenerateFlushRequest(this_cfds, &flush_req);
      SchedulePendingFlush(flush_req, FlushReason::kWriteBufferFull);
    }
    for (auto cfd : this_cfds) {
      SchedulePendingCompaction(cfd);
    }
    MaybeScheduleFlushOrCompaction();

    if (merge_options.max_preload_files >= 0) {
      table_cache_->SetCapacity(old_capacity);
    }
  }

  assert(s.ok());
  return s;
}
}  // namespace ROCKSDB_NAMESPACE
