//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_builder.h"

#include <cinttypes>
#include <algorithm>
#include <atomic>
#include <functional>
#include <map>
#include <set>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "db/dbformat.h"
#include "db/internal_stats.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "port/port.h"
#include "table/table_reader.h"
#include "util/string_util.h"

namespace rocksdb {

bool NewestFirstBySeqNo(FileMetaData* a, FileMetaData* b) {
  if (a->fd.largest_seqno != b->fd.largest_seqno) {
    return a->fd.largest_seqno > b->fd.largest_seqno;
  }
  if (a->fd.smallest_seqno != b->fd.smallest_seqno) {
    return a->fd.smallest_seqno > b->fd.smallest_seqno;
  }
  // Break ties by file number
  return a->fd.GetNumber() > b->fd.GetNumber();
}

namespace {
bool BySmallestKey(FileMetaData* a, FileMetaData* b,
                   const InternalKeyComparator* cmp) {
  int r = cmp->Compare(a->smallest, b->smallest);
  if (r != 0) {
    return (r < 0);
  }
  // Break ties by file number
  return (a->fd.GetNumber() < b->fd.GetNumber());
}
}  // namespace

class VersionBuilder::Rep {
 private:
  // Helper to sort files_ in v
  // kLevel0 -- NewestFirstBySeqNo
  // kLevelNon0 -- BySmallestKey
  struct FileComparator {
    enum SortMethod { kLevel0 = 0, kLevelNon0 = 1, } sort_method;
    const InternalKeyComparator* internal_comparator;

    FileComparator() : internal_comparator(nullptr) {}

    bool operator()(FileMetaData* f1, FileMetaData* f2) const {
      switch (sort_method) {
        case kLevel0:
          return NewestFirstBySeqNo(f1, f2);
        case kLevelNon0:
          return BySmallestKey(f1, f2, internal_comparator);
      }
      assert(false);
      return false;
    }
  };

  struct LevelState {
    // Files in base version that should be deleted.
    std::unordered_set<uint64_t> deleted_base_files;
    // Files moved from base version.
    // Those files will not be referenced by VersionBuilder.
    std::unordered_map<uint64_t, FileMetaData*> moved_files;
    // Files added, must not intersect with moved_files.
    // Those files will be referenced during the lifetime of VersionBuilder.
    std::unordered_map<uint64_t, FileMetaData*> added_files;
  };

  const EnvOptions& env_options_;
  Logger* info_log_;
  TableCache* table_cache_;
  VersionStorageInfo* base_vstorage_;
  int num_levels_;
  LevelState* levels_;
  // Store sizes of levels larger than num_levels_. We do this instead of
  // storing them in levels_ to avoid regression in case there are no files
  // on invalid levels. The version is not consistent if in the end the files
  // on invalid levels don't cancel out.
  std::unordered_map<int, size_t> invalid_level_sizes_;
  // Whether there are invalid new files or invalid deletion on levels larger
  // than num_levels_.
  bool has_invalid_levels_;
  // Current levels of table files affected by additions/deletions.
  std::unordered_map<uint64_t, int> table_file_levels_;
  FileComparator level_zero_cmp_;
  FileComparator level_nonzero_cmp_;

 public:
  Rep(const EnvOptions& env_options, Logger* info_log, TableCache* table_cache,
      VersionStorageInfo* base_vstorage)
      : env_options_(env_options),
        info_log_(info_log),
        table_cache_(table_cache),
        base_vstorage_(base_vstorage),
        num_levels_(base_vstorage->num_levels()),
        has_invalid_levels_(false) {
    levels_ = new LevelState[num_levels_];
    level_zero_cmp_.sort_method = FileComparator::kLevel0;
    level_nonzero_cmp_.sort_method = FileComparator::kLevelNon0;
    level_nonzero_cmp_.internal_comparator =
        base_vstorage_->InternalComparator();
  }

  ~Rep() {
    for (int level = 0; level < num_levels_; level++) {
      const auto& added = levels_[level].added_files;
      for (auto& pair : added) {
        UnrefFile(pair.second);
      }
    }

    delete[] levels_;
  }

  void UnrefFile(FileMetaData* f) {
    if (f->Unref()) {
      if (f->table_reader_handle) {
        assert(table_cache_ != nullptr);
        table_cache_->ReleaseHandle(f->table_reader_handle);
        f->table_reader_handle = nullptr;
      }
      delete f;
    }
  }

  Status CheckConsistency(VersionStorageInfo* vstorage) {
#ifdef NDEBUG
    if (!vstorage->force_consistency_checks()) {
      // Dont run consistency checks in release mode except if
      // explicitly asked to
      return Status::OK();
    }
#endif
    // make sure the files are sorted correctly
    for (int level = 0; level < num_levels_; level++) {
      auto& level_files = vstorage->LevelFiles(level);
      for (size_t i = 1; i < level_files.size(); i++) {
        auto f1 = level_files[i - 1];
        auto f2 = level_files[i];
#ifndef NDEBUG
        auto pair = std::make_pair(&f1, &f2);
        TEST_SYNC_POINT_CALLBACK("VersionBuilder::CheckConsistency", &pair);
#endif
        if (level == 0) {
          if (!level_zero_cmp_(f1, f2)) {
            fprintf(stderr, "L0 files are not sorted properly");
            return Status::Corruption("L0 files are not sorted properly");
          }

          if (f2->fd.smallest_seqno == f2->fd.largest_seqno) {
            // This is an external file that we ingested
            SequenceNumber external_file_seqno = f2->fd.smallest_seqno;
            // If f1 and f2 are both ingested files, their seqno may be same.
            // Because RocksDB will assign only one seqno for all files ingested
            // in once call by `IngestExternalFile`.
            if (!((f1->fd.smallest_seqno == f1->fd.largest_seqno &&
                   f1->fd.smallest_seqno == external_file_seqno) ||
                  external_file_seqno < f1->fd.largest_seqno ||
                  external_file_seqno == 0)) {
              fprintf(stderr,
                      "L0 file with seqno %" PRIu64 " %" PRIu64
                      " vs. file with global_seqno %" PRIu64 "\n",
                      f1->fd.smallest_seqno, f1->fd.largest_seqno,
                      external_file_seqno);
              return Status::Corruption("L0 file with seqno " +
                                        NumberToString(f1->fd.smallest_seqno) +
                                        " " +
                                        NumberToString(f1->fd.largest_seqno) +
                                        " vs. file with global_seqno" +
                                        NumberToString(external_file_seqno) +
                                        " with fileNumber " +
                                        NumberToString(f1->fd.GetNumber()));
            }
          } else if (f1->fd.smallest_seqno <= f2->fd.smallest_seqno) {
            fprintf(stderr,
                    "L0 files seqno %" PRIu64 " %" PRIu64 " vs. %" PRIu64
                    " %" PRIu64 "\n",
                    f1->fd.smallest_seqno, f1->fd.largest_seqno,
                    f2->fd.smallest_seqno, f2->fd.largest_seqno);
            return Status::Corruption(
                "L0 files seqno " + NumberToString(f1->fd.smallest_seqno) +
                " " + NumberToString(f1->fd.largest_seqno) + " " +
                NumberToString(f1->fd.GetNumber()) + " vs. " +
                NumberToString(f2->fd.smallest_seqno) + " " +
                NumberToString(f2->fd.largest_seqno) + " " +
                NumberToString(f2->fd.GetNumber()));
          }
        } else {
          if (!level_nonzero_cmp_(f1, f2)) {
            fprintf(stderr, "L%d files are not sorted properly", level);
            return Status::Corruption("L" + NumberToString(level) +
                                      " files are not sorted properly");
          }

          // Make sure there is no overlap in levels > 0
          if (vstorage->InternalComparator()->Compare(f1->largest,
                                                      f2->smallest) >= 0) {
            fprintf(
                stderr,
                "L%d have overlapping ranges %s of file %s vs. %s of file %s\n",
                level, (f1->largest).DebugString(true).c_str(),
                NumberToString(f1->fd.GetNumber()).c_str(),
                (f2->smallest).DebugString(true).c_str(),
                NumberToString(f2->fd.GetNumber()).c_str());
            return Status::Corruption(
                "L" + NumberToString(level) + " have overlapping ranges " +
                (f1->largest).DebugString(true) + " of file " +
                NumberToString(f1->fd.GetNumber()) + " vs. " +
                (f2->smallest).DebugString(true) + " of file " +
                NumberToString(f2->fd.GetNumber()));
          }
        }
      }
    }
    return Status::OK();
  }

  bool CheckConsistencyForNumLevels() const {
    // Make sure there are no files on or beyond num_levels().
    if (has_invalid_levels_) {
      return false;
    }

    for (const auto& pair : invalid_level_sizes_) {
      const size_t level_size = pair.second;
      if (level_size != 0) {
        return false;
      }
    }

    return true;
  }

  int GetCurrentLevelForTableFile(uint64_t file_number) const {
    auto it = table_file_levels_.find(file_number);
    if (it != table_file_levels_.end()) {
      return it->second;
    }

    assert(base_vstorage_);
    return base_vstorage_->GetFileLocation(file_number).GetLevel();
  }

  Status ApplyFileDeletion(int level, uint64_t file_number) {
    assert(level != VersionStorageInfo::FileLocation::Invalid().GetLevel());

    const int current_level = GetCurrentLevelForTableFile(file_number);

    if (level != current_level) {
      if (level >= num_levels_) {
        has_invalid_levels_ = true;
      }

      std::ostringstream oss;
      oss << "Cannot delete table file #" << file_number << " from level "
          << level << " since it is ";
      if (current_level ==
          VersionStorageInfo::FileLocation::Invalid().GetLevel()) {
        oss << "not in the LSM tree";
      } else {
        oss << "on level " << current_level;
      }

      return Status::Corruption("VersionBuilder", oss.str());
    }

    if (level >= num_levels_) {
      assert(invalid_level_sizes_[level] > 0);
      --invalid_level_sizes_[level];

      table_file_levels_[file_number] =
          VersionStorageInfo::FileLocation::Invalid().GetLevel();

      return Status::OK();
    }

    auto& level_state = levels_[level];

    auto& add_files = level_state.added_files;
    auto add_it = add_files.find(file_number);
    if (add_it != add_files.end()) {
      UnrefFile(add_it->second);
      add_files.erase(add_it);
    }

    auto& moved_files = level_state.moved_files;
    auto moved_it = moved_files.find(file_number);
    if (moved_it != moved_files.end()) {
      moved_files.erase(moved_it);
    }

    auto& del_files = level_state.deleted_base_files;
    assert(del_files.find(file_number) == del_files.end());
    del_files.emplace(file_number);

    table_file_levels_[file_number] =
        VersionStorageInfo::FileLocation::Invalid().GetLevel();

    return Status::OK();
  }

  Status ApplyFileAddition(int level, const FileMetaData& meta) {
    assert(level != VersionStorageInfo::FileLocation::Invalid().GetLevel());

    const uint64_t file_number = meta.fd.GetNumber();

    const int current_level = GetCurrentLevelForTableFile(file_number);

    if (current_level !=
        VersionStorageInfo::FileLocation::Invalid().GetLevel()) {
      if (level >= num_levels_) {
        has_invalid_levels_ = true;
      }

      std::ostringstream oss;
      oss << "Cannot add table file #" << file_number << " to level " << level
          << " since it is already in the LSM tree on level " << current_level;
      return Status::Corruption("VersionBuilder", oss.str());
    }

    if (level >= num_levels_) {
      ++invalid_level_sizes_[level];
      table_file_levels_[file_number] = level;

      return Status::OK();
    }

    auto& level_state = levels_[level];
    // Try to reuse file meta from base version.
    FileMetaData* f = base_vstorage_->GetFileMetaDataByNumber(file_number);
    std::unordered_map<uint64_t, FileMetaData*>* container = nullptr;
    if (f != nullptr) {
      // This should be a file trivially moved to a new position. Make sure the
      // two are the same physical file.
      if (f->fd.GetPathId() != meta.fd.GetPathId()) {
        std::ostringstream oss;
        oss << "Cannot add table file #" << file_number << " to level " << level
            << " by trivial move since it isn't trivial to move to a "
            << "different path";
        return Status::Corruption("VersionBuilder", oss.str());
      }
      // No need to Ref() this file held by base_vstorage_.
      container = &level_state.moved_files;
    } else {
      f = new FileMetaData(meta);
      // Will drop reference in dtor.
      f->Ref();
      container = &level_state.added_files;
    }

    assert(container && container->find(file_number) == container->end());
    container->emplace(file_number, f);

    table_file_levels_[file_number] = level;

    return Status::OK();
  }

  // Apply all of the edits in *edit to the current state.
  Status Apply(VersionEdit* edit) {
    {
      const Status s = CheckConsistency(base_vstorage_);
      if (!s.ok()) {
        return s;
      }
    }

    // Delete files
    for (const auto& deleted_file : edit->GetDeletedFiles()) {
      const int level = deleted_file.first;
      const uint64_t file_number = deleted_file.second;

      const Status s = ApplyFileDeletion(level, file_number);
      if (!s.ok()) {
        return s;
      }
    }

    // Add new files
    for (const auto& new_file : edit->GetNewFiles()) {
      const int level = new_file.first;
      const FileMetaData& meta = new_file.second;

      const Status s = ApplyFileAddition(level, meta);
      if (!s.ok()) {
        return s;
      }
    }

    return Status::OK();
  }

  // Save the current state in *v.
  Status SaveTo(VersionStorageInfo* vstorage) {
    Status s = CheckConsistency(base_vstorage_);
    if (!s.ok()) {
      return s;
    }

    s = CheckConsistency(vstorage);
    if (!s.ok()) {
      return s;
    }

    uint64_t total_file_size = 0;
    int64_t delta_file_size = 0;
    for (int level = 0; level < num_levels_; level++) {
      const auto& cmp = (level == 0) ? level_zero_cmp_ : level_nonzero_cmp_;
      // Merge the set of added files with the set of pre-existing files.
      // Drop any deleted files.  Store the result in *v.
      const auto& base_files = base_vstorage_->LevelFiles(level);
      const auto& del_files = levels_[level].deleted_base_files;
      const auto& unordered_added_files = levels_[level].added_files;
      const auto& unordered_moved_files = levels_[level].moved_files;

      vstorage->Reserve(level,
                        base_files.size() + unordered_added_files.size());

      // Sort delta files for the level.
      std::vector<FileMetaData*> delta_files;
      delta_files.reserve(unordered_added_files.size() +
                          unordered_moved_files.size());
      for (const auto& pair : unordered_added_files) {
        delta_files.push_back(pair.second);
      }
      for (const auto& pair : unordered_moved_files) {
        // SaveTo will always be called under db mutex.
        pair.second->being_moved_to = level;
        delta_files.push_back(pair.second);
      }
      std::sort(delta_files.begin(), delta_files.end(), cmp);

#ifndef NDEBUG
      FileMetaData* prev_file = nullptr;
      for (const auto& delta : delta_files) {
        if (level > 0 && prev_file != nullptr) {
          assert(base_vstorage_->InternalComparator()->Compare(
                     prev_file->smallest, delta->smallest) <= 0);
        }
        prev_file = delta;
      }
#endif

      auto base_iter = base_files.begin();
      auto base_end = base_files.end();
      auto delta_iter = delta_files.begin();
      auto delta_end = delta_files.end();

      // Delta file supersedes base file because base is masked by
      // deleted_base_files.
      while (delta_iter != delta_end || base_iter != base_end) {
        if (delta_iter == delta_end ||
            (base_iter != base_end && cmp(*base_iter, *delta_iter))) {
          FileMetaData* f = *base_iter++;
          const uint64_t file_number = f->fd.GetNumber();

          if (del_files.find(file_number) != del_files.end()) {
            // vstorage inherited base_vstorage_'s stats, need to account for
            // deleted base files.
            vstorage->RemoveCurrentStats(f);
          } else {
#ifndef NDEBUG
            assert(unordered_added_files.find(file_number) ==
                   unordered_added_files.end());
#endif
            vstorage->AddFile(level, f, false /*new_file*/, info_log_);
          }
        } else {
          FileMetaData* f = *delta_iter++;
          if (f->init_stats_from_file) {
            // A moved file whose stats is inited by base_vstorage_ and then
            // deleted from it.
            vstorage->UpdateAccumulatedStats(f);
          }
          vstorage->AddFile(level, f, f->being_moved_to != level /*new_file*/, info_log_);
        }
      }
    }

    s = CheckConsistency(vstorage);
    return s;
  }

  Status LoadTableHandlers(InternalStats* internal_stats, int max_threads,
                           bool prefetch_index_and_filter_in_cache,
                           bool is_initial_load,
                           const SliceTransform* prefix_extractor) {
    assert(table_cache_ != nullptr);

    size_t table_cache_capacity = table_cache_->get_cache()->GetCapacity();
    bool always_load = (table_cache_capacity == TableCache::kInfiniteCapacity);
    size_t max_load = port::kMaxSizet;

    if (!always_load) {
      // If it is initial loading and not set to always laoding all the
      // files, we only load up to kInitialLoadLimit files, to limit the
      // time reopening the DB.
      const size_t kInitialLoadLimit = 16;
      size_t load_limit;
      // If the table cache is not 1/4 full, we pin the table handle to
      // file metadata to avoid the cache read costs when reading the file.
      // The downside of pinning those files is that LRU won't be followed
      // for those files. This doesn't matter much because if number of files
      // of the DB excceeds table cache capacity, eventually no table reader
      // will be pinned and LRU will be followed.
      if (is_initial_load) {
        load_limit = std::min(kInitialLoadLimit, table_cache_capacity / 4);
      } else {
        load_limit = table_cache_capacity / 4;
      }

      size_t table_cache_usage = table_cache_->get_cache()->GetUsage();
      if (table_cache_usage >= load_limit) {
        // TODO (yanqin) find a suitable status code.
        return Status::OK();
      } else {
        max_load = load_limit - table_cache_usage;
      }
    }

    // <file metadata, level>
    std::vector<std::pair<FileMetaData*, int>> files_meta;
    std::vector<Status> statuses;
    for (int level = 0; level < num_levels_; level++) {
      for (auto& file_meta_pair : levels_[level].added_files) {
        auto* file_meta = file_meta_pair.second;
        // If the file has been opened before, just skip it.
        if (!file_meta->table_reader_handle) {
          files_meta.emplace_back(file_meta, level);
          statuses.emplace_back(Status::OK());
        }
        if (files_meta.size() >= max_load) {
          break;
        }
      }
      if (files_meta.size() >= max_load) {
        break;
      }
    }

    std::atomic<size_t> next_file_meta_idx(0);
    std::function<void()> load_handlers_func([&]() {
      while (true) {
        size_t file_idx = next_file_meta_idx.fetch_add(1);
        if (file_idx >= files_meta.size()) {
          break;
        }

        auto* file_meta = files_meta[file_idx].first;
        int level = files_meta[file_idx].second;
        statuses[file_idx] = table_cache_->FindTable(
            env_options_, *(base_vstorage_->InternalComparator()),
            file_meta->fd, &file_meta->table_reader_handle, prefix_extractor,
            false /*no_io */, true /* record_read_stats */,
            internal_stats->GetFileReadHist(level), false, level,
            prefetch_index_and_filter_in_cache);
        if (file_meta->table_reader_handle != nullptr) {
          // Load table_reader
          file_meta->fd.table_reader = table_cache_->GetTableReaderFromHandle(
              file_meta->table_reader_handle);
        }
      }
    });

    std::vector<port::Thread> threads;
    for (int i = 1; i < max_threads; i++) {
      threads.emplace_back(load_handlers_func);
    }
    load_handlers_func();
    for (auto& t : threads) {
      t.join();
    }
    for (const auto& s : statuses) {
      if (!s.ok()) {
        return s;
      }
    }
    return Status::OK();
  }
};

VersionBuilder::VersionBuilder(const EnvOptions& env_options,
                               TableCache* table_cache,
                               VersionStorageInfo* base_vstorage,
                               Logger* info_log)
    : rep_(new Rep(env_options, info_log, table_cache, base_vstorage)) {}

VersionBuilder::~VersionBuilder() { delete rep_; }

Status VersionBuilder::CheckConsistency(VersionStorageInfo* vstorage) {
  return rep_->CheckConsistency(vstorage);
}

bool VersionBuilder::CheckConsistencyForNumLevels() {
  return rep_->CheckConsistencyForNumLevels();
}

Status VersionBuilder::Apply(VersionEdit* edit) { return rep_->Apply(edit); }

Status VersionBuilder::SaveTo(VersionStorageInfo* vstorage) {
  return rep_->SaveTo(vstorage);
}

Status VersionBuilder::LoadTableHandlers(
    InternalStats* internal_stats, int max_threads,
    bool prefetch_index_and_filter_in_cache, bool is_initial_load,
    const SliceTransform* prefix_extractor) {
  return rep_->LoadTableHandlers(internal_stats, max_threads,
                                 prefetch_index_and_filter_in_cache,
                                 is_initial_load, prefix_extractor);
}
}  // namespace rocksdb
