//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <string>
#include <unordered_map>

#include "db/db_test_util.h"
#include "port/stack_trace.h"

namespace ROCKSDB_NAMESPACE {

const uint32_t default_cf = 0;
uint32_t operator"" _db(unsigned long long int i) { return i; }
uint32_t operator"" _cf(unsigned long long int i) {
  assert(i > 0);
  return i;
}

class DBMergeTest : public testing::Test {
  struct DBHandles {
    std::string path;
    DBImpl* db;
    std::unordered_map<uint32_t, ColumnFamilyHandle*> cfs;
  };

 public:
  DBMergeTest() {
    options_.create_if_missing = true;
    options_.write_buffer_manager.reset(
        new WriteBufferManager(options_.db_write_buffer_size));
    // avoid stalling the tests.
    options_.disable_write_stall = true;
    options_.avoid_flush_during_shutdown = true;
    // avoid background flush/compaction.
    options_.level0_file_num_compaction_trigger = 10;
    options_.level0_slowdown_writes_trigger = 10;
    options_.level0_stop_writes_trigger = 10;
    options_.max_write_buffer_number = 10;
  }

  ~DBMergeTest() { DestroyAll(); }

  void IsOverlapError(Status s) {
    ASSERT_EQ(s.ToString(),
              "Invalid argument: Source DBs have overlapping range");
  }

  void IsWALNotEmpty(Status s) {
    ASSERT_EQ(s.ToString(), "Invalid argument: DB WAL is not empty");
  }

  // 0 for default cf.
  std::vector<ColumnFamilyDescriptor> GenColumnFamilyDescriptors(
      const std::vector<uint32_t>& cf_ids) {
    std::vector<ColumnFamilyDescriptor> column_families;
    for (auto cf_id : cf_ids) {
      if (cf_id == 0) {
        column_families.push_back(
            ColumnFamilyDescriptor(ROCKSDB_NAMESPACE::kDefaultColumnFamilyName,
                                   ColumnFamilyOptions(options_)));
      } else {
        column_families.push_back(ColumnFamilyDescriptor(
            std::to_string(cf_id), ColumnFamilyOptions(options_)));
      }
    }
    return std::move(column_families);
  }

  std::string GenDBPath(uint32_t db_id) {
    return test::PerThreadDBPath(env_, std::to_string(db_id));
  }

  void AddDB(uint32_t db_id, DB* db,
             std::vector<ColumnFamilyHandle*> cf_handles) {
    assert(dbs_.count(db_id) == 0);
    DBHandles db_handles;
    db_handles.path = GenDBPath(db_id);
    db_handles.db = static_cast<DBImpl*>(db);
    for (auto* handle : cf_handles) {
      uint32_t id = 0;
      if (handle->GetName() != "default") {
        id = stoul(handle->GetName());
      }
      db_handles.cfs[id] = handle;
    }
    dbs_[db_id] = db_handles;
  }

  void Open(uint32_t db_id, const std::vector<uint32_t>& cf_ids,
            bool reopen = false) {
    if (dbs_.count(db_id) > 0) {
      if (reopen) {
        auto& db_handles = dbs_[db_id];
        auto* db = db_handles.db;
        for (auto& cf : db_handles.cfs) {
          ASSERT_OK(db->DestroyColumnFamilyHandle(cf.second));
        }
        delete db;
        dbs_.erase(db_id);
      } else {
        Destroy(db_id);
      }
    }
    std::vector<ColumnFamilyDescriptor> column_families =
        GenColumnFamilyDescriptors(cf_ids);
    auto path = GenDBPath(db_id);
    DB* db = nullptr;
    if (!reopen) {
      ASSERT_OK(DB::Open(options_, path, &db));
      for (auto& cf : column_families) {
        if (cf.name != "default") {
          ColumnFamilyHandle* cf_handle;
          ASSERT_OK(db->CreateColumnFamily(cf.options, cf.name, &cf_handle));
          ASSERT_OK(db->DestroyColumnFamilyHandle(cf_handle));
        }
      }
      delete db;
      db = nullptr;
    }
    std::vector<ColumnFamilyHandle*> handles;
    ASSERT_OK(DB::Open(options_, path, column_families, &handles, &db));
    AddDB(db_id, db, handles);
  }

  void Destroy(uint32_t db_id) {
    DestroyImpl(dbs_[db_id]);
    dbs_.erase(db_id);
  }

  void DestroyAll() {
    for (auto& db_handles : dbs_) {
      DestroyImpl(db_handles.second);
    }
    dbs_.clear();
  }

  void DestroyImpl(DBHandles& db_handles) {
    auto* db = db_handles.db;
    for (auto& cf : db_handles.cfs) {
      ASSERT_OK(db->DestroyColumnFamilyHandle(cf.second));
    }
    delete db;
    ASSERT_OK(DestroyDB(db_handles.path, options_));
  }

  // cfs are ignored if target already exists
  Status Merge(const MergeInstanceOptions& mopts, std::vector<uint32_t>&& from,
               uint32_t to,
               const std::vector<uint32_t>& cfs = std::vector<uint32_t>()) {
    std::vector<DB*> source_dbs;
    for (auto db_id : from) {
      source_dbs.push_back(get_db(db_id));
    }
    bool newly_opened = false;
    if (dbs_.count(to) == 0) {
      assert(cfs.size() > 0);
      Open(to, cfs);
      newly_opened = true;
    }
    auto s = get_db(to)->MergeDisjointInstances(mopts, source_dbs);
    if (newly_opened && !s.ok()) {
      Destroy(to);
    }
    return s;
  }

  void VerifyKeyValue(uint32_t db_id, uint32_t cf_id, std::string key,
                      std::string value,
                      const ReadOptions& ropts = ReadOptions()) {
    std::string ret;
    if (value == "NotFound") {
      assert(get_db(db_id)
                 ->Get(ropts, get_cf(db_id, cf_id), key, &ret)
                 .IsNotFound());
    } else {
      ASSERT_OK(get_db(db_id)->Get(ropts, get_cf(db_id, cf_id), key, &ret));
      ASSERT_EQ(value, ret);
    }
  }

  int Property(uint32_t db_id, const std::string& name) {
    std::string property;
    int result;
    if (get_db(db_id)->GetProperty(name, &property) &&
        sscanf(property.c_str(), "%d", &result) == 1) {
      return result;
    } else {
      return -1;
    }
  }

  bool has_db(uint32_t db_id) { return dbs_.count(db_id) > 0; }

  DBImpl* get_db(uint32_t db_id) {
    assert(dbs_.count(db_id) == 1);
    return dbs_[db_id].db;
  }

  ColumnFamilyHandle* get_cf(uint32_t db_id, uint32_t cf_id) {
    assert(dbs_.count(db_id) == 1);
    return dbs_[db_id].cfs[cf_id];
  }

  Env* env_ = Env::Default();
  Options options_;
  std::unordered_map<uint32_t, DBHandles> dbs_;
};

TEST_F(DBMergeTest, MultiMerge) {
  FlushOptions fopts;
  fopts.allow_write_stall = true;
  MergeInstanceOptions mopts;
  mopts.merge_memtable = true;
  WriteOptions wopts;
  wopts.disableWAL = true;
  Random rnd(301);

  std::unordered_map<std::string, std::string> kvs[3];
  for (uint32_t i = 0; i < 10; ++i) {
    Open(i, {default_cf, 1_cf, 2_cf});
    auto* db = get_db(i);
    uint32_t keys_per_file = 1 + (i - 5) * (i - 5);  // scatter seqno.
    for (auto cf : {default_cf, 1_cf, 2_cf}) {
      for (uint32_t f = 0; f < 20; ++f) {
        std::string prefix =
            std::to_string(cf) + std::to_string(i) + std::to_string(f);
        for (uint32_t k = 0; k < keys_per_file; ++k) {
          auto keystr = prefix + "-" + std::to_string(k);
          ASSERT_OK(db->Put(wopts, get_cf(i, cf), keystr, keystr));
          kvs[cf][keystr] = keystr;
        }
        ASSERT_OK(db->Flush(fopts, get_cf(i, cf)));
        if (f % 5 == 0) {
          ASSERT_OK(db->CompactRange(CompactRangeOptions(), get_cf(i, cf),
                                     nullptr, nullptr));
        }
      }
    }
  }

  ASSERT_OK(Merge(mopts,
                  {0_db, 1_db, 2_db, 3_db, 4_db, 5_db, 6_db, 7_db, 8_db, 9_db},
                  10_db, {default_cf, 1_cf, 2_cf}));
  ASSERT_OK(Merge(mopts, {0_db, 1_db, 2_db, 3_db, 4_db, 5_db, 6_db, 7_db, 8_db},
                  9_db));

  for (auto cf : {default_cf, 1_cf, 2_cf}) {
    for (auto& kv : kvs[cf]) {
      VerifyKeyValue(9_db, cf, kv.first, kv.second);
      VerifyKeyValue(10_db, cf, kv.first, kv.second);
    }
  }

  // overwrite random to 9 and 10.
  for (auto cf : {default_cf, 1_cf, 2_cf}) {
    for (uint32_t i = 0; i < 10; ++i) {
      auto iter = kvs[cf].begin();
      std::advance(iter, rnd.Next() % kvs[cf].size());

      ASSERT_OK(
          get_db(9_db)->Put(wopts, get_cf(9_db, cf), iter->first, "new_v"));
      ASSERT_OK(
          get_db(10_db)->Put(wopts, get_cf(10_db, cf), iter->first, "new_v"));
      iter->second = "new_v";
    }
    for (auto& kv : kvs[cf]) {
      VerifyKeyValue(9_db, cf, kv.first, kv.second);
      VerifyKeyValue(10_db, cf, kv.first, kv.second);
    }
    ASSERT_OK(get_db(9_db)->Flush(fopts, get_cf(9_db, cf)));
    ASSERT_OK(get_db(10_db)->Flush(fopts, get_cf(10_db, cf)));
    for (auto& kv : kvs[cf]) {
      VerifyKeyValue(9_db, cf, kv.first, kv.second);
      VerifyKeyValue(10_db, cf, kv.first, kv.second);
    }
  }

  // delete old instance.
  for (auto db : {0_db, 1_db, 2_db, 3_db, 4_db, 5_db, 6_db, 7_db, 8_db}) {
    Destroy(db);
  }
  for (auto cf : {default_cf, 1_cf, 2_cf}) {
    for (uint32_t i = 0; i < 10; ++i) {
      auto iter = kvs[cf].begin();
      std::advance(iter, rnd.Next() % kvs[cf].size());

      ASSERT_OK(
          get_db(9_db)->Put(wopts, get_cf(9_db, cf), iter->first, "new_v2"));
      ASSERT_OK(
          get_db(10_db)->Put(wopts, get_cf(10_db, cf), iter->first, "new_v2"));
      iter->second = "new_v2";
    }
    for (auto& kv : kvs[cf]) {
      VerifyKeyValue(9_db, cf, kv.first, kv.second);
      VerifyKeyValue(10_db, cf, kv.first, kv.second);
    }
    ASSERT_OK(get_db(9_db)->Flush(fopts, get_cf(9_db, cf)));
    ASSERT_OK(get_db(10_db)->Flush(fopts, get_cf(10_db, cf)));
    for (auto& kv : kvs[cf]) {
      VerifyKeyValue(9_db, cf, kv.first, kv.second);
      VerifyKeyValue(10_db, cf, kv.first, kv.second);
    }
  }

  Open(9_db, {default_cf, 1_cf, 2_cf}, true /*reopen*/);
  Open(10_db, {default_cf, 1_cf, 2_cf}, true /*reopen*/);
  for (auto cf : {default_cf, 1_cf, 2_cf}) {
    for (auto& kv : kvs[cf]) {
      VerifyKeyValue(9_db, cf, kv.first, kv.second);
      VerifyKeyValue(10_db, cf, kv.first, kv.second);
    }
  }
}

TEST_F(DBMergeTest, BinaryMerge) {
  FlushOptions fopts;
  fopts.allow_write_stall = true;
  MergeInstanceOptions mopts;
  mopts.merge_memtable = true;
  WriteOptions wopts;
  wopts.disableWAL = true;
  Random rnd(301);

  std::unordered_map<std::string, std::string> kvs[3];
  std::vector<uint32_t> dbs = {0_db, 1_db, 2_db, 3_db, 4_db,
                               5_db, 6_db, 7_db, 8_db, 9_db};
  while (dbs.size() >= 2) {
    for (uint32_t i = 0; i < dbs.size(); ++i) {
      if (!has_db(dbs[i])) {
        Open(dbs[i], {default_cf, 1_cf, 2_cf});
      }
      auto* db = get_db(dbs[i]);
      uint32_t keys_per_file = 1 + (i - 5) * (i - 5);  // scatter seqno.
      for (auto cf : {default_cf, 1_cf, 2_cf}) {
        for (uint32_t f = 0; f < 3; ++f) {
          std::string prefix =
              std::to_string(cf) + std::to_string(dbs[i]) + std::to_string(f);
          for (uint32_t k = 0; k < keys_per_file; ++k) {
            auto keystr = prefix + "-" + std::to_string(k);
            if (rnd.Next() % 4 == 0) {
              ASSERT_OK(db->SingleDelete(wopts, get_cf(dbs[i], cf), keystr));
              kvs[cf][keystr] = "NotFound";
            } else {
              auto value = rnd.RandomString(16);
              ASSERT_OK(db->Put(wopts, get_cf(dbs[i], cf), keystr, value));
              kvs[cf][keystr] = value;
            }
          }
          ASSERT_OK(db->Flush(fopts, get_cf(dbs[i], cf)));
        }
      }
    }
    // merge random neighbors.
    uint32_t src = rnd.Next() % dbs.size();
    uint32_t dst = (src + 1) % dbs.size();
    if ((rnd.Next() % 2 == 0 && src > 0) || dst == 0) {
      dst = (src - 1) % dbs.size();
    }
    ASSERT_OK(Merge(mopts, {dbs[src]}, dbs[dst]));
    Destroy(dbs[src]);
    dbs.erase(dbs.begin() + src);
  }
  for (auto cf : {default_cf, 1_cf, 2_cf}) {
    for (auto& kv : kvs[cf]) {
      VerifyKeyValue(dbs[0], cf, kv.first, kv.second);
    }
    ASSERT_OK(get_db(dbs[0])->Flush(fopts, get_cf(dbs[0], cf)));
  }
  Open(dbs[0], {default_cf, 1_cf, 2_cf}, true /*reopen*/);
  for (auto cf : {default_cf, 1_cf, 2_cf}) {
    for (auto& kv : kvs[cf]) {
      VerifyKeyValue(dbs[0], cf, kv.first, kv.second);
    }
  }
}

TEST_F(DBMergeTest, KeyOverlappedInstance) {
  FlushOptions fopts;
  fopts.allow_write_stall = true;
  MergeInstanceOptions mopts;
  mopts.merge_memtable = false;
  WriteOptions wopts;
  wopts.disableWAL = true;
  CompactRangeOptions copts;
  copts.bottommost_level_compaction = BottommostLevelCompaction::kForce;

  Open(1_db, {default_cf, 1_cf});
  Open(2_db, {1_cf, default_cf});
  ASSERT_OK(get_db(1_db)->Put(wopts, get_cf(1_db, 1_cf), "1", "v1"));
  ASSERT_OK(get_db(2_db)->Put(wopts, get_cf(2_db, 1_cf), "0", "v0"));

  ASSERT_OK(Merge(mopts, {1_db, 2_db}, 3_db, {default_cf, 1_cf}));
  Destroy(3_db);

  ASSERT_OK(get_db(2_db)->Put(wopts, get_cf(2_db, 1_cf), "3", "v3"));
  IsOverlapError(Merge(mopts, {1_db, 2_db}, 3_db, {default_cf, 1_cf}));
  IsOverlapError(Merge(mopts, {1_db}, 2_db, {default_cf, 1_cf}));

  // Skip overlapped cf.
  ASSERT_OK(Merge(mopts, {1_db, 2_db}, 3_db, {default_cf}));
  Destroy(3_db);

  // Only flush one.
  ASSERT_OK(get_db(2_db)->Flush(fopts, get_cf(2_db, 1_cf)));
  IsOverlapError(Merge(mopts, {1_db, 2_db}, 3_db, {default_cf, 1_cf}));
  IsOverlapError(Merge(mopts, {1_db}, 2_db, {default_cf, 1_cf}));

  // Both flushed.
  ASSERT_OK(get_db(1_db)->Flush(fopts, get_cf(1_db, 1_cf)));
  IsOverlapError(Merge(mopts, {1_db, 2_db}, 3_db, {default_cf, 1_cf}));
  IsOverlapError(Merge(mopts, {1_db}, 2_db, {default_cf, 1_cf}));

  // Delete in memory.
  ASSERT_OK(get_db(1_db)->SingleDelete(wopts, get_cf(1_db, 1_cf), "1"));
  IsOverlapError(Merge(mopts, {1_db, 2_db}, 3_db, {default_cf, 1_cf}));
  IsOverlapError(Merge(mopts, {1_db}, 2_db, {default_cf, 1_cf}));

  ASSERT_OK(get_db(1_db)->Flush(fopts, get_cf(1_db, 1_cf)));
  IsOverlapError(Merge(mopts, {1_db, 2_db}, 3_db, {default_cf, 1_cf}));
  IsOverlapError(Merge(mopts, {1_db}, 2_db, {default_cf, 1_cf}));

  ASSERT_OK(
      get_db(1_db)->CompactRange(copts, get_cf(1_db, 1_cf), nullptr, nullptr));
  ASSERT_OK(Merge(mopts, {1_db, 2_db}, 3_db, {default_cf, 1_cf}));

  VerifyKeyValue(3_db, 1_cf, "0", "v0");
  VerifyKeyValue(3_db, 1_cf, "3", "v3");
  VerifyKeyValue(3_db, 1_cf, "1", "NotFound");
}

TEST_F(DBMergeTest, TombstoneOverlappedInstance) {
  WriteOptions wopts;
  wopts.disableWAL = true;
  MergeInstanceOptions mopts;
  mopts.merge_memtable = false;
  CompactRangeOptions copts;
  copts.bottommost_level_compaction = BottommostLevelCompaction::kForce;

  Open(1_db, {default_cf, 1_cf});
  Open(2_db, {default_cf, 1_cf});
  Open(3_db, {default_cf, 1_cf});
  Open(4_db, {default_cf, 1_cf});
  ASSERT_OK(get_db(1_db)->Put(wopts, get_cf(1_db, 1_cf), "1", "v1"));
  ASSERT_OK(get_db(2_db)->Put(wopts, get_cf(2_db, 1_cf), "2", "v2"));
  ASSERT_OK(get_db(3_db)->Put(wopts, get_cf(3_db, 1_cf), "3", "v3"));
  ASSERT_OK(get_db(4_db)->Put(wopts, get_cf(4_db, 1_cf), "4", "v4"));

  ASSERT_OK(Merge(mopts, {1_db, 2_db, 3_db, 4_db}, 0_db, {default_cf, 1_cf}));
  Destroy(0_db);

  // Lower bound overlap.
  ASSERT_OK(get_db(2_db)->DeleteRange(wopts, get_cf(2_db, 1_cf), "0", "9"));
  ASSERT_OK(get_db(2_db)->Put(wopts, get_cf(2_db, 1_cf), "2", "v2"));
  IsOverlapError(Merge(mopts, {1_db, 2_db}, 0_db, {default_cf, 1_cf}));

  // Upper bound overlap.
  ASSERT_OK(get_db(3_db)->DeleteRange(wopts, get_cf(3_db, 1_cf), "0", "9"));
  ASSERT_OK(get_db(3_db)->Put(wopts, get_cf(3_db, 1_cf), "3", "v3"));
  IsOverlapError(Merge(mopts, {3_db, 4_db}, 0_db, {default_cf, 1_cf}));

  // nullptr is an empty key.
  ASSERT_OK(get_db(4_db)->SingleDelete(wopts, get_cf(4_db, 1_cf), nullptr));
  IsOverlapError(Merge(mopts, {1_db, 4_db}, 0_db, {default_cf, 1_cf}));

  Slice start = "0";
  Slice end = "2";
  ASSERT_OK(
      get_db(2_db)->CompactRange(copts, get_cf(2_db, 1_cf), &start, &end));
  start = "22";
  end = "99";
  ASSERT_OK(
      get_db(2_db)->CompactRange(copts, get_cf(2_db, 1_cf), &start, &end));

  start = "0";
  end = "3";
  ASSERT_OK(
      get_db(3_db)->CompactRange(copts, get_cf(3_db, 1_cf), &start, &end));
  start = "33";
  end = "99";
  ASSERT_OK(
      get_db(3_db)->CompactRange(copts, get_cf(3_db, 1_cf), &start, &end));

  end = "4";
  ASSERT_OK(
      get_db(4_db)->CompactRange(copts, get_cf(4_db, 1_cf), nullptr, &end));

  mopts.merge_memtable = true;
  ASSERT_OK(Merge(mopts, {1_db, 2_db, 3_db, 4_db}, 0_db, {default_cf, 1_cf}));

  VerifyKeyValue(0_db, 1_cf, "1", "v1");
  VerifyKeyValue(0_db, 1_cf, "2", "v2");
  VerifyKeyValue(0_db, 1_cf, "3", "v3");
  VerifyKeyValue(0_db, 1_cf, "4", "v4");
}

TEST_F(DBMergeTest, WithWAL) {
  WriteOptions wopts;
  wopts.disableWAL = false;
  MergeInstanceOptions mopts;
  FlushOptions fopts;
  fopts.allow_write_stall = true;

  Open(1_db, {default_cf, 1_cf});
  Open(2_db, {default_cf, 1_cf});
  ASSERT_OK(get_db(1_db)->Put(wopts, get_cf(1_db, 1_cf), "1", "v1"));
  ASSERT_OK(get_db(2_db)->Put(wopts, get_cf(2_db, 1_cf), "2", "v2"));

  // Ignore WAL and memtable.
  mopts.merge_memtable = false;
  ASSERT_OK(Merge(mopts, {1_db}, 2_db));
  VerifyKeyValue(2_db, 1_cf, "2", "v2");
  VerifyKeyValue(2_db, 1_cf, "1", "NotFound");

  mopts.merge_memtable = true;
  IsWALNotEmpty(Merge(mopts, {1_db, 2_db}, 3_db, {default_cf, 1_cf}));

  for (auto db : {1_db, 2_db}) {
    ASSERT_OK(get_db(db)->Flush(fopts, get_cf(db, 1_cf)));
  }
  ASSERT_OK(Merge(mopts, {1_db, 2_db}, 3_db, {default_cf, 1_cf}));
}

TEST_F(DBMergeTest, MemtableIsolation) {
  WriteOptions wopts;
  wopts.disableWAL = true;
  MergeInstanceOptions mopts;
  mopts.merge_memtable = true;

  Open(1_db, {default_cf});
  Open(2_db, {default_cf});
  ASSERT_OK(get_db(1_db)->Put(wopts, get_cf(1_db, default_cf), "1", "v1"));
  ASSERT_OK(Merge(mopts, {1_db}, 2_db, {default_cf}));
  VerifyKeyValue(2_db, default_cf, "1", "v1");
  ASSERT_OK(get_db(1_db)->Put(wopts, get_cf(1_db, default_cf), "1", "v2"));
  // Increase the seqno of 2_db so that snapshot might include new writes.
  ASSERT_OK(get_db(2_db)->Put(wopts, get_cf(2_db, default_cf), "2", "v"));
  ASSERT_OK(get_db(2_db)->Put(wopts, get_cf(2_db, default_cf), "2", "v"));
  // Check merged DB is not affected by source DB writes.
  VerifyKeyValue(2_db, default_cf, "1", "v1");
}

TEST_F(DBMergeTest, CacheReuse) {
  BlockBasedTableOptions table_options;
  // Otherwise the reader will not attempt to read cache first.
  table_options.cache_index_and_filter_blocks = true;
  options_.table_factory.reset(NewBlockBasedTableFactory(table_options));
  WriteOptions wopts;
  wopts.disableWAL = true;
  ReadOptions ropts;
  ropts.fill_cache = true;
  MergeInstanceOptions mopts;
  mopts.merge_memtable = true;

  Open(1_db, {default_cf});
  Open(2_db, {default_cf});
  ASSERT_OK(get_db(1_db)->Put(wopts, get_cf(1_db, default_cf), "1", "v1"));
  ASSERT_OK(get_db(2_db)->Put(wopts, get_cf(2_db, default_cf), "2", "v1"));
  for (auto db : {1_db, 2_db}) {
    ASSERT_OK(get_db(db)->Flush(FlushOptions(), get_cf(db, default_cf)));
  }
  VerifyKeyValue(1_db, default_cf, "1", "v1", ropts);
  VerifyKeyValue(2_db, default_cf, "2", "v1", ropts);
  ropts.read_tier = ReadTier::kBlockCacheTier;
  VerifyKeyValue(1_db, default_cf, "1", "v1", ropts);
  VerifyKeyValue(2_db, default_cf, "2", "v1", ropts);

  ASSERT_OK(Merge(mopts, {1_db, 2_db}, 3_db, {default_cf}));

  ropts.read_tier = ReadTier::kBlockCacheTier;
  VerifyKeyValue(3_db, default_cf, "1", "v1", ropts);
  VerifyKeyValue(3_db, default_cf, "2", "v1", ropts);
}

TEST_F(DBMergeTest, ConcurrentFlush) {
  WriteOptions wopts;
  wopts.disableWAL = true;
  MergeInstanceOptions mopts;
  mopts.merge_memtable = true;

  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::MergeDisjointInstances:AfterMergeMemtable:1",
      [&](void* /*arg*/) {
        for (auto db : {1_db, 2_db}) {
          ASSERT_OK(get_db(db)->Flush(FlushOptions(), get_cf(db, default_cf)));
        }
      });
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->EnableProcessing();

  Open(1_db, {default_cf});
  Open(2_db, {default_cf});
  Open(3_db, {default_cf});
  ASSERT_OK(get_db(3_db)->PauseBackgroundWork());

  // Put some to memtable.
  ASSERT_OK(get_db(1_db)->Put(wopts, get_cf(1_db, default_cf), "1", "v1"));
  ASSERT_OK(get_db(2_db)->Put(wopts, get_cf(2_db, default_cf), "2", "v1"));
  ASSERT_EQ(Property(1_db, "rocksdb.num-files-at-level0"), 0);
  ASSERT_EQ(Property(2_db, "rocksdb.num-files-at-level0"), 0);

  ASSERT_OK(Merge(mopts, {1_db, 2_db}, 3_db, {default_cf}));
  ASSERT_EQ(Property(1_db, "rocksdb.num-files-at-level0"), 1);
  ASSERT_EQ(Property(2_db, "rocksdb.num-files-at-level0"), 1);
  ASSERT_EQ(Property(3_db, "rocksdb.num-files-at-level0"), 0);

  VerifyKeyValue(3_db, default_cf, "1", "v1");
  VerifyKeyValue(3_db, default_cf, "2", "v1");

  ASSERT_OK(get_db(3_db)->ContinueBackgroundWork());
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->DisableProcessing();
  ROCKSDB_NAMESPACE::SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(DBMergeTest, MissingCF) {
  WriteOptions wopts;
  wopts.disableWAL = true;
  MergeInstanceOptions mopts;
  mopts.merge_memtable = true;

  Open(1_db, {default_cf, 1_cf});
  Open(2_db, {default_cf, 2_cf});
  Open(3_db, {default_cf, 3_cf});
  ASSERT_OK(get_db(1_db)->Put(wopts, get_cf(1_db, 1_cf), "key", "v1"));
  ASSERT_OK(get_db(2_db)->Put(wopts, get_cf(2_db, 2_cf), "key", "v2"));
  ASSERT_OK(get_db(3_db)->Put(wopts, get_cf(3_db, 3_cf), "key", "v3"));

  ASSERT_OK(
      Merge(mopts, {1_db, 2_db, 3_db}, 4_db, {default_cf, 1_cf, 2_cf, 3_cf}));

  VerifyKeyValue(4_db, 1_cf, "key", "v1");
  VerifyKeyValue(4_db, 2_cf, "key", "v2");
  VerifyKeyValue(4_db, 3_cf, "key", "v3");
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  RegisterCustomObjects(argc, argv);
  return RUN_ALL_TESTS();
}
