//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstring>
#include <string>
#include "db/version_edit.h"
#include "db/version_set.h"
#include "logging/logging.h"
#include "test_util/testharness.h"
#include "test_util/testutil.h"
#include "util/string_util.h"

namespace rocksdb {

class VersionBuilderTest : public testing::Test {
 public:
  const Comparator* ucmp_;
  InternalKeyComparator icmp_;
  Options options_;
  ImmutableCFOptions ioptions_;
  MutableCFOptions mutable_cf_options_;
  VersionStorageInfo vstorage_;
  uint32_t file_num_;
  CompactionOptionsFIFO fifo_options_;
  std::vector<uint64_t> size_being_compacted_;

  VersionBuilderTest()
      : ucmp_(BytewiseComparator()),
        icmp_(ucmp_),
        ioptions_(options_),
        mutable_cf_options_(options_),
        vstorage_(&icmp_, ucmp_, options_.num_levels, kCompactionStyleLevel,
                  nullptr, false),
        file_num_(1) {
    mutable_cf_options_.RefreshDerivedOptions(ioptions_);
    size_being_compacted_.resize(options_.num_levels);
  }

  ~VersionBuilderTest() override {
    for (int i = 0; i < vstorage_.num_levels(); i++) {
      for (auto* f : vstorage_.LevelFiles(i)) {
        if (f->Unref()) {
          delete f;
        }
      }
    }
  }

  InternalKey GetInternalKey(const char* ukey,
                             SequenceNumber smallest_seq = 100) {
    return InternalKey(ukey, smallest_seq, kTypeValue);
  }

  void Add(int level, uint64_t file_number, const char* smallest,
           const char* largest, uint64_t file_size = 0, uint32_t path_id = 0,
           SequenceNumber smallest_seq = 100, SequenceNumber largest_seq = 100,
           uint64_t num_entries = 0, uint64_t num_deletions = 0,
           bool sampled = false, SequenceNumber smallest_seqno = 0,
           SequenceNumber largest_seqno = 0) {
    assert(level < vstorage_.num_levels());
    FileMetaData* f = new FileMetaData;
    f->fd = FileDescriptor(file_number, path_id, file_size);
    f->smallest = GetInternalKey(smallest, smallest_seq);
    f->largest = GetInternalKey(largest, largest_seq);
    f->fd.smallest_seqno = smallest_seqno;
    f->fd.largest_seqno = largest_seqno;
    f->compensated_file_size = file_size;
    f->num_entries = num_entries;
    f->num_deletions = num_deletions;
    vstorage_.AddFile(level, f);
    if (sampled) {
      f->init_stats_from_file = true;
      vstorage_.UpdateAccumulatedStats(f);
    }
  }

  void UpdateVersionStorageInfo() {
    vstorage_.UpdateFilesByCompactionPri(ioptions_.compaction_pri);
    vstorage_.UpdateNumNonEmptyLevels();
    vstorage_.GenerateFileIndexer();
    vstorage_.GenerateLevelFilesBrief();
    vstorage_.CalculateBaseBytes(ioptions_, mutable_cf_options_);
    vstorage_.GenerateLevel0NonOverlapping();
    vstorage_.SetFinalized();
  }
};

// Check that one file number is mapped to one unique FileMetaData in a series
// of versions.
struct FileReferenceChecker {
  std::unordered_map<uint64_t, FileMetaData*> files;

  bool Check(const VersionStorageInfo* vstorage) {
    for (int i = 0; i < vstorage->num_levels(); i++) {
      for (auto* f : vstorage->LevelFiles(i)) {
        auto it = files.find(f->fd.GetNumber());
        if (it == files.end()) {
          files[f->fd.GetNumber()] = f;
        } else if (it->second != f) {
          return false;
        }
      }
    }
    return true;
  }
};

void UnrefFilesInVersion(VersionStorageInfo* new_vstorage) {
  for (int i = 0; i < new_vstorage->num_levels(); i++) {
    for (auto* f : new_vstorage->LevelFiles(i)) {
      if (f->Unref()) {
        delete f;
      }
    }
  }
}

TEST_F(VersionBuilderTest, ApplyAndSaveTo) {
  Add(0, 1U, "150", "200", 100U);

  Add(1, 66U, "150", "200", 100U);
  Add(1, 88U, "201", "300", 100U);

  Add(2, 6U, "150", "179", 100U);
  Add(2, 7U, "180", "220", 100U);
  Add(2, 8U, "221", "300", 100U);

  Add(3, 26U, "150", "170", 100U);
  Add(3, 27U, "171", "179", 100U);
  Add(3, 28U, "191", "220", 100U);
  Add(3, 29U, "221", "300", 100U);
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  version_edit.AddFile(2, 666, 0, 100U, GetInternalKey("301"),
                       GetInternalKey("350"), 200, 200, false);
  version_edit.DeleteFile(3, 27U);

  EnvOptions env_options;

  VersionBuilder version_builder(env_options, nullptr, &vstorage_);

  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, nullptr, false);
  version_builder.Apply(&version_edit);
  version_builder.SaveTo(&new_vstorage);

  ASSERT_EQ(400U, new_vstorage.NumLevelBytes(2));
  ASSERT_EQ(300U, new_vstorage.NumLevelBytes(3));

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, ApplyAndSaveToDynamic) {
  ioptions_.level_compaction_dynamic_level_bytes = true;

  Add(0, 1U, "150", "200", 100U, 0, 200U, 200U, 0, 0, false, 200U, 200U);
  Add(0, 88U, "201", "300", 100U, 0, 100U, 100U, 0, 0, false, 100U, 100U);

  Add(4, 6U, "150", "179", 100U);
  Add(4, 7U, "180", "220", 100U);
  Add(4, 8U, "221", "300", 100U);

  Add(5, 26U, "150", "170", 100U);
  Add(5, 27U, "171", "179", 100U);
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  version_edit.AddFile(3, 666, 0, 100U, GetInternalKey("301"),
                       GetInternalKey("350"), 200, 200, false);
  version_edit.DeleteFile(0, 1U);
  version_edit.DeleteFile(0, 88U);

  EnvOptions env_options;

  VersionBuilder version_builder(env_options, nullptr, &vstorage_);

  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, nullptr, false);
  version_builder.Apply(&version_edit);
  version_builder.SaveTo(&new_vstorage);

  ASSERT_EQ(0U, new_vstorage.NumLevelBytes(0));
  ASSERT_EQ(100U, new_vstorage.NumLevelBytes(3));
  ASSERT_EQ(300U, new_vstorage.NumLevelBytes(4));
  ASSERT_EQ(200U, new_vstorage.NumLevelBytes(5));

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, ApplyAndSaveToDynamic2) {
  ioptions_.level_compaction_dynamic_level_bytes = true;

  Add(0, 1U, "150", "200", 100U, 0, 200U, 200U, 0, 0, false, 200U, 200U);
  Add(0, 88U, "201", "300", 100U, 0, 100U, 100U, 0, 0, false, 100U, 100U);

  Add(4, 6U, "150", "179", 100U);
  Add(4, 7U, "180", "220", 100U);
  Add(4, 8U, "221", "300", 100U);

  Add(5, 26U, "150", "170", 100U);
  Add(5, 27U, "171", "179", 100U);
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  version_edit.AddFile(4, 666, 0, 100U, GetInternalKey("301"),
                       GetInternalKey("350"), 200, 200, false);
  version_edit.DeleteFile(0, 1U);
  version_edit.DeleteFile(0, 88U);
  version_edit.DeleteFile(4, 6U);
  version_edit.DeleteFile(4, 7U);
  version_edit.DeleteFile(4, 8U);

  EnvOptions env_options;

  VersionBuilder version_builder(env_options, nullptr, &vstorage_);

  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, nullptr, false);
  version_builder.Apply(&version_edit);
  version_builder.SaveTo(&new_vstorage);

  ASSERT_EQ(0U, new_vstorage.NumLevelBytes(0));
  ASSERT_EQ(100U, new_vstorage.NumLevelBytes(4));
  ASSERT_EQ(200U, new_vstorage.NumLevelBytes(5));

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, ApplyMultipleAndSaveTo) {
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  version_edit.AddFile(2, 666, 0, 100U, GetInternalKey("301"),
                       GetInternalKey("350"), 200, 200, false);
  version_edit.AddFile(2, 676, 0, 100U, GetInternalKey("401"),
                       GetInternalKey("450"), 200, 200, false);
  version_edit.AddFile(2, 636, 0, 100U, GetInternalKey("601"),
                       GetInternalKey("650"), 200, 200, false);
  version_edit.AddFile(2, 616, 0, 100U, GetInternalKey("501"),
                       GetInternalKey("550"), 200, 200, false);
  version_edit.AddFile(2, 606, 0, 100U, GetInternalKey("701"),
                       GetInternalKey("750"), 200, 200, false);

  EnvOptions env_options;

  VersionBuilder version_builder(env_options, nullptr, &vstorage_);

  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, nullptr, false);
  version_builder.Apply(&version_edit);
  version_builder.SaveTo(&new_vstorage);

  ASSERT_EQ(500U, new_vstorage.NumLevelBytes(2));

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, ApplyDeleteAndSaveTo) {
  UpdateVersionStorageInfo();

  EnvOptions env_options;
  VersionBuilder version_builder(env_options, nullptr, &vstorage_);
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, nullptr, false);

  VersionEdit version_edit;
  version_edit.AddFile(2, 666, 0, 100U, GetInternalKey("301"),
                       GetInternalKey("350"), 200, 200, false);
  version_edit.AddFile(2, 676, 0, 100U, GetInternalKey("401"),
                       GetInternalKey("450"), 200, 200, false);
  version_edit.AddFile(2, 636, 0, 100U, GetInternalKey("601"),
                       GetInternalKey("650"), 200, 200, false);
  version_edit.AddFile(2, 616, 0, 100U, GetInternalKey("501"),
                       GetInternalKey("550"), 200, 200, false);
  version_edit.AddFile(2, 606, 0, 100U, GetInternalKey("701"),
                       GetInternalKey("750"), 200, 200, false);
  version_builder.Apply(&version_edit);

  VersionEdit version_edit2;
  version_edit.AddFile(2, 808, 0, 100U, GetInternalKey("901"),
                       GetInternalKey("950"), 200, 200, false);
  version_edit2.DeleteFile(2, 616);
  version_edit2.DeleteFile(2, 636);
  version_edit.AddFile(2, 806, 0, 100U, GetInternalKey("801"),
                       GetInternalKey("850"), 200, 200, false);
  version_builder.Apply(&version_edit2);

  version_builder.SaveTo(&new_vstorage);

  ASSERT_EQ(300U, new_vstorage.NumLevelBytes(2));

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, ApplyFileDeletionIncorrectLevel) {
  constexpr int level = 1;
  constexpr uint64_t file_number = 2345;
  constexpr char smallest[] = "bar";
  constexpr char largest[] = "foo";

  Add(level, file_number, smallest, largest);

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;

  VersionBuilder builder(env_options, table_cache, &vstorage_);

  VersionEdit edit;

  constexpr int incorrect_level = 3;

  edit.DeleteFile(incorrect_level, file_number);

  const Status s = builder.Apply(&edit);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(s.getState(),
                          "Cannot delete table file #2345 from level 3 since "
                          "it is on level 1"));
}

TEST_F(VersionBuilderTest, ApplyFileDeletionNotInLSMTree) {
  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;

  VersionBuilder builder(env_options, table_cache, &vstorage_);

  VersionEdit edit;

  constexpr int level = 3;
  constexpr uint64_t file_number = 1234;

  edit.DeleteFile(level, file_number);

  const Status s = builder.Apply(&edit);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(s.getState(),
                          "Cannot delete table file #1234 from level 3 since "
                          "it is not in the LSM tree"));
}

TEST_F(VersionBuilderTest, ApplyFileDeletionAndAddition) {
  constexpr int level = 1;
  constexpr uint64_t file_number = 2345;
  constexpr uint64_t path_id = 0;
  constexpr uint64_t file_size = 1024;
  constexpr char smallest[] = "bar";
  constexpr char largest[] = "foo";
  constexpr SequenceNumber smallest_seq = 100;
  constexpr SequenceNumber largest_seq = 100;
  constexpr uint64_t num_entries = 1000;
  constexpr uint64_t num_deletions = 0;
  constexpr bool sampled = true;
  constexpr SequenceNumber smallest_seqno = 1;
  constexpr SequenceNumber largest_seqno = 1000;
  constexpr bool marked_for_compaction = false;
  constexpr bool force_consistency_checks = false;

  Add(level, file_number, smallest, largest, file_size, path_id, smallest_seq,
      largest_seq, num_entries, num_deletions, sampled, smallest_seqno,
      largest_seqno);

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;

  VersionEdit deletion;
  deletion.DeleteFile(level, file_number);

  {
    VersionBuilder builder(env_options, table_cache, &vstorage_);
    ASSERT_OK(builder.Apply(&deletion));
    VersionEdit addition;
    addition.AddFile(level, file_number, path_id, file_size,
                     GetInternalKey("181", smallest_seq),
                     GetInternalKey("798", largest_seq), smallest_seqno,
                     largest_seqno, marked_for_compaction);
    ASSERT_OK(builder.Apply(&addition));
    VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                    kCompactionStyleLevel, &vstorage_,
                                    force_consistency_checks);
    ASSERT_OK(builder.SaveTo(&new_vstorage));
    ASSERT_EQ(new_vstorage.GetFileLocation(file_number).GetLevel(), level);
    FileReferenceChecker checker;
    ASSERT_TRUE(checker.Check(&vstorage_));
    ASSERT_TRUE(checker.Check(&new_vstorage));
    UnrefFilesInVersion(&new_vstorage);
  }

  {  // Move to a higher level.
    VersionBuilder builder(env_options, table_cache, &vstorage_);
    ASSERT_OK(builder.Apply(&deletion));
    VersionEdit addition;
    addition.AddFile(level + 1, file_number, path_id, file_size,
                     GetInternalKey("181", smallest_seq),
                     GetInternalKey("798", largest_seq), smallest_seqno,
                     largest_seqno, marked_for_compaction);
    ASSERT_OK(builder.Apply(&addition));
    VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                    kCompactionStyleLevel, &vstorage_,
                                    force_consistency_checks);
    ASSERT_OK(builder.SaveTo(&new_vstorage));
    ASSERT_EQ(new_vstorage.GetFileLocation(file_number).GetLevel(), level + 1);
    // File movement should not change key estimation.
    ASSERT_EQ(vstorage_.GetEstimatedActiveKeys(),
              new_vstorage.GetEstimatedActiveKeys());
    FileReferenceChecker checker;
    ASSERT_TRUE(checker.Check(&vstorage_));
    ASSERT_TRUE(checker.Check(&new_vstorage));
    UnrefFilesInVersion(&new_vstorage);
  }

  {  // Move to a different path.
    VersionBuilder builder(env_options, table_cache, &vstorage_);
    ASSERT_OK(builder.Apply(&deletion));
    VersionEdit addition;
    addition.AddFile(level, file_number, path_id + 1, file_size,
                     GetInternalKey("181", smallest_seq),
                     GetInternalKey("798", largest_seq), smallest_seqno,
                     largest_seqno, marked_for_compaction);
    const Status s = builder.Apply(&addition);
    ASSERT_TRUE(s.IsCorruption());
    ASSERT_TRUE(
        std::strstr(s.getState(),
                    "Cannot add table file #2345 to level 1 by trivial "
                    "move since it isn't trivial to move to a different "
                    "path"));
  }

  {  // Move twice.
    VersionBuilder builder(env_options, table_cache, &vstorage_);
    ASSERT_OK(builder.Apply(&deletion));
    VersionEdit addition_1;
    addition_1.AddFile(level + 1, file_number, path_id, file_size,
                       GetInternalKey("181", smallest_seq),
                       GetInternalKey("798", largest_seq), smallest_seqno,
                       largest_seqno, marked_for_compaction);
    VersionEdit deletion_1;
    deletion_1.DeleteFile(level + 1, file_number);
    VersionEdit addition_2;
    addition_2.AddFile(level + 2, file_number, path_id, file_size,
                       GetInternalKey("181", smallest_seq),
                       GetInternalKey("798", largest_seq), smallest_seqno,
                       largest_seqno, marked_for_compaction);
    ASSERT_OK(builder.Apply(&addition_1));
    ASSERT_OK(builder.Apply(&deletion_1));
    ASSERT_OK(builder.Apply(&addition_2));
    VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                    kCompactionStyleLevel, &vstorage_,
                                    force_consistency_checks);
    ASSERT_OK(builder.SaveTo(&new_vstorage));
    ASSERT_EQ(new_vstorage.GetFileLocation(file_number).GetLevel(), level + 2);
    // File movement should not change key estimation.
    ASSERT_EQ(vstorage_.GetEstimatedActiveKeys(),
              new_vstorage.GetEstimatedActiveKeys());
    FileReferenceChecker checker;
    ASSERT_TRUE(checker.Check(&vstorage_));
    ASSERT_TRUE(checker.Check(&new_vstorage));
    UnrefFilesInVersion(&new_vstorage);
  }
}

TEST_F(VersionBuilderTest, ApplyFileAdditionAlreadyInBase) {
  constexpr int level = 1;
  constexpr uint64_t file_number = 2345;
  constexpr char smallest[] = "bar";
  constexpr char largest[] = "foo";

  Add(level, file_number, smallest, largest);

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;

  VersionBuilder builder(env_options, table_cache, &vstorage_);

  VersionEdit edit;

  constexpr int new_level = 2;
  constexpr uint32_t path_id = 0;
  constexpr uint64_t file_size = 10000;
  constexpr SequenceNumber smallest_seqno = 100;
  constexpr SequenceNumber largest_seqno = 1000;
  constexpr bool marked_for_compaction = false;

  // Add an existing file.
  edit.AddFile(new_level, file_number, path_id, file_size,
               GetInternalKey(smallest), GetInternalKey(largest),
               smallest_seqno, largest_seqno, marked_for_compaction);

  const Status s = builder.Apply(&edit);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(s.getState(),
                          "Cannot add table file #2345 to level 2 since it is "
                          "already in the LSM tree on level 1"));
}

TEST_F(VersionBuilderTest, ApplyFileAdditionAlreadyApplied) {
  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;

  VersionBuilder builder(env_options, table_cache, &vstorage_);

  VersionEdit edit;

  constexpr int level = 3;
  constexpr uint64_t file_number = 2345;
  constexpr uint32_t path_id = 0;
  constexpr uint64_t file_size = 10000;
  constexpr char smallest[] = "bar";
  constexpr char largest[] = "foo";
  constexpr SequenceNumber smallest_seqno = 100;
  constexpr SequenceNumber largest_seqno = 1000;
  constexpr bool marked_for_compaction = false;

  edit.AddFile(level, file_number, path_id, file_size, GetInternalKey(smallest),
               GetInternalKey(largest), smallest_seqno, largest_seqno,
               marked_for_compaction);

  ASSERT_OK(builder.Apply(&edit));

  VersionEdit other_edit;

  constexpr int new_level = 2;

  other_edit.AddFile(new_level, file_number, path_id, file_size,
                     GetInternalKey(smallest), GetInternalKey(largest),
                     smallest_seqno, largest_seqno, marked_for_compaction);

  const Status s = builder.Apply(&other_edit);
  ASSERT_TRUE(s.IsCorruption());
  ASSERT_TRUE(std::strstr(s.getState(),
                          "Cannot add table file #2345 to level 2 since it is "
                          "already in the LSM tree on level 3"));
}

TEST_F(VersionBuilderTest, ApplyFileAdditionAndDeletion) {
  constexpr int level = 1;
  constexpr uint64_t file_number = 2345;
  constexpr uint32_t path_id = 0;
  constexpr uint64_t file_size = 10000;
  constexpr char smallest[] = "bar";
  constexpr char largest[] = "foo";
  constexpr SequenceNumber smallest_seqno = 100;
  constexpr SequenceNumber largest_seqno = 1000;
  constexpr bool marked_for_compaction = false;

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;

  VersionBuilder builder(env_options, table_cache, &vstorage_);

  VersionEdit addition;

  addition.AddFile(level, file_number, path_id, file_size,
                   GetInternalKey(smallest), GetInternalKey(largest),
                   smallest_seqno, largest_seqno, marked_for_compaction);

  ASSERT_OK(builder.Apply(&addition));

  VersionEdit deletion;

  deletion.DeleteFile(level, file_number);

  ASSERT_OK(builder.Apply(&deletion));

  constexpr bool force_consistency_checks = false;
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, &vstorage_,
                                  force_consistency_checks);

  ASSERT_OK(builder.SaveTo(&new_vstorage));
  ASSERT_FALSE(new_vstorage.GetFileLocation(file_number).IsValid());

  UnrefFilesInVersion(&new_vstorage);
}

TEST_F(VersionBuilderTest, CheckConsistencyForFileDeletedTwice) {
  Add(0, 1U, "150", "200", 100U);
  UpdateVersionStorageInfo();

  VersionEdit version_edit;
  version_edit.DeleteFile(0, 1U);

  EnvOptions env_options;
  constexpr TableCache* table_cache = nullptr;

  VersionBuilder version_builder(env_options, table_cache, &vstorage_);
  VersionStorageInfo new_vstorage(&icmp_, ucmp_, options_.num_levels,
                                  kCompactionStyleLevel, nullptr,
                                  true /* force_consistency_checks */);
  ASSERT_OK(version_builder.Apply(&version_edit));
  ASSERT_OK(version_builder.SaveTo(&new_vstorage));

  VersionBuilder version_builder2(env_options, table_cache, &new_vstorage);
  VersionStorageInfo new_vstorage2(&icmp_, ucmp_, options_.num_levels,
                                   kCompactionStyleLevel, nullptr,
                                   true /* force_consistency_checks */);
  ASSERT_NOK(version_builder2.Apply(&version_edit));

  UnrefFilesInVersion(&new_vstorage);
  UnrefFilesInVersion(&new_vstorage2);
}

TEST_F(VersionBuilderTest, EstimatedActiveKeys) {
  const uint32_t kTotalSamples = 20;
  const uint32_t kNumLevels = 5;
  const uint32_t kFilesPerLevel = 8;
  const uint32_t kNumFiles = kNumLevels * kFilesPerLevel;
  const uint32_t kEntriesPerFile = 1000;
  const uint32_t kDeletionsPerFile = 100;
  for (uint32_t i = 0; i < kNumFiles; ++i) {
    Add(static_cast<int>(i / kFilesPerLevel), i + 1,
        ToString((i + 100) * 1000).c_str(),
        ToString((i + 100) * 1000 + 999).c_str(),
        100U,  0, 100, 100,
        kEntriesPerFile, kDeletionsPerFile,
        (i < kTotalSamples));
  }
  // minus 2X for the number of deletion entries because:
  // 1x for deletion entry does not count as a data entry.
  // 1x for each deletion entry will actually remove one data entry.
  ASSERT_EQ(vstorage_.GetEstimatedActiveKeys(),
            (kEntriesPerFile - 2 * kDeletionsPerFile) * kNumFiles);
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
