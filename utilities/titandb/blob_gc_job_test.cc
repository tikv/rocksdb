//
// Created by 郑志铨 on 2018/8/27.
//

#include "utilities/titandb/blob_gc_picker.h"
#include "util/filename.h"
#include "util/testharness.h"
#include "utilities/titandb/blob_file_builder.h"
#include "utilities/titandb/blob_file_cache.h"
#include "utilities/titandb/blob_file_iterator.h"
#include "utilities/titandb/blob_file_reader.h"
#include "utilities/titandb/version.h"
#include "utilities/titandb/titan_db.h"
#include "utilities/titandb/titan_db_impl.h"
#include "utilities/titandb/blob_gc_job.h"

namespace rocksdb {
namespace titandb {

const static int MAX_KEY_NUM = 1000;

class BlobGCJobTest : public testing::Test {
 public:
  TitanDB* db_;
  DBImpl* base_db_;
  TitanDBImpl* tdb_;
  VersionSet* version_set_;
  TitanDBOptions tdb_options_;
  TitanOptions options_;
  port::Mutex* mutex_;

  BlobGCJobTest() {}
  ~BlobGCJobTest() {}

  void NewDB() {
    Status s;
    options_.create_if_missing = true;
    s = TitanDB::Open(options_, "/tmp/titandb/" + std::to_string(Random::GetTLSInstance()->Next()) + "/", &db_);
    ASSERT_OK(s);
    tdb_ = reinterpret_cast<TitanDBImpl*>(db_);
    version_set_ = tdb_->vset_.get();
    mutex_ = &tdb_->mutex_;
    base_db_ = reinterpret_cast<DBImpl*>(tdb_->GetRootDB());
  }

  void RunGC() {
    MutexLock l(mutex_);
    Status s;
    auto* cfh = base_db_->DefaultColumnFamily();

    // Build BlobGC
    std::unique_ptr<BlobGC> blob_gc;
    {
      std::shared_ptr<BlobGCPicker> blob_gc_picker =
          std::make_shared<BasicBlobGCPicker>();
      blob_gc = blob_gc_picker->PickBlobGC(
          version_set_->current()->GetBlobStorage(cfh->GetID()).get());
    }
    ASSERT_TRUE(blob_gc);

    BlobGCJob blob_gc_job(blob_gc.get(),
                          tdb_->db_options_,
                          tdb_->titan_cfs_options_[cfh->GetID()],
                          tdb_->env_,
                          EnvOptions(),
                          tdb_->blob_manager_.get(),
                          version_set_,
                          base_db_,
                          cfh->GetID(),
                          cfh,
                          mutex_);

    s = blob_gc_job.Prepare();
    ASSERT_OK(s);

    {
      mutex_->Unlock();
      s = blob_gc_job.Run();
      mutex_->Lock();
    }
    ASSERT_OK(s);

    s = blob_gc_job.Finish();
    ASSERT_OK(s);
  }

  Status NewIterator(uint64_t file_number,
                     uint64_t file_size,
                     InternalIterator** iter) {
    std::unique_ptr<RandomAccessFileReader> file;
    Status s = NewBlobFileReader(file_number, 0, tdb_->db_options_,
                                 tdb_->env_options_, tdb_->env_, &file);
    // TODO memory leak here
    if (!s.ok()) {
      return s;
    }
    *iter = new BlobFileIterator(std::move(file), file_number, file_size);
    return Status::OK();
  }
};

TEST_F(BlobGCJobTest, Basic) {
  NewDB();
  for (int i = 0; i < MAX_KEY_NUM; i++) {
    std::string key = std::to_string(i);
    std::string value(key.data(), 10240);
    db_->Put(WriteOptions(), key, value);
  }
  FlushOptions flush_options;
  flush_options.wait = true;
  db_->Flush(flush_options);
  std::string result;
  ASSERT_OK(db_->Get(ReadOptions(), std::to_string(0), &result));
  ASSERT_OK(db_->Get(ReadOptions(), std::to_string(2), &result));
  for (int i = 0; i < MAX_KEY_NUM; i++) {
    if (i % 2 != 0)
      continue;
    std::string key = std::to_string(i);
    db_->Delete(WriteOptions(), key);
  }
  db_->Flush(flush_options);
  ASSERT_NOK(db_->Get(ReadOptions(), std::to_string(0), &result));
  ASSERT_NOK(db_->Get(ReadOptions(), std::to_string(2), &result));
  Version* v = nullptr;
  {
    MutexLock l(mutex_);
    v = version_set_->current();
  }
  ASSERT_TRUE(v != nullptr);
  auto b = v->GetBlobStorage(base_db_->DefaultColumnFamily()->GetID());
  ASSERT_EQ(b->files().size(), 1);
  auto old = b->files().begin()->first;
  for(auto& f : *b->mutable_files()) {
    f.second->marked_for_sample = false;
  }
  InternalIterator* iter;
  ASSERT_OK(NewIterator(b->files().begin()->second->file_number,
                        b->files().begin()->second->file_size,
                        &iter));
  iter->SeekToFirst();
  for (int i = 0; i < MAX_KEY_NUM; i++, iter->Next()) {
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
//    std::string key = std::to_string(i);
//    ASSERT_TRUE(iter->key().size() == key.size());
//    ASSERT_TRUE(iter->key().compare(Slice(key)) == 0);
//    fprintf(stderr, "%s, ", iter->key().data());
  }
//  fprintf(stderr, "\n\n");
  RunGC();
  {
    v = version_set_->current();
  }
  b = v->GetBlobStorage(base_db_->DefaultColumnFamily()->GetID());
  ASSERT_EQ(b->files().size(), 1);
  auto new1 = b->files().begin()->first;
  ASSERT_TRUE(old != new1);
  ASSERT_OK(NewIterator(b->files().begin()->second->file_number,
                        b->files().begin()->second->file_size,
                        &iter));
  iter->SeekToFirst();
  for (int i = 0; i < MAX_KEY_NUM; i++) {
    if (i % 2 == 0)
      continue;
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
//    std::string key = std::to_string(i);
//    ASSERT_TRUE(iter->key().size() == key.size());
//    ASSERT_TRUE(iter->key().compare(Slice(key)) == 0);
//    fprintf(stderr, "%s, ", iter->key().data());
    ASSERT_OK(db_->Get(ReadOptions(), iter->key(), &result));
    ASSERT_TRUE(iter->value().size() == result.size());
    ASSERT_TRUE(iter->value().compare(result) == 0);
    iter->Next();
  }
}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
