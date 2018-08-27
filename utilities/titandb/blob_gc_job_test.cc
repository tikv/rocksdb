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
#include "titan_db_impl.h"
#include "blob_gc_job.h"

namespace rocksdb {
namespace titandb {

class BlobGCJobTest : public testing::Test {
 public:
  TitanDB* db_;
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
    auto* db_impl = reinterpret_cast<TitanDBImpl*>(db_);
    version_set_ = db_impl->vset_.get();
    mutex_ = &db_impl->mutex_;
  }

  void RunGC() {
    Status s;
    auto* tdb_impl = reinterpret_cast<TitanDBImpl*>(db_);
    auto* db_impl = reinterpret_cast<DBImpl*>(db_);
    auto* cfh = db_impl->DefaultColumnFamily();

    // Build BlobGC
    std::unique_ptr<BlobGC> blob_gc;
    {
      MutexLock l(mutex_);
      std::shared_ptr<BlobGCPicker> blob_gc_picker =
          std::make_shared<BasicBlobGCPicker>();
      blob_gc = blob_gc_picker->PickBlobGC(
          version_set_->current()->GetBlobStorage(cfh->GetID()).get());
    }
    ASSERT_EQ(blob_gc, nullptr);

    BlobGCJob blob_gc_job(blob_gc.get(),
                          tdb_impl->db_options_,
                          TitanCFOptions(),
                          tdb_impl->env_,
                          tdb_impl->env_options_,
                          tdb_impl->blob_manager_.get(),
                          version_set_,
                          db_,
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
};

TEST_F(BlobGCJobTest, Basic) {
  NewDB();
  for (int i = 0; i < 10000; i++) {
    std::string key = std::to_string(i);
    std::string value(key.data(), 10240);
    db_->Put(WriteOptions(), key, value);
  }
  FlushOptions flush_options;
  flush_options.wait = true;
  db_->Flush(flush_options);
  RunGC();
}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
