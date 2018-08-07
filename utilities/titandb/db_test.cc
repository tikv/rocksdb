#include <inttypes.h>

#include "util/testharness.h"
#include "utilities/titandb/db.h"

namespace rocksdb {
namespace titandb {

class TitanDBTest : public testing::Test {
 public:
  TitanDBTest() : dbname_(test::TmpDir()) {
    options_.create_if_missing = true;
    tdb_options_.min_blob_size = 0;
  }

  ~TitanDBTest() {
    delete db_;
  }

  void Open() {
    ASSERT_OK(TitanDB::Open(dbname_, options_, tdb_options_, &db_));
  }

  void Reopen() {
    ASSERT_OK(db_->Close());
    delete db_;
    ASSERT_OK(TitanDB::Open(dbname_, options_, tdb_options_, &db_));
  }

  void Put(uint64_t i, std::map<std::string, std::string>* data = nullptr) {
    std::string key = GenKey(i);
    std::string value = GenValue(i);
    ASSERT_OK(db_->Put(WriteOptions(), key, value));
    if (data != nullptr) {
      data->emplace(key, value);
    }
  }

  void Flush() {
    db_->Flush(FlushOptions());
  }

  void VerifyDB(const std::map<std::string, std::string>& data) {
    ReadOptions ropts;
    ropts.readahead_size = 1024;

    for (auto& kv : data) {
      std::string value;
      ASSERT_OK(db_->Get(ropts, kv.first, &value));
      ASSERT_EQ(value, kv.second);
    }

    std::unique_ptr<Iterator> iter(db_->NewIterator(ropts));
    iter->SeekToFirst();
    for (auto& kv : data) {
      ASSERT_EQ(iter->Valid(), true);
      ASSERT_EQ(iter->key(), kv.first);
      ASSERT_EQ(iter->value(), kv.second);
      iter->Next();
    }
  }

  std::string GenKey(uint64_t i) {
    char buf[64];
    snprintf(buf, sizeof(buf), "k-%08" PRIu64, i);
    return buf;
  }

  std::string GenValue(uint64_t i) {
    char buf[64];
    snprintf(buf, sizeof(buf), "v-%08" PRIu64, i);
    return buf;
  }

  std::string dbname_;
  Options options_;
  TitanDBOptions tdb_options_;

  TitanDB* db_ = nullptr;
};

TEST_F(TitanDBTest, Basic) {
  const uint64_t kNumKeys = 10000;
  const uint64_t kNumFiles = 10;
  std::map<std::string, std::string> data;
  Open();
  for (uint64_t i = 1; i <= kNumKeys; i++) {
    Put(i, &data);
    if (i % (kNumKeys / kNumFiles) == 0) {
      Flush();
    }
  }
  VerifyDB(data);
  Reopen();
  VerifyDB(data);
}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
