#include "util/testharness.h"
#include "util/filename.h"
#include "utilities/titandb/blob_file_cache.h"
#include "utilities/titandb/blob_file_reader.h"
#include "utilities/titandb/blob_file_builder.h"

namespace rocksdb {
namespace titandb {

class BlobFileTest : public testing::Test {
 public:
  BlobFileTest()
      : env_(db_options_.env),
        env_options_(db_options_),
        dirname_(test::TmpDir(env_)) {
    file_name_ = BlobFileName(dirname_, file_number_);
  }

  ~BlobFileTest() {
    env_->DeleteFile(file_name_);
    env_->DeleteDir(dirname_);
  }

  void TestBlobFile(TitanDBOptions tdb_options) {
    tdb_options.dirname = dirname_;
    BlobFileCache cache(db_options_, tdb_options);

    const int n = 100;
    std::vector<BlobHandle> handles(n);

    std::unique_ptr<WritableFileWriter> file;
    {
      std::unique_ptr<WritableFile> result;
      ASSERT_OK(env_->NewWritableFile(file_name_, &result, env_options_));
      file.reset(new WritableFileWriter(std::move(result), env_options_));
    }
    std::unique_ptr<BlobFileBuilder> builder(
        new BlobFileBuilder(tdb_options, file.get()));
    for (int i = 0; i < n; i++) {
      auto id = std::to_string(i);
      BlobRecord record;
      record.key = id;
      record.value = id;
      builder->Add(record, &handles[i]);
      ASSERT_OK(builder->status());
    }
    ASSERT_OK(builder->Finish());
    ASSERT_OK(builder->status());

    uint64_t file_size = 0;
    ASSERT_OK(env_->GetFileSize(file_name_, &file_size));

    ReadOptions ro;
    std::unique_ptr<BlobFileReader> reader;
    ASSERT_OK(cache.NewReader(ro, file_number_, file_size, &reader));
    for (int i = 0; i < n; i++) {
      auto id = std::to_string(i);
      BlobRecord expect;
      expect.key = id;
      expect.value = id;
      BlobRecord record;
      std::string buffer;
      ASSERT_OK(reader->Get(ro, handles[i], &record, &buffer));
      ASSERT_EQ(record, expect);
      ASSERT_OK(cache.Get(ro, file_number_, file_size, handles[i],
                          &record, &buffer));
      ASSERT_EQ(record, expect);
    }
  }

  DBOptions db_options_;
  Env* env_;
  EnvOptions env_options_;
  std::string dirname_;
  std::string file_name_;
  uint64_t file_number_ {1};
};

TEST_F(BlobFileTest, Basic) {
  TitanDBOptions options;
  TestBlobFile(options);
}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
