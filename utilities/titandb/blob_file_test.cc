#include "util/filename.h"
#include "util/testharness.h"
#include "utilities/titandb/blob_file_builder.h"
#include "utilities/titandb/blob_file_cache.h"
#include "utilities/titandb/blob_file_reader.h"

namespace rocksdb {
namespace titandb {

class BlobFileTest : public testing::Test {
 public:
  BlobFileTest() : dirname_(test::TmpDir(env_)) {
    file_name_ = BlobFileName(dirname_, file_number_);
  }

  ~BlobFileTest() {
    env_->DeleteFile(file_name_);
    env_->DeleteDir(dirname_);
  }

  void TestBlobFile(TitanOptions options) {
    options.dirname = dirname_;
    TitanDBOptions db_options(options);
    TitanCFOptions cf_options(options);
    BlobFileCache cache(db_options, cf_options, {NewLRUCache(128)});

    const int n = 100;
    std::vector<BlobHandle> handles(n);

    std::unique_ptr<WritableFileWriter> file;
    {
      std::unique_ptr<WritableFile> f;
      ASSERT_OK(env_->NewWritableFile(file_name_, &f, env_options_));
      file.reset(new WritableFileWriter(std::move(f), env_options_));
    }
    std::unique_ptr<BlobFileBuilder> builder(
        new BlobFileBuilder(cf_options, file.get()));

    for (int i = 0; i < n; i++) {
      auto key = std::to_string(i);
      auto value = std::string(1024, i);
      BlobRecord record;
      record.key = key;
      record.value = value;
      builder->Add(record, &handles[i]);
      ASSERT_OK(builder->status());
    }
    ASSERT_OK(builder->Finish());
    ASSERT_OK(builder->status());

    uint64_t file_size = 0;
    ASSERT_OK(env_->GetFileSize(file_name_, &file_size));

    ReadOptions ro;
    std::unique_ptr<BlobFilePrefetcher> prefetcher;
    ASSERT_OK(cache.NewPrefetcher(file_number_, file_size, &prefetcher));
    for (int i = 0; i < n; i++) {
      auto key = std::to_string(i);
      auto value = std::string(1024, i);
      BlobRecord expect;
      expect.key = key;
      expect.value = value;
      BlobRecord record;
      PinnableSlice buffer;
      ASSERT_OK(
          cache.Get(ro, file_number_, file_size, handles[i], &record, &buffer));
      ASSERT_EQ(record, expect);
      buffer.Reset();
      ASSERT_OK(
          cache.Get(ro, file_number_, file_size, handles[i], &record, &buffer));
      ASSERT_EQ(record, expect);
      buffer.Reset();
      ASSERT_OK(prefetcher->Get(ro, handles[i], &record, &buffer));
      ASSERT_EQ(record, expect);
      buffer.Reset();
      ASSERT_OK(prefetcher->Get(ro, handles[i], &record, &buffer));
      ASSERT_EQ(record, expect);
    }
  }

  Env* env_{Env::Default()};
  EnvOptions env_options_;
  std::string dirname_;
  std::string file_name_;
  uint64_t file_number_{1};
};

TEST_F(BlobFileTest, Basic) {
  TitanOptions options;
  TestBlobFile(options);
  options.blob_cache = NewLRUCache(1 << 20);
  TestBlobFile(options);
  options.blob_file_compression = kLZ4Compression;
  TestBlobFile(options);
}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
