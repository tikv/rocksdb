//
// Created by 郑志铨 on 2018/8/13.
//

#include "utilities/titandb/blob_file_iterator.h"
#include "util/filename.h"
#include "util/testharness.h"
#include "utilities/titandb/blob_file_builder.h"
#include "utilities/titandb/blob_file_cache.h"
#include "utilities/titandb/blob_file_reader.h"

namespace rocksdb {
namespace titandb {

class BlobFileIteratorTest : public testing::Test {
 public:
  BlobFileIteratorTest() : dirname_(test::TmpDir(env_)) {
    file_name_ = BlobFileName(dirname_, file_number_);
  }

  ~BlobFileIteratorTest() {
    env_->DeleteFile(file_name_);
    env_->DeleteDir(dirname_);
  }

  void TestBlobFileIterator(TitanOptions options) {
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

    std::unique_ptr<RandomAccessFileReader> readable_file;
    NewBlobFileReader(file_number_, 0, db_options, env_options_, env_,
                      &readable_file);
    BlobFileIterator blob_file_iterator(std::move(readable_file), file_number_,
                                        file_size);
    blob_file_iterator.SeekToFirst();
    for (int i = 0; i < n; blob_file_iterator.Next(), i++) {
      ASSERT_OK(blob_file_iterator.status());
      ASSERT_EQ(blob_file_iterator.Valid(), true);
      auto id = std::to_string(i);
      ASSERT_EQ(id, blob_file_iterator.key());
      ASSERT_EQ(id, blob_file_iterator.value());
      std::string tmp;
      blob_file_iterator.GetProperty(BlobFileIterator::PROPERTY_FILE_NAME,
                                     &tmp);
      uint64_t file_number = *reinterpret_cast<const uint64_t*>(tmp.data());
      ASSERT_EQ(file_number_, file_number);
      blob_file_iterator.GetProperty(BlobFileIterator::PROPERTY_FILE_OFFSET,
                                     &tmp);
      uint64_t entry_offset = *reinterpret_cast<const uint64_t*>(tmp.data());
      ASSERT_EQ(handles[i].offset, entry_offset);
    }
  }

 private:
  Env* env_{Env::Default()};
  EnvOptions env_options_;
  std::string dirname_;
  std::string file_name_;
  uint64_t file_number_{1};
};

TEST_F(BlobFileIteratorTest, Basic) {
  TitanOptions options;
  TestBlobFileIterator(options);
}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
