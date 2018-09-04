#include "utilities/titandb/blob_gc_picker.h"
#include "util/filename.h"
#include "util/testharness.h"
#include "utilities/titandb/blob_file_builder.h"
#include "utilities/titandb/blob_file_cache.h"
#include "utilities/titandb/blob_file_iterator.h"
#include "utilities/titandb/blob_file_reader.h"
#include "utilities/titandb/version.h"

namespace rocksdb {
namespace titandb {

class BlobGCPickerTest : public testing::Test {
 public:
  std::unique_ptr<BlobStorage> blob_storage_;
  std::unique_ptr<BlobGCPicker> basic_blob_gc_picker_;

  BlobGCPickerTest() {}
  ~BlobGCPickerTest() {}

  void NewBlobStorageAndPicker(const TitanDBOptions& titan_db_options,
                               const TitanCFOptions& titan_cf_options) {
    auto blob_file_cache = std::make_shared<BlobFileCache>(
        titan_db_options, titan_cf_options, NewLRUCache(128));
    blob_storage_.reset(new BlobStorage(titan_cf_options, blob_file_cache));
    basic_blob_gc_picker_.reset(new BasicBlobGCPicker(titan_cf_options));
  }

  void AddBlobFile(uint64_t file_number, uint64_t file_size,
                   uint64_t discardable_size, bool being_gc = false) {
    blob_storage_->files_[file_number] = std::make_shared<BlobFileMeta>(
        file_number, file_size, discardable_size, being_gc);
  }

  void UpdateBlobStorage() { blob_storage_->ComputeGCScore(); }
};

TEST_F(BlobGCPickerTest, Basic) {
  TitanDBOptions titan_db_options;
  TitanCFOptions titan_cf_options;
  titan_cf_options.min_gc_batch_size = 0;
  NewBlobStorageAndPicker(titan_db_options, titan_cf_options);
  AddBlobFile(1U, 1U, 0U);
  UpdateBlobStorage();
  auto blob_gc = basic_blob_gc_picker_->PickBlobGC(blob_storage_.get());
  ASSERT_TRUE(blob_gc != nullptr);
  ASSERT_EQ(blob_gc->candidate_files().size(), 1);
  ASSERT_EQ(blob_gc->candidate_files()[0]->file_number, 1U);
}

TEST_F(BlobGCPickerTest, BeingGC) {
  TitanDBOptions titan_db_options;
  TitanCFOptions titan_cf_options;
  titan_cf_options.min_gc_batch_size = 0;
  NewBlobStorageAndPicker(titan_db_options, titan_cf_options);
  AddBlobFile(1U, 1U, 0U, true);
  UpdateBlobStorage();
  auto blob_gc = basic_blob_gc_picker_->PickBlobGC(blob_storage_.get());
  ASSERT_EQ(nullptr, blob_gc);
  NewBlobStorageAndPicker(titan_db_options, titan_cf_options);
  AddBlobFile(1U, 1U, 0U, true);
  AddBlobFile(2U, 1U, 0U);
  UpdateBlobStorage();
  blob_gc = basic_blob_gc_picker_->PickBlobGC(blob_storage_.get());
  ASSERT_EQ(blob_gc->candidate_files().size(), 1);
  ASSERT_EQ(blob_gc->candidate_files()[0]->file_number, 2U);
}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
