#include "utilities/titandb/blob_file_size_collector.h"

#include "util/filename.h"
#include "util/testharness.h"
#include "utilities/titandb/blob_file_builder.h"
#include "utilities/titandb/blob_file_cache.h"
#include "utilities/titandb/blob_gc_picker.h"
#include "utilities/titandb/version_set.h"

namespace rocksdb {
namespace titandb {

const static uint32_t kDefauleColumnFamilyID = 0x77;

class BlobFileSizeCollectorTest : public testing::Test {
 public:
  port::Mutex mutex_;
  std::unique_ptr<VersionSet> vset_;

  BlobFileSizeCollectorTest() {}
  ~BlobFileSizeCollectorTest() {}

  void NewVersionSet(const TitanDBOptions& titan_db_options,
                     const TitanCFOptions& titan_cf_options) {
    auto blob_file_cache = std::make_shared<BlobFileCache>(
        titan_db_options, titan_cf_options, NewLRUCache(128));
    auto v = new Version(vset_.get());
    auto storage =
        std::make_shared<BlobStorage>(TitanCFOptions(), blob_file_cache);
    v->column_families_.emplace(kDefauleColumnFamilyID, storage);
    vset_.reset(new VersionSet(titan_db_options));
    vset_->versions_.Append(v);
  }

  void AddBlobFile(uint64_t file_number, uint64_t file_size,
                   uint64_t discardable_size, bool being_gc = false) {
    vset_->current()
        ->column_families_[kDefauleColumnFamilyID]
        ->files_[file_number] = std::make_shared<BlobFileMeta>(
        file_number, file_size, discardable_size, being_gc);
  }

  void TestBasic() {
    NewVersionSet(TitanDBOptions(), TitanCFOptions());
    CompactionJobInfo cji;
    cji.cf_id = kDefauleColumnFamilyID;
    AddBlobFile(1, 100, 5);
    auto file = vset_->current()->GetBlobStorage(kDefauleColumnFamilyID).lock()->files_[1];
    ASSERT_EQ(file->discardable_size, 5);
    TablePropertiesCollectorFactory::Context context;
    context.column_family_id = kDefauleColumnFamilyID;
    BlobFileSizeCollectorFactory factory;
    std::shared_ptr<TablePropertiesCollector> c(
        factory.CreateTablePropertiesCollector(context));
    BlobIndex bi;
    bi.file_number = 1;
    bi.blob_handle.size = 80;
    std::string tmp;
    bi.EncodeTo(&tmp);
    ASSERT_OK(c->AddUserKey("random", tmp, EntryType::kEntryBlobIndex, 0, 0));
    std::shared_ptr<TableProperties> tp = std::make_shared<TableProperties>();
    UserCollectedProperties u;
    c->Finish(&u);
    tp->user_collected_properties.insert(u.begin(), u.end());
    cji.table_properties["1"] = tp;
    cji.input_files.emplace_back("1");
    c.reset(factory.CreateTablePropertiesCollector(context));
    bi.file_number = 1;
    bi.blob_handle.size = 60;
    tmp.clear();
    bi.EncodeTo(&tmp);
    ASSERT_OK(c->AddUserKey("random2", tmp, EntryType::kEntryBlobIndex, 0, 0));
    u.clear();
    c->Finish(&u);
    std::shared_ptr<TableProperties> tp2 = std::make_shared<TableProperties>();
    tp2->user_collected_properties.insert(u.begin(), u.end());
    cji.table_properties["2"] = tp2;
    cji.output_files.emplace_back("2");
    port::Mutex mutex;
    BlobDiscardableSizeListener listener(nullptr, &mutex, vset_.get());
    listener.OnCompactionCompleted(nullptr, cji);
    ASSERT_EQ(file->discardable_size, 25);
  }
};

TEST_F(BlobFileSizeCollectorTest, Basic) {
  TestBasic();
}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
