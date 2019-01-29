#include "utilities/titandb/blob_file_size_collector.h"

#include "utilities/titandb/base_db_listener.h"
#include "util/testharness.h"
#include "utilities/titandb/util.h"

namespace rocksdb {
namespace titandb {

class BlobFileSizeCollectorTest : public testing::Test { };

TEST_F(BlobFileSizeCollectorTest, Basic) {
  BlockBasedTableOptions op;
  std::unique_ptr<TableFactory> block_based_table_fac(
      NewBlockBasedTableFactory());
  std::unique_ptr<WritableFile> file;
  ASSERT_OK(Env::Default()->NewWritableFile("Test", &file, EnvOptions()));
  std::unique_ptr<WritableFileWriter> file_writer(new WritableFileWriter(std::move(file), EnvOptions()));
  std::vector<std::unique_ptr<IntTblPropCollectorFactory>>
      int_tbl_prop_collector_factories;
  TablePropertiesCollectorFactory::Context context;
  context.column_family_id = 0;
  std::unique_ptr<TablePropertiesCollector> collector(
      BlobFileSizeCollectorFactory().CreateTablePropertiesCollector(context));
  int_tbl_prop_collector_factories.emplace_back(collector);
  std::string compression_dict;
  TableBuilderOptions tboptions(ImmutableCFOptions(), MutableCFOptions(), InternalKeyComparator(BytewiseComparator()), &int_tbl_prop_collector_factories, CompressionType::kNoCompression, CompressionOptions(), &compression_dict, true, "TEST", 0);
  std::unique_ptr<TableBuilder> table_builder(block_based_table_fac->NewTableBuilder(tboptions, 0, file_writer.get()));

}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
