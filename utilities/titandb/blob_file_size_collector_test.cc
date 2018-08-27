//
// Created by 郑志铨 on 2018/8/22.
//

#include "util/filename.h"
#include "util/testharness.h"
#include "utilities/titandb/blob_file_builder.h"
#include "utilities/titandb/blob_file_cache.h"
#include "utilities/titandb/blob_file_iterator.h"
#include "utilities/titandb/blob_file_reader.h"
#include "utilities/titandb/blob_gc_picker.h"
#include "utilities/titandb/version.h"
#include "utilities/titandb/version_set.h"

namespace rocksdb {
namespace titandb {

class BlobFileSizeCollectorTest : public testing::Test {
 public:
  port::Mutex mutex_;
  VersionSet* vset_;

  BlobFileSizeCollectorTest() {}
  ~BlobFileSizeCollectorTest() {}
};

TEST_F(BlobFileSizeCollectorTest, Basic) {}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
