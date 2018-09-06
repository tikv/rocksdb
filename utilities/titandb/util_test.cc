#include "utilities/titandb/util.h"

namespace rocksdb {
namespace titandb {

class UtilTest : public testing::Test {};

TEST(UtilTest, Compression) {
  Slice input("aaaaaaaaaaaaaaaaaaaaaaaaaa");
  for (auto compression :
       {kSnappyCompression, kZlibCompression, kLZ4Compression, kZSTD}) {
    std::string buffer;
    auto compressed = Compress(&compression, input, &buffer);
    ASSERT_TRUE(compressed.size() <= input.size());
    Slice output;
    std::unique_ptr<char[]> uncompressed;
    ASSERT_OK(Uncompress(compression, compressed, &output, &uncompressed));
    ASSERT_EQ(output, input);
  }
}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
