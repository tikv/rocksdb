#include "utilities/titandb/util.h"

namespace rocksdb {
namespace titandb {

class UtilTest : public testing::Test {};

TEST(UtilTest, Compression) {
  Slice input("aaaaaaaaaaaaaaaaaaaaaaaaaa");
  for (auto compression :
       {kSnappyCompression, kZlibCompression, kLZ4Compression, kZSTD}) {
    std::string buffer;
    CompressionContext compression_ctx(compression);
    auto compressed = Compress(compression_ctx, input, &buffer, &compression);
    ASSERT_TRUE(compressed.size() <= input.size());
    UncompressionContext uncompression_ctx(compression);
    Slice output;
    std::unique_ptr<char[]> uncompressed;
    ASSERT_OK(
        Uncompress(uncompression_ctx, compressed, &output, &uncompressed));
    ASSERT_EQ(output, input);
  }
}

}  // namespace titandb
}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
