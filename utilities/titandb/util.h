#pragma once

#include "rocksdb/cache.h"
#include "util/compression.h"
#include "util/testharness.h"

namespace rocksdb {
namespace titandb {

template <typename T>
void CheckCodec(const T& input) {
  std::string buffer;
  input.EncodeTo(&buffer);
  T output;
  ASSERT_OK(DecodeInto(buffer, &output));
  ASSERT_EQ(output, input);
}

// A slice pointed to an owned buffer.
class OwnedSlice : public Slice {
 public:
  void reset(std::unique_ptr<char[]> _data, size_t _size) {
    data_ = _data.get();
    size_ = _size;
    buffer_ = std::move(_data);
  }

  char* release() {
    data_ = nullptr;
    size_ = 0;
    return buffer_.release();
  }

  static void CleanupFunc(void* buffer, void*) {
    delete[] reinterpret_cast<char*>(buffer);
  }

 private:
  std::unique_ptr<char[]> buffer_;
};

// Compresses the input data according to the compression context.
// Returns a slice with the output data and sets "*type" to the output
// compression type.
//
// If compression is actually performed, fills "*output" with the
// compressed data. However, if the compression ratio is not good, it
// returns the input slice directly and sets "*type" to
// kNoCompression.
Slice Compress(const CompressionContext& ctx, const Slice& input,
               std::string* output, CompressionType* type);

// Uncompresses the input data according to the uncompression type.
// If successful, fills "*buffer" with the uncompressed data and
// points "*output" to it.
Status Uncompress(const UncompressionContext& ctx, const Slice& input,
                  OwnedSlice* output);

void UnrefCacheHandle(void* cache, void* handle);

template <class T>
void DeleteCacheValue(const Slice&, void* value) {
  delete reinterpret_cast<T*>(value);
}

}  // namespace titandb
}  // namespace rocksdb
