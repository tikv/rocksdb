#pragma once

#include "rocksdb/cache.h"
#include "util/compression.h"

namespace rocksdb {
namespace titandb {

template<typename T>
void CheckCodec(const T& input) {
  std::string buffer;
  input.EncodeTo(&buffer);
  T output;
  ASSERT_OK(DecodeInto(buffer, &output));
  ASSERT_EQ(output, input);
}

// Compresses the input data according to the compression context.
// Returns a slice with the output data and sets "*type" to the
// compression type.
//
// If compression is actually performed, fills "*output" with the
// compressed data. However, if the compression ratio is not good, it
// returns the input slice directly and sets "*type" to
// kNoCompression.
Slice Compress(const CompressionContext& ctx,
               const Slice& input,
               std::string* output,
               CompressionType* type);

// Uncompresses the input data according to the uncompression context.
// If successful, fills "*buffer" with the uncompressed data and
// points "*output" to it.
Status Uncompress(const UncompressionContext& ctx,
                  const Slice& input,
                  Slice* output,
                  std::unique_ptr<char[]>* buffer);

void UnrefCacheHandle(void* cache, void* handle);

template<class T>
void DeleteCacheValue(const Slice&, void* value) {
  delete reinterpret_cast<T*>(value);
}

}  // namespace titandb
}  // namespace rocksdb
