#pragma once

#include "rocksdb/cache.h"
#include "util/compression.h"
#include "util/testharness.h"
#include "utilities/titandb/blob_format.h"

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

// Compresses the input data according to the compression type.
// Returns a slice with the output data and sets "*type" to the output
// compression type.
//
// If compression is actually performed, fills "*output" with the
// compressed data. However, if the compression ratio is not good, it
// returns the input slice directly and sets "*type" to
// kNoCompression.
Slice Compress(CompressionType* type, const Slice& input, std::string* output);

// Uncompresses the input data according to the uncompression type.
// If successful, fills "*buffer" with the uncompressed data and
// points "*output" to it.
Status Uncompress(CompressionType type, const Slice& input, Slice* output,
                  std::unique_ptr<char[]>* buffer);

void UnrefCacheHandle(void* cache, void* handle);

template <class T>
void DeleteCacheValue(const Slice&, void* value) {
  delete reinterpret_cast<T*>(value);
}

}  // namespace titandb
}  // namespace rocksdb
