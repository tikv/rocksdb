#include "utilities/titandb/util.h"

namespace rocksdb {
namespace titandb {

// See util/compression.h.
const uint32_t kCompressionFormat = 2;

bool GoodCompressionRatio(size_t compressed_size, size_t raw_size) {
  // Check to see if compressed less than 12.5%
  return compressed_size < raw_size - (raw_size / 8u);
}

Slice Compress(CompressionType* type, const Slice& input, std::string* output) {
  if (*type == kNoCompression) {
    return input;
  }

  // TODO: use a configurable options.
  CompressionOptions opts;

  // Returns compressed block contents if:
  // (1) the compression method is supported in this platform and
  // (2) the compression rate is "good enough".
  switch (*type) {
    case kSnappyCompression:
      if (Snappy_Compress(opts, input.data(), input.size(), output) &&
          GoodCompressionRatio(output->size(), input.size())) {
        return *output;
      }
      break;
    case kZlibCompression:
      if (Zlib_Compress(opts, kCompressionFormat, input.data(), input.size(),
                        output) &&
          GoodCompressionRatio(output->size(), input.size())) {
        return *output;
      }
      break;
    case kBZip2Compression:
      if (BZip2_Compress(opts, kCompressionFormat, input.data(), input.size(),
                         output) &&
          GoodCompressionRatio(output->size(), input.size())) {
        return *output;
      }
      break;
    case kLZ4Compression:
      if (LZ4_Compress(opts, kCompressionFormat, input.data(), input.size(),
                       output) &&
          GoodCompressionRatio(output->size(), input.size())) {
        return *output;
      }
      break;
    case kLZ4HCCompression:
      if (LZ4HC_Compress(opts, kCompressionFormat, input.data(), input.size(),
                         output) &&
          GoodCompressionRatio(output->size(), input.size())) {
        return *output;
      }
      break;
    case kXpressCompression:
      if (XPRESS_Compress(input.data(), input.size(), output) &&
          GoodCompressionRatio(output->size(), input.size())) {
        return *output;
      }
      break;
    case kZSTD:
    case kZSTDNotFinalCompression:
      if (ZSTD_Compress(opts, input.data(), input.size(), output) &&
          GoodCompressionRatio(output->size(), input.size())) {
        return *output;
      }
      break;
    default: {} // Do not recognize this compression type
  }

  // Compression method is not supported, or not good compression
  // ratio, so just fall back to uncompressed form.
  *type = kNoCompression;
  return input;
}

Status Uncompress(CompressionType type, const Slice& input, Slice* output,
                  std::unique_ptr<char[]>* buffer) {
  int size = 0;
  assert(type != kNoCompression);

  switch (type) {
    case kSnappyCompression: {
      size_t usize = 0;
      if (!Snappy_GetUncompressedLength(input.data(), input.size(), &usize)) {
        return Status::Corruption("Corrupted compressed blob", "Snappy");
      }
      buffer->reset(new char[usize]);
      if (!Snappy_Uncompress(input.data(), input.size(), buffer->get())) {
        return Status::Corruption("Corrupted compressed blob", "Snappy");
      }
      *output = Slice(buffer->get(), usize);
      break;
    }
    case kZlibCompression:
      buffer->reset(Zlib_Uncompress(input.data(), input.size(), &size,
                                    kCompressionFormat));
      if (!buffer->get()) {
        return Status::Corruption("Corrupted compressed blob", "Zlib");
      }
      *output = Slice(buffer->get(), size);
      break;
    case kBZip2Compression:
      buffer->reset(BZip2_Uncompress(
          input.data(), input.size(), &size, kCompressionFormat));
      if (!buffer->get()) {
        return Status::Corruption("Corrupted compressed blob", "Bzip2");
      }
      *output = Slice(buffer->get(), size);
      break;
    case kLZ4Compression:
      buffer->reset(LZ4_Uncompress(input.data(), input.size(), &size,
                                   kCompressionFormat));
      if (!buffer->get()) {
        return Status::Corruption("Corrupted compressed blob", "LZ4");
      }
      *output = Slice(buffer->get(), size);
      break;
    case kLZ4HCCompression:
      buffer->reset(LZ4_Uncompress(input.data(), input.size(), &size,
                                   kCompressionFormat));
      if (!buffer->get()) {
        return Status::Corruption("Corrupted compressed blob", "LZ4HC");
      }
      *output = Slice(buffer->get(), size);
      break;
    case kXpressCompression:
      buffer->reset(XPRESS_Uncompress(input.data(), input.size(), &size));
      if (!buffer->get()) {
        return Status::Corruption("Corrupted compressed blob", "Xpress");
      }
      *output = Slice(buffer->get(), size);
      break;
    case kZSTD:
    case kZSTDNotFinalCompression:
      buffer->reset(ZSTD_Uncompress(input.data(), input.size(), &size));
      if (!buffer->get()) {
        return Status::Corruption("Corrupted compressed blob", "ZSTD");
      }
      *output = Slice(buffer->get(), size);
      break;
    default:
      return Status::Corruption("bad compression type");
  }

  return Status::OK();
}

void UnrefCacheHandle(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

}  // namespace titandb
}  // namespace rocksdb
