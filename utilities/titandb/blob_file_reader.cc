#include "utilities/titandb/blob_file_reader.h"

#include "util/crc32c.h"
#include "util/filename.h"

namespace rocksdb {
namespace titandb {

Status NewBlobFileReader(uint64_t file_number, uint64_t readahead_size,
                         const TitanDBOptions& db_options,
                         const EnvOptions& env_options, Env* env,
                         std::unique_ptr<RandomAccessFileReader>* result) {
  std::unique_ptr<RandomAccessFile> file;
  auto file_name = BlobFileName(db_options.dirname, file_number);
  Status s = env->NewRandomAccessFile(file_name, &file, env_options);
  if (!s.ok()) return s;

  if (readahead_size > 0) {
    file = NewReadaheadRandomAccessFile(std::move(file), readahead_size);
  }
  result->reset(new RandomAccessFileReader(std::move(file), file_name));
  return s;
}

const uint64_t kMinReadaheadSize = 4 << 10;
const uint64_t kMaxReadaheadSize = 256 << 10;

namespace {

void GenerateCachePrefix(std::string* dst, Cache* cc, RandomAccessFile* file) {
  char buffer[kMaxVarint64Length * 3 + 1];
  auto size = file->GetUniqueId(buffer, sizeof(buffer));
  if (size == 0) {
    auto end = EncodeVarint64(buffer, cc->NewId());
    size = end - buffer;
  }
  dst->assign(buffer, size);
}

void EncodeBlobCache(std::string* dst, const Slice& prefix, uint64_t offset) {
  dst->assign(prefix.data(), prefix.size());
  PutVarint64(dst, offset);
}

}  // namespace

Status BlobFileReader::Open(const TitanCFOptions& options,
                            std::unique_ptr<RandomAccessFileReader> file,
                            uint64_t file_size,
                            std::unique_ptr<BlobFileReader>* result) {
  if (file_size < BlobFileFooter::kEncodedLength) {
    return Status::Corruption("file is too short to be a blob file");
  }

  char footer_space[BlobFileFooter::kEncodedLength];
  Slice footer_input;
  Status s =
      file->Read(file_size - BlobFileFooter::kEncodedLength,
                 BlobFileFooter::kEncodedLength, &footer_input, footer_space);
  if (!s.ok()) return s;

  BlobFileFooter footer;
  s = DecodeInto(footer_input, &footer);
  if (!s.ok()) return s;

  auto reader = new BlobFileReader(options, std::move(file));
  reader->footer_ = footer;
  result->reset(reader);
  return s;
}

BlobFileReader::BlobFileReader(const TitanCFOptions& options,
                               std::unique_ptr<RandomAccessFileReader> file)
    : options_(options), file_(std::move(file)), cache_(options.blob_cache) {
  if (cache_) {
    GenerateCachePrefix(&cache_prefix_, cache_.get(), file_->file());
  }
}

Status BlobFileReader::Get(const ReadOptions& /*options*/,
                           const BlobHandle& handle, BlobRecord* record,
                           PinnableSlice* buffer) {
  Status s;
  std::string cache_key;
  Cache::Handle* cache_handle = nullptr;
  if (cache_) {
    EncodeBlobCache(&cache_key, cache_prefix_, handle.offset);
    cache_handle = cache_->Lookup(cache_key);
    if (cache_handle) {
      auto blob = reinterpret_cast<OwnedSlice*>(cache_->Value(cache_handle));
      buffer->PinSlice(*blob, UnrefCacheHandle, cache_.get(), cache_handle);
      return DecodeInto(*blob, record);
    }
  }

  OwnedSlice blob;
  s = ReadBlob(handle, &blob);
  if (!s.ok()) return s;
  s = DecodeInto(blob, record);
  if (!s.ok()) return s;

  if (cache_) {
    auto cache_value = new OwnedSlice(std::move(blob));
    auto cache_size = cache_value->size() + sizeof(*cache_value);
    cache_->Insert(cache_key, cache_value, cache_size,
                   &DeleteCacheValue<OwnedSlice>, &cache_handle);
    buffer->PinSlice(*cache_value, UnrefCacheHandle, cache_.get(),
                     cache_handle);
  } else {
    buffer->PinSlice(blob, OwnedSlice::CleanupFunc, blob.release(), nullptr);
  }

  return s;
}

Status BlobFileReader::ReadBlob(const BlobHandle& handle, OwnedSlice* output) {
  Slice blob;
  size_t blob_size = handle.size + kBlobTailerSize;
  std::unique_ptr<char[]> compressed(new char[blob_size]);
  Status s = file_->Read(handle.offset, blob_size, &blob, compressed.get());
  if (!s.ok()) return s;

  auto tailer = blob.data() + handle.size;
  auto checksum = DecodeFixed32(tailer + 1);
  if (crc32c::Value(blob.data(), handle.size) != checksum) {
    return Status::Corruption("BlobRecord", "checksum");
  }
  auto compression = static_cast<CompressionType>(*tailer);
  if (compression == kNoCompression) {
    output->reset(std::move(compressed), handle.size);
  } else {
    UncompressionContext ctx(compression);
    Slice input(blob.data(), handle.size);
    s = Uncompress(ctx, input, output);
    if (!s.ok()) return s;
  }

  return s;
}

Status BlobFilePrefetcher::Get(const ReadOptions& options,
                               const BlobHandle& handle, BlobRecord* record,
                               PinnableSlice* buffer) {
  if (handle.offset == last_offset_) {
    last_offset_ = handle.offset + handle.size;
    if (handle.offset + handle.size > readahead_limit_) {
      readahead_size_ = std::max(handle.size, readahead_size_);
      reader_->file_->Prefetch(handle.offset, readahead_size_);
      readahead_limit_ = handle.offset + readahead_size_;
      readahead_size_ = std::min(kMaxReadaheadSize, readahead_size_ * 2);
    }
  } else {
    last_offset_ = handle.offset + handle.size;
    readahead_size_ = 0;
    readahead_limit_ = 0;
  }

  return reader_->Get(options, handle, record, buffer);
}

}  // namespace titandb
}  // namespace rocksdb
