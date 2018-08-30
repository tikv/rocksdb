#include "utilities/titandb/blob_file_reader.h"

namespace rocksdb {
namespace titandb {

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

void DeleteBlobCache(const Slice&, void* value) {
  delete reinterpret_cast<std::string*>(value);
}

}

Status BlobFileReader::Open(const TitanCFOptions& options,
                            std::unique_ptr<RandomAccessFileReader> file,
                            uint64_t file_size,
                            std::unique_ptr<BlobFileReader>* result) {
  if (file_size < BlobFileFooter::kEncodedLength) {
    return Status::Corruption("file is too short to be a blob file");
  }

  char footer_space[BlobFileFooter::kEncodedLength];
  Slice footer_input;
  Status s = file->Read(file_size - BlobFileFooter::kEncodedLength,
                        BlobFileFooter::kEncodedLength,
                        &footer_input, footer_space);
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
    : options_(options),
      file_(std::move(file)),
      cache_(options.blob_cache) {
  if (cache_) {
    GenerateCachePrefix(&cache_prefix_, cache_.get(), file_->file());
  }
}

Status BlobFileReader::Get(const ReadOptions& /*options*/,
                           const BlobHandle& handle,
                           BlobRecord* record, std::string* buffer) {
  Status s;
  std::string cache_key;
  if (cache_) {
    EncodeBlobCache(&cache_key, cache_prefix_, handle.offset);
    auto cache_handle = cache_->Lookup(cache_key);
    if (cache_handle) {
      *buffer = *reinterpret_cast<std::string*>(cache_->Value(cache_handle));
      s = DecodeInto(*buffer, record);
      cache_->Release(cache_handle);
      return s;
    }
  }

  Slice blob;
  buffer->resize(handle.size);
  s = file_->Read(handle.offset, handle.size, &blob, &(*buffer)[0]);
  if (!s.ok()) return s;

  if (cache_) {
    auto cache_value = new std::string(*buffer);
    cache_->Insert(cache_key, cache_value, cache_value->size(),
                   &DeleteBlobCache);
  }

  return DecodeInto(blob, record);
}

Status BlobFilePrefetcher::Get(const ReadOptions& options,
                               const BlobHandle& handle,
                               BlobRecord* record, std::string* buffer) {
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
