#pragma once

#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "rocksdb/options.h"
#include "table/format.h"

namespace rocksdb {
namespace titandb {

// Blob record format:
//
// key          : varint64 length + length bytes
// value        : varint64 length + length bytes
// checksum     : fixed32
struct BlobRecord {
  Slice key;
  Slice value;

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* src);

  friend bool operator==(const BlobRecord& lhs, const BlobRecord& rhs);
};

// Blob handle format:
//
// offset       : varint64
// size         : varint64
struct BlobHandle {
  uint64_t offset {0};
  uint64_t size {0};

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* src);

  friend bool operator==(const BlobHandle& lhs, const BlobHandle& rhs);
};

// Blob index format:
//
// file_number  : varint64
// blob_handle  : varint64 offset + varint64 size
struct BlobIndex {
  uint64_t file_number {0};
  BlobHandle blob_handle;

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* src);

  friend bool operator==(const BlobIndex& lhs, const BlobIndex& rhs);
};

// Blob file meta format:
//
// column_family_id : varint32
// file_number      : varint64
// file_size        : varint64
struct BlobFileMeta {
  uint32_t column_family_id {0};
  uint64_t file_number {0};
  uint64_t file_size {0};

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* src);

  friend bool operator==(const BlobFileMeta& lhs, const BlobFileMeta& rhs);
};

// Blob file footer format:
//
// compression          : 1 byte
// meta_index_handle    : varint64 offset + varint64 size
// <padding>            : [... kEncodedLength - 12] bytes
// magic_number         : fixed64
// checksum             : fixed32
struct BlobFileFooter {
  // The first 64bits from $(echo titandb/blob | sha1sum).
  static const uint64_t kMagicNumber {0xcd3f52ea0fe14511ull};
  static const uint64_t kEncodedLength {
      1 + BlockHandle::kMaxEncodedLength + 8 + 4
  };

  CompressionType compression {kNoCompression};
  BlockHandle meta_index_handle {BlockHandle::NullBlockHandle()};

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* src);

  friend bool operator==(const BlobFileFooter& lhs, const BlobFileFooter& rhs);
};

// A convenient template to decode a const slice.
template<typename T>
Status DecodeInto(const Slice& src, T* target) {
  auto tmp = src;
  auto s = target->DecodeFrom(&tmp);
  if (!s.ok() || !tmp.empty()) {
    s = Status::Corruption(Slice());
  }
  return s;
}

}  // namespace titandb
}  // namespace rocksdb
