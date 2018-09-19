#pragma once

#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "table/format.h"
#include "utilities/titandb/util.h"

namespace rocksdb {
namespace titandb {

// Blob header format:
//
// crc          : fixed32
// size         : fixed32
// compression  : char
const uint64_t kBlobHeaderSize = 9;

// Blob record format:
//
// key          : varint64 length + length bytes
// value        : varint64 length + length bytes
struct BlobRecord {
  Slice key;
  Slice value;

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* src);

  friend bool operator==(const BlobRecord& lhs, const BlobRecord& rhs);
};

class BlobEncoder {
 public:
  BlobEncoder(CompressionType compression) : compression_ctx_(compression) {}

  void EncodeRecord(const BlobRecord& record);

  Slice GetHeader() const { return Slice(header_, sizeof(header_)); }
  Slice GetRecord() const { return record_; }

  size_t GetEncodedSize() const { return sizeof(header_) + record_.size(); }

 private:
  char header_[kBlobHeaderSize];
  Slice record_;
  std::string record_buffer_;
  std::string compressed_buffer_;
  CompressionContext compression_ctx_;
};

class BlobDecoder {
 public:
  Status DecodeHeader(Slice* src);
  Status DecodeRecord(Slice* src, BlobRecord* record, OwnedSlice* buffer);

  size_t GetRecordSize() const { return record_size_; }

 private:
  uint32_t crc_{0};
  uint32_t header_crc_{0};
  uint32_t record_size_{0};
  CompressionType compression_{kNoCompression};
};

// Blob handle format:
//
// offset       : varint64
// size         : varint64
struct BlobHandle {
  uint64_t offset{0};
  uint64_t size{0};

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* src);

  friend bool operator==(const BlobHandle& lhs, const BlobHandle& rhs);
};

// Blob index format:
//
// type         : char
// file_number  : varint64
// blob_handle  : varint64 offset + varint64 size
struct BlobIndex {
  enum Type : unsigned char {
    kBlobRecord = 1,
  };
  uint64_t file_number{0};
  BlobHandle blob_handle;

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* src);

  friend bool operator==(const BlobIndex& lhs, const BlobIndex& rhs);
};

// Blob file meta format:
//
// file_number      : varint64
// file_size        : varint64
struct BlobFileMeta {
  BlobFileMeta(){};
  BlobFileMeta(uint64_t _file_number, uint64_t _file_size,
               uint64_t _discardable_size = 0, bool _being_gc = false,
               bool _marked_for_sample = true)
      : file_number(_file_number),
        file_size(_file_size),
        discardable_size(_discardable_size),
        marked_for_sample(_marked_for_sample),
        being_gc(_being_gc) {}

  // Persistent field, we should never modify it.
  uint64_t file_number{0};
  uint64_t file_size{0};

  // Not persistent field
  // These fields maybe are mutate, need to be protected by db.mutex_
  uint64_t discardable_size{0};
  bool marked_for_gc = false;
  bool marked_for_sample = true;

  // This field can be modified concurrently
  std::atomic_bool being_gc{false};

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* src);

  friend bool operator==(const BlobFileMeta& lhs, const BlobFileMeta& rhs);
};

// Blob file footer format:
//
// meta_index_handle    : varint64 offset + varint64 size
// <padding>            : [... kEncodedLength - 12] bytes
// magic_number         : fixed64
// checksum             : fixed32
struct BlobFileFooter {
  // The first 64bits from $(echo titandb/blob | sha1sum).
  static const uint64_t kMagicNumber{0xcd3f52ea0fe14511ull};
  static const uint64_t kEncodedLength{BlockHandle::kMaxEncodedLength + 8 + 4};

  BlockHandle meta_index_handle{BlockHandle::NullBlockHandle()};

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* src);

  friend bool operator==(const BlobFileFooter& lhs, const BlobFileFooter& rhs);
};

// A convenient template to decode a const slice.
template <typename T>
Status DecodeInto(const Slice& src, T* target) {
  auto tmp = src;
  auto s = target->DecodeFrom(&tmp);
  if (s.ok() && !tmp.empty()) {
    s = Status::Corruption(Slice());
  }
  return s;
}

}  // namespace titandb
}  // namespace rocksdb
