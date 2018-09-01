#include "utilities/titandb/blob_format.h"

#include "util/coding.h"
#include "util/crc32c.h"

namespace rocksdb {
namespace titandb {

namespace {

bool GetChar(Slice* src, unsigned char* value) {
  if (src->size() < 1) return false;
  *value = *src->data();
  src->remove_prefix(1);
  return true;
}

}  // namespace

void BlobRecord::EncodeTo(std::string* dst) const {
  PutLengthPrefixedSlice(dst, key);
  PutLengthPrefixedSlice(dst, value);
}

Status BlobRecord::DecodeFrom(Slice* src) {
  if (!GetLengthPrefixedSlice(src, &key) ||
      !GetLengthPrefixedSlice(src, &value)) {
    return Status::Corruption("BlobRecord");
  }
  return Status::OK();
}

bool operator==(const BlobRecord& lhs, const BlobRecord& rhs) {
  return lhs.key == rhs.key && lhs.value == rhs.value;
}

void BlobHandle::EncodeTo(std::string* dst) const {
  PutVarint64(dst, offset);
  PutVarint64(dst, size);
}

Status BlobHandle::DecodeFrom(Slice* src) {
  if (!GetVarint64(src, &offset) ||
      !GetVarint64(src, &size)) {
    return Status::Corruption("BlobHandle");
  }
  return Status::OK();
}

bool operator==(const BlobHandle& lhs, const BlobHandle& rhs) {
  return lhs.offset == rhs.offset && lhs.size == rhs.size;
}

void BlobIndex::EncodeTo(std::string* dst) const {
  dst->push_back(kBlobRecord);
  PutVarint64(dst, file_number);
  blob_handle.EncodeTo(dst);
}

Status BlobIndex::DecodeFrom(Slice* src) {
  unsigned char type;
  if (!GetChar(src, &type) || type != kBlobRecord ||
      !GetVarint64(src, &file_number)) {
    return Status::Corruption("BlobIndex");
  }
  Status s = blob_handle.DecodeFrom(src);
  if (!s.ok()) {
    return Status::Corruption("BlobIndex", s.ToString());
  }
  return s;
}

bool operator==(const BlobIndex& lhs, const BlobIndex& rhs) {
  return (lhs.file_number == rhs.file_number &&
          lhs.blob_handle == rhs.blob_handle);
}

void BlobFileMeta::EncodeTo(std::string* dst) const {
  PutVarint64(dst, file_number);
  PutVarint64(dst, file_size);
}

Status BlobFileMeta::DecodeFrom(Slice* src) {
  if (!GetVarint64(src, &file_number) || !GetVarint64(src, &file_size)) {
    return Status::Corruption("BlobFileMeta");
  }
  return Status::OK();
}

bool operator==(const BlobFileMeta& lhs, const BlobFileMeta& rhs) {
  return (lhs.file_number == rhs.file_number && lhs.file_size == rhs.file_size);
}

void BlobFileFooter::EncodeTo(std::string* dst) const {
  auto size = dst->size();
  meta_index_handle.EncodeTo(dst);
  // Add padding to make a fixed size footer.
  dst->resize(size + kEncodedLength - 12);
  PutFixed64(dst, kMagicNumber);
  Slice encoded(dst->data() + size, dst->size() - size);
  PutFixed32(dst, crc32c::Value(encoded.data(), encoded.size()));
}

Status BlobFileFooter::DecodeFrom(Slice* src) {
  auto data = src->data();
  Status s = meta_index_handle.DecodeFrom(src);
  if (!s.ok()) {
    return Status::Corruption("BlobFileFooter", s.ToString());
  }
  // Remove padding.
  src->remove_prefix(data + kEncodedLength - 12 - src->data());
  uint64_t magic_number = 0;
  if (!GetFixed64(src, &magic_number) || magic_number != kMagicNumber) {
    return Status::Corruption("BlobFileFooter", "magic number");
  }
  Slice decoded(data, src->data() - data);
  uint32_t checksum = 0;
  if (!GetFixed32(src, &checksum) ||
      crc32c::Value(decoded.data(), decoded.size()) != checksum) {
    return Status::Corruption("BlobFileFooter", "checksum");
  }
  return Status::OK();
}

bool operator==(const BlobFileFooter& lhs, const BlobFileFooter& rhs) {
  return (lhs.meta_index_handle.offset() == rhs.meta_index_handle.offset() &&
          lhs.meta_index_handle.size() == rhs.meta_index_handle.size());
}

}  // namespace titandb
}  // namespace rocksdb
