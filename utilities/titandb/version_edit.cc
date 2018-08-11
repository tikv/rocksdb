#include "utilities/titandb/version_edit.h"

#include "util/coding.h"

namespace rocksdb {
namespace titandb {

enum Tag {
  kNextFileNumber = 1,
  kColumnFamilyID = 10,
  kAddedBlobFile = 11,
  kDeletedBlobFile = 12,
};

void VersionEdit::EncodeTo(std::string* dst) const {
  if (has_next_file_number_) {
    PutVarint32Varint64(dst, kNextFileNumber, next_file_number_);
  }

  PutVarint32Varint32(dst, kColumnFamilyID, column_family_id_);

  for (auto& file : added_files_) {
    PutVarint32(dst, kAddedBlobFile);
    file.EncodeTo(dst);
  }
  for (auto& file : deleted_files_) {
    PutVarint32Varint64(dst, kDeletedBlobFile, file);
  }
}

Status VersionEdit::DecodeFrom(Slice* src) {
  uint32_t tag;
  uint64_t file_number;
  BlobFileMeta blob_file;

  const char* error = nullptr;
  while (!error && !src->empty()) {
    if (!GetVarint32(src, &tag)) {
      error = "invalid tag";
      break;
    }
    switch (tag) {
      case kNextFileNumber:
        if (GetVarint64(src, &next_file_number_)) {
          has_next_file_number_ = true;
        } else {
          error = "next file number";
        }
        break;
      case kColumnFamilyID:
        if (GetVarint32(src, &column_family_id_)) {
        } else {
          error = "column family id";
        }
        break;
      case kAddedBlobFile:
        if (blob_file.DecodeFrom(src).ok()) {
          AddBlobFile(blob_file);
        } else {
          error = "added blob file";
        }
        break;
      case kDeletedBlobFile:
        if (GetVarint64(src, &file_number)) {
          DeleteBlobFile(file_number);
        } else {
          error = "deleted blob file";
        }
        break;
      default:
        error = "unknown tag";
        break;
    }
  }

  if (error) {
    return Status::Corruption("VersionEdit", error);
  }
  return Status::OK();
}

bool operator==(const VersionEdit& lhs, const VersionEdit& rhs) {
  return (lhs.has_next_file_number_ == rhs.has_next_file_number_ &&
          lhs.next_file_number_ == rhs.next_file_number_ &&
          lhs.column_family_id_ == rhs.column_family_id_ &&
          lhs.added_files_ == rhs.added_files_ &&
          lhs.deleted_files_ == rhs.deleted_files_);
}

}  // namespace titandb
}  // namespace rocksdb
