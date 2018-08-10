#pragma once

#include <set>

#include "rocksdb/slice.h"
#include "utilities/titandb/blob_format.h"

namespace rocksdb {
namespace titandb {

class VersionEdit {
 public:
  void SetNextFileNumber(uint64_t v) {
    has_next_file_number_ = true;
    next_file_number_ = v;
  }

  void AddBlobFile(const BlobFileMeta& file) {
    auto it = added_files_.emplace(file.file_number, file);
    if (!it.second) abort();
  }

  void DeleteBlobFile(uint64_t file_number) {
    auto it = deleted_files_.emplace(file_number);
    if (!it.second) abort();
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* src);

  friend bool operator==(const VersionEdit& lhs, const VersionEdit& rhs);

 private:
  friend class VersionSet;
  friend class VersionBuilder;

  bool has_next_file_number_ {false};
  uint64_t next_file_number_ {0};

  std::map<uint64_t, BlobFileMeta> added_files_;
  std::set<uint64_t> deleted_files_;
};

}  // namespace titandb
}  // namespace rocksdb
