#include "utilities/titandb/version.h"

#include "utilities/titandb/version_set.h"

namespace rocksdb {
namespace titandb {

Version::~Version() {
  assert(refs_ == 0);

  // Remove linked list
  prev_->next_ = next_;
  next_->prev_ = prev_;
}

void Version::Ref() {
  refs_++;
}

void Version::Unref() {
  refs_--;
  if (refs_ == 0) {
    delete this;
  }
}

Status Version::Get(const ReadOptions& options,
                    const BlobIndex& index,
                    BlobRecord* record, std::string* buffer) {
  const BlobFileMeta* file;
  Status s = FindFile(index.file_number, &file);
  if (!s.ok()) return s;
  return vset_->file_cache_->Get(
      options, file->file_number, file->file_size, index.blob_handle,
      record, buffer);
}

Status Version::NewReader(const ReadOptions& options,
                          uint64_t file_number,
                          std::unique_ptr<BlobFileReader>* result) {
  const BlobFileMeta* file;
  Status s = FindFile(file_number, &file);
  if (!s.ok()) return s;
  return vset_->file_cache_->NewReader(
      options, file->file_number, file->file_size, result);
}

Status Version::FindFile(uint64_t file_number,
                         const BlobFileMeta** file) {
  auto it = files_.find(file_number);
  if (it != files_.end()) {
    *file = &it->second;
    return Status::OK();
  }
  return Status::Corruption("missing blob file " + std::to_string(file_number));
}

}  // namespace titandb
}  // namespace rocksdb
