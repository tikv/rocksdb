#include "utilities/titandb/version_builder.h"

#include <inttypes.h>

namespace rocksdb {
namespace titandb {

void VersionBuilder::Builder::AddFile(const BlobFileMeta& file) {
  auto number = file.file_number;
  if (base_->files_.find(number) != base_->files_.end() ||
      added_files_.find(number) != added_files_.end()) {
    fprintf(stderr, "blob file %" PRIu64 " has been added before\n", number);
    abort();
  }
  if (deleted_files_.find(number) != deleted_files_.end()) {
    fprintf(stderr, "blob file %" PRIu64 " has been deleted before\n", number);
    abort();
  }
  added_files_.emplace(number, file);
}

void VersionBuilder::Builder::DeleteFile(uint64_t number) {
  if (base_->files_.find(number) == base_->files_.end() &&
      added_files_.find(number) == added_files_.end()) {
    fprintf(stderr, "blob file %" PRIu64 " doesn't exist before\n", number);
    abort();
  }
  if (deleted_files_.find(number) != deleted_files_.end()) {
    fprintf(stderr, "blob file %" PRIu64 " has been deleted before\n", number);
    abort();
  }
  deleted_files_.emplace(number);
}

std::shared_ptr<BlobStorage> VersionBuilder::Builder::Build() {
  // If nothing is changed, we can reuse the base;
  if (added_files_.empty() && deleted_files_.empty()) {
    return base_;
  }

  auto vs = std::make_shared<BlobStorage>(*base_);
  vs->files_.insert(added_files_.begin(), added_files_.end());
  for (auto& file : deleted_files_) {
    vs->files_.erase(file);
  }
  return vs;
}

VersionBuilder::VersionBuilder(Version* base) : base_(base) {
  base_->Ref();
  for (auto& it : base_->column_families_) {
    column_families_.emplace(it.first, Builder(it.second));
  }
}

VersionBuilder::~VersionBuilder() {
  base_->Unref();
}

void VersionBuilder::Apply(VersionEdit* edit) {
  auto cf_id = edit->column_family_id_;
  auto it = column_families_.find(cf_id);
  if (it == column_families_.end()) {
    fprintf(stderr, "missing column family %" PRIu32 "\n", cf_id);
    abort();
  }
  auto& builder = it->second;

  for (auto& file : edit->deleted_files_) {
    builder.DeleteFile(file);
  }
  for (auto& file : edit->added_files_) {
    builder.AddFile(file);
  }
}

void VersionBuilder::SaveTo(Version* v) {
  v->column_families_.clear();
  for (auto& it : column_families_) {
    v->column_families_.emplace(it.first, it.second.Build());
  }
}

}  // namespace titandb
}  // namespace rocksdb
