#include "utilities/titandb/version_builder.h"

#include <inttypes.h>

namespace rocksdb {
namespace titandb {

VersionBuilder::VersionBuilder(Version* base) : base_(base) {
  base_->Ref();
  added_files_ = base_->files_;
}

VersionBuilder::~VersionBuilder() {
  base_->Unref();
}

void VersionBuilder::Apply(VersionEdit* edit) {
  for (auto& file : edit->deleted_files_) {
    if (added_files_.find(file) == added_files_.end()) {
      fprintf(stderr, "deleted blob file %" PRIu64 " doesn't exist before\n",
              file);
      abort();
    }
    if (deleted_files_.find(file) != deleted_files_.end()) {
      fprintf(stderr,
              "deleted blob file %" PRIu64 " has been deleted before\n",
              file);
      abort();
    }
    deleted_files_.emplace(file);
  }

  for (auto& file : edit->added_files_) {
    if (added_files_.find(file.first) != added_files_.end()) {
      fprintf(stderr, "added blob file %" PRIu64 " has been added before\n",
              file.first);
      abort();
    }
    if (deleted_files_.find(file.first) != deleted_files_.end()) {
      fprintf(stderr, "added blob file %" PRIu64 " has been deleted before\n",
              file.first);
      abort();
    }
    added_files_.emplace(file);
  }
}

void VersionBuilder::SaveTo(Version* v) {
  v->files_.clear();
  for (auto& file : added_files_) {
    if (deleted_files_.find(file.first) == deleted_files_.end()) {
      v->files_.insert(file);
    }
  }
}

}  // namespace titandb
}  // namespace rocksdb
