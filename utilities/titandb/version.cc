#include "utilities/titandb/version.h"
#include "utilities/titandb/version_set.h"

namespace rocksdb {
namespace titandb {

Status BlobStorage::Get(const ReadOptions& options, const BlobIndex& index,
                        BlobRecord* record, PinnableSlice* buffer) {
  std::shared_ptr<BlobFileMeta> file;
  Status s = FindFile(index.file_number, &file);
  if (!s.ok()) return s;
  return file_cache_->Get(options, file->file_number, file->file_size,
                          index.blob_handle, record, buffer);
}

Status BlobStorage::NewPrefetcher(
                              uint64_t file_number,
                              std::unique_ptr<BlobFilePrefetcher>* result) {
  std::shared_ptr< BlobFileMeta> file;
  Status s = FindFile(file_number, &file);
  if (!s.ok()) return s;
  return file_cache_->NewPrefetcher( file->file_number, file->file_size, result);
}

Status BlobStorage::FindFile(uint64_t file_number,
                             std::shared_ptr<BlobFileMeta>* file) {
  auto it = files_.find(file_number);
  if (it != files_.end()) {
    *file = it->second;
    return Status::OK();
  }
  auto number = std::to_string(file_number);
  return Status::Corruption("missing blob file " + number);
}

void BlobStorage::ComputeGCScore() {
  gc_score_.clear();
  for (auto& file : files_) {
    gc_score_.push_back({});
    auto& score = gc_score_.back();
    score.file_number = file.first;
    if (file.second->marked_for_gc) {
      score.score = 1;
      file.second->marked_for_gc = false;
    } else if (file.second->file_size < 8 << 20) {
      // TODO configurable
      score.score = 1;
      file.second->marked_for_sample = false;
    } else {
      score.score = file.second->discardable_size / file.second->file_size;
      if (score.score >= 0.5) file.second->marked_for_sample = false;
    }
  }

  std::sort(gc_score_.begin(), gc_score_.end(),
            [](const GCScore& first, const GCScore& second) {
              return first.score > second.score;
            });
}

Version::~Version() {
  assert(refs_ == 0);

  // Remove linked list
  prev_->next_ = next_;
  next_->prev_ = prev_;

  // Drop references to files
  for (auto& column_family : column_families_) {
    // Someone else holds this storage
    if (column_family.second.use_count() > 1) continue;
    auto& blob_storage = *column_family.second;
    for (size_t i = 0; i < blob_storage.files_.size(); i++) {
      if (blob_storage.files_[i].use_count() > 1) continue;
      vset_->obsolete_files_.blob_files.emplace_back(
          std::move(blob_storage.files_[i]));
    }
  }
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

std::shared_ptr<BlobStorage> Version::GetBlobStorage(uint32_t cf_id) {
  auto it = column_families_.find(cf_id);
  if (it != column_families_.end()) {
    return it->second;
  }
  return nullptr;
}

VersionList::VersionList() { Append(new Version(nullptr)); }

VersionList::~VersionList() {
  current_->Unref();
  assert(list_.prev_ == &list_);
  assert(list_.next_ == &list_);
}

void VersionList::Append(Version* v) {
  assert(v->refs_ == 0);
  assert(v != current_);

  if (current_) {
    current_->Unref();
  }
  current_ = v;
  current_->Ref();

  v->prev_ = list_.prev_;
  v->next_ = &list_;
  v->prev_->next_ = v;
  v->next_->prev_ = v;
}

}  // namespace titandb
}  // namespace rocksdb
