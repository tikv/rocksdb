//
// Created by 郑志铨 on 2018/8/21.
//

#include "utilities/titandb/blob_gc_picker.h"

namespace rocksdb {
namespace titandb {

BasicBlobGCPicker::BasicBlobGCPicker(TitanCFOptions titan_cf_options)
    : titan_cf_options_(titan_cf_options) {}

BasicBlobGCPicker::~BasicBlobGCPicker() {}

std::unique_ptr<BlobGC> BasicBlobGCPicker::PickBlobGC(
    BlobStorage* blob_storage) {
  Status s;
  std::vector<std::shared_ptr<BlobFileMeta>> blob_files;
  std::shared_ptr<BlobFileMeta> blob_file = nullptr;

  uint64_t batch_size = 0;
  for (auto& gc_score : blob_storage->gc_score()) {
    s = blob_storage->FindFile(gc_score.file_number, &blob_file);
    assert(s.ok());

    if (!CheckForPick(blob_file, gc_score)) continue;
    MarkedForPick(blob_file);
    blob_files.push_back(blob_file);

    batch_size += blob_file->file_size;
    if (batch_size >= titan_cf_options_.blob_gc_batch_size) break;
  }

  if (blob_files.empty()) return nullptr;
  std::unique_ptr<BlobGC> blob_gc(new BlobGC(std::move(blob_files)));

  return blob_gc;
}

bool BasicBlobGCPicker::CheckForPick(
    const std::shared_ptr<rocksdb::titandb::BlobFileMeta>& blob_file,
    const GCScore& gc_score) const {
  if (blob_file->being_gc.load(std::memory_order_acquire))
    return false;
  if (gc_score.score >= titan_cf_options_.blob_file_discardable_ratio)
    blob_file->marked_for_sample = false;
  return true;
}

void BasicBlobGCPicker::MarkedForPick(
    std::shared_ptr<rocksdb::titandb::BlobFileMeta> blob_file) {
  blob_file->being_gc = true;
}

}  // namespace titandb
}  // namespace rocksdb
