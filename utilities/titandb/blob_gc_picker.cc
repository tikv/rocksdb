//
// Created by 郑志铨 on 2018/8/21.
//

#include "utilities/titandb/blob_gc_picker.h"

namespace rocksdb {
namespace titandb {

BasicBlobGCPicker::BasicBlobGCPicker() {}

BasicBlobGCPicker::~BasicBlobGCPicker() {}

std::unique_ptr<BlobGC> BasicBlobGCPicker::PickBlobGC(
    BlobStorage* blob_storage) {
  std::vector<std::shared_ptr<BlobFileMeta>> blob_files;
  std::shared_ptr<BlobFileMeta> blob_file_meta = nullptr;
  uint64_t total_file_size = 0;
  for (auto& gc_score : blob_storage->gc_score()) {
    // TODO check return value
    blob_storage->FindFile(gc_score.file_number, &blob_file_meta);
    if (blob_file_meta->being_gc) continue;
    blob_file_meta->being_gc = true;
    blob_files.push_back(blob_file_meta);
    total_file_size += blob_file_meta->file_size;
    // TODO configurable
    if (total_file_size >= 2U << 30) break;
  }
  if (blob_files.empty()) return nullptr;
  std::unique_ptr<BlobGC> blob_gc(new BlobGC(std::move(blob_files)));
  return blob_gc;
}

}  // namespace titandb
}  // namespace rocksdb
