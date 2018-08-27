//
// Created by 郑志铨 on 2018/8/8.
//

#ifndef ROCKSDB_BLOB_GC_H
#define ROCKSDB_BLOB_GC_H

#include <memory>

#include "utilities/titandb/blob_format.h"

namespace rocksdb {
namespace titandb {

// A BlobGC encapsulates information about a blob gc.
class BlobGC {
 public:
  BlobGC(std::vector<std::shared_ptr<BlobFileMeta>>&& blob_files);
  ~BlobGC();

  const std::vector<std::shared_ptr<BlobFileMeta>>& candidates() {
    return candidates_;
  }

  void set_selected(std::vector<std::shared_ptr<BlobFileMeta>>&& files) {
    selected_ = std::move(files);
  }

  const std::vector<std::shared_ptr<BlobFileMeta>>& selected() {
    return selected_;
  }

 private:
  std::vector<std::shared_ptr<BlobFileMeta>> candidates_;
  std::vector<std::shared_ptr<BlobFileMeta>> selected_;
};

}  // namespace titandb
}  // namespace rocksdb

#endif  // ROCKSDB_BLOB_GC_H
