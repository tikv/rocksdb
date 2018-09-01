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

  const std::vector<std::shared_ptr<BlobFileMeta>>& candidate_files() {
    return candidate_files_;
  }

  void set_selected_files(std::vector<std::shared_ptr<BlobFileMeta>>&& files) {
    selected_files_ = std::move(files);
  }

  const std::vector<std::shared_ptr<BlobFileMeta>>& selected_files() {
    return selected_files_;
  }

 private:
  std::vector<std::shared_ptr<BlobFileMeta>> candidate_files_;
  std::vector<std::shared_ptr<BlobFileMeta>> selected_files_;
};

struct GCScore {
  uint64_t file_number;
  double score;
};

}  // namespace titandb
}  // namespace rocksdb

#endif  // ROCKSDB_BLOB_GC_H
