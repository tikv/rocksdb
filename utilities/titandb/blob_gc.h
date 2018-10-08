#pragma once

#include <memory>

#include "utilities/titandb/blob_format.h"
#include "utilities/titandb/options.h"

namespace rocksdb {
namespace titandb {

// A BlobGC encapsulates information about a blob gc.
class BlobGC {
 public:
  BlobGC(std::vector<BlobFileMeta*>&& blob_files,
         TitanCFOptions&& _titan_cf_options);
  ~BlobGC();

  const std::vector<BlobFileMeta*>& candidate_files() {
    return candidate_files_;
  }

  void set_selected_files(std::vector<BlobFileMeta*>&& files) {
    selected_files_ = std::move(files);
  }

  const std::vector<BlobFileMeta*>& selected_files() { return selected_files_; }

  void ClearSelectedFiles() { selected_files_.clear(); }

  const TitanCFOptions& titan_cf_options() { return titan_cf_options_; }

 private:
  std::vector<BlobFileMeta*> candidate_files_;
  std::vector<BlobFileMeta*> selected_files_;
  TitanCFOptions titan_cf_options_;
};

struct GCScore {
  uint64_t file_number;
  double score;
};

}  // namespace titandb
}  // namespace rocksdb
