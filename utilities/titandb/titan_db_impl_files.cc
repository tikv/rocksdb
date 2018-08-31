//
// Created by 郑志铨 on 2018/8/29.
//

#include "utilities/titandb/titan_db_impl.h"

namespace rocksdb {
namespace titandb {

void TitanDBImpl::PurgeObsoleteFiles() {
  Status s;
  ObsoleteFiles obsolete_files;
  vset_->GetObsoleteFiles(&obsolete_files);

  {
    mutex_.Unlock();
    std::vector<std::string> candidate_files;
    for (auto& blob_file : obsolete_files.blob_files) {
      candidate_files.emplace_back(
          BlobFileName(db_options_.dirname, blob_file->file_number));
    }
    for (auto& manifest : obsolete_files.manifests) {
      candidate_files.emplace_back(std::move(manifest));
    }

    // dedup state.candidate_files so we don't try to delete the same
    // file twice
    std::sort(candidate_files.begin(), candidate_files.end());
    candidate_files.erase(
        std::unique(candidate_files.begin(), candidate_files.end()),
        candidate_files.end());

    for (const auto& candidate_file : candidate_files) {
      s = env_->DeleteFile(candidate_file);
      assert(s.ok());
    }
    mutex_.Lock();
  }
}

}  // namespace titandb
}  // namespace rocksdb
