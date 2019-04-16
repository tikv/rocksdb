#include "utilities/titandb/db_impl.h"
#include "util/testharness.h"

namespace rocksdb {
namespace titandb {

void TitanDBImpl::PurgeObsoleteFiles() {
  Status s;
  ObsoleteFiles obsolete_files;
  {
    MutexLock l(&mutex_);
    auto oldest_sequence = GetOldestSnapshotSequence();
    vset_->GetObsoleteFiles(&obsolete_files, oldest_sequence);
  }

  {
    std::vector<std::string> candidate_files;
    for (auto& blob_file : obsolete_files.blob_files) {
      candidate_files.emplace_back(
          BlobFileName(db_options_.dirname, blob_file.first));
    }
    for (auto& manifest : obsolete_files.manifests) {
      candidate_files.emplace_back(std::move(manifest));
    }

    // dedup state.inputs so we don't try to delete the same
    // file twice
    std::sort(candidate_files.begin(), candidate_files.end());
    candidate_files.erase(
        std::unique(candidate_files.begin(), candidate_files.end()),
        candidate_files.end());

    for (const auto& candidate_file : candidate_files) {
      ROCKS_LOG_INFO(db_options_.info_log, "Titan deleting obsolete file [%s]",
                     candidate_file.c_str());
      s = env_->DeleteFile(candidate_file);
      if (!s.ok()) {
        fprintf(stderr, "Titan deleting file [%s] failed, status:%s",
                candidate_file.c_str(), s.ToString().c_str());
        abort();
      }
    }
  }
}

}  // namespace titandb
}  // namespace rocksdb
