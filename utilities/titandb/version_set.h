#pragma once

#include "rocksdb/options.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "util/mutexlock.h"
#include "utilities/titandb/options.h"
#include "utilities/titandb/version.h"
#include "utilities/titandb/version_edit.h"
#include "utilities/titandb/version_builder.h"
#include "utilities/titandb/blob_file_cache.h"

namespace rocksdb {
namespace titandb {

class VersionSet {
 public:
  VersionSet(const DBOptions& db_options, const TitanDBOptions& tdb_options);

  ~VersionSet();

  // Sets up the storage specified in "tdb_options.dirname".
  // If the manifest doesn't exist, it will create one.
  // If the manifest exists, it will recover from the lastest one.
  Status Open();

  // Applies *edit on the current version to form a new version that is
  // both saved to the manifest and installed as the new current version.
  // REQUIRES: *mutex is held
  Status LogAndApply(VersionEdit* edit, port::Mutex* mutex);

  // Returns the current version.
  Version* current() { return current_; }

  // Allocates a new file number.
  uint64_t NewFileNumber();

 private:
  friend class Version;

  Status Recover();

  Status OpenManifest(uint64_t number);

  Status WriteSnapshot(log::Writer* log);

  void AppendVersion(Version* v);

  std::string dirname_;
  Env* env_;
  DBOptions db_options_;
  EnvOptions env_options_;
  TitanDBOptions tdb_options_;

  Version version_list_;
  Version* current_ {nullptr};

  std::atomic<uint64_t> next_file_number_ {1};
  std::unique_ptr<log::Writer> manifest_log_;

  std::unique_ptr<BlobFileCache> file_cache_;
};

}  // namespace titandb
}  // namespace rocksdb
