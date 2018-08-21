#pragma once

#include "db/log_reader.h"
#include "db/log_writer.h"
#include "util/mutexlock.h"
#include "utilities/titandb/options.h"
#include "utilities/titandb/version.h"
#include "utilities/titandb/version_edit.h"

namespace rocksdb {
namespace titandb {

class VersionSet {
 public:
  VersionSet(const TitanDBOptions& options);

  // Sets up the storage specified in "options.dirname".
  // If the manifest doesn't exist, it will create one.
  // If the manifest exists, it will recover from the latest one.
  // It is a corruption if the persistent storage contains data
  // outside of the provided column families.
  Status Open(const std::map<uint32_t, TitanCFOptions>& column_families);

  // Applies *edit on the current version to form a new version that is
  // both saved to the manifest and installed as the new current version.
  // REQUIRES: *mutex is held
  Status LogAndApply(VersionEdit* edit, port::Mutex* mutex);

  // Adds some column families with the specified options.
  // REQUIRES: mutex is held
  void AddColumnFamilies(
      const std::map<uint32_t, TitanCFOptions>& column_families);
  // Drops some column families. The obsolete files will be deleted in
  // background when they will not be accessed anymore.
  // REQUIRES: mutex is held
  void DropColumnFamilies(const std::vector<uint32_t>& column_families);

  // Returns the current version.
  Version* current() { return versions_.current(); }

  // Allocates a new file number.
  uint64_t NewFileNumber() { return next_file_number_.fetch_add(1); }

 private:
  Status Recover();

  Status OpenManifest(uint64_t number);

  Status WriteSnapshot(log::Writer* log);

  std::string dirname_;
  Env* env_;
  EnvOptions env_options_;
  TitanDBOptions db_options_;
  std::shared_ptr<Cache> file_cache_;

  VersionList versions_;
  std::unique_ptr<log::Writer> manifest_;
  std::atomic<uint64_t> next_file_number_ {1};
};

}  // namespace titandb
}  // namespace rocksdb
