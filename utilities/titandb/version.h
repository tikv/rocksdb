#pragma once

#include "rocksdb/options.h"
#include "utilities/titandb/blob_format.h"
#include "utilities/titandb/blob_file_cache.h"

namespace rocksdb {
namespace titandb {

// Provides methods to access the blob storage for a specific
// version. The version must be valid when this storage is used.
class BlobStorage {
 public:
  BlobStorage(std::shared_ptr<BlobFileCache> file_cache)
      : file_cache_(file_cache) {}

  // Gets the blob record pointed by the blob index. The provided
  // buffer is used to store the record data, so the buffer must be
  // valid when the record is used.
  Status Get(const ReadOptions& options,
             const BlobIndex& index,
             BlobRecord* record, std::string* buffer);

  // Creates a new blob file reader for the specified file number.
  // If successful, sets "*result" to the new blob file reader.
  Status NewReader(const ReadOptions& options,
                   uint64_t file_number,
                   std::unique_ptr<BlobFileReader>* result);

  // Finds the blob file meta for the specified file number. It is a
  // corruption if the file doesn't exist in the specific version.
  Status FindFile(uint64_t file_number, const BlobFileMeta** file);

 private:
  friend class VersionSet;
  friend class VersionTest;
  friend class VersionBuilder;

  std::map<uint64_t, BlobFileMeta> files_;
  std::shared_ptr<BlobFileCache> file_cache_;
};

class Version {
 public:
  Version() : prev_(this), next_(this) {}

  // Reference count management.
  // REQUIRES: lock is held
  void Ref();
  void Unref();

  // Returns the blob storage for the specific column family.
  // The version must be valid when the blob storage is used.
  std::shared_ptr<BlobStorage> GetBlobStorage(uint32_t cf_id);

 private:
  friend class VersionSet;
  friend class VersionList;
  friend class VersionTest;
  friend class VersionBuilder;

  ~Version();

  int refs_ {0};
  Version* prev_;
  Version* next_;
  std::map<uint32_t, std::shared_ptr<BlobStorage>> column_families_;
};

class VersionList {
 public:
  VersionList();

  ~VersionList();

  Version* current() { return current_; }

  void Append(Version* v);

 private:
  Version list_;
  Version* current_ {nullptr};
};

}  // namespace titandb
}  // namespace rocksdb
