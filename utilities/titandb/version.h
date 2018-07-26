#pragma once

#include "rocksdb/options.h"
#include "utilities/titandb/blob_format.h"
#include "utilities/titandb/blob_file_cache.h"

namespace rocksdb {
namespace titandb {

class VersionSet;

class Version {
 public:
  Version(VersionSet* vset)
      : prev_(this),
        next_(this),
        vset_(vset) {}

  // Reference count management.
  // REQUIRES: lock is held
  void Ref();
  void Unref();

  // Gets the blob record for the index. The data of the record is
  // stored in the provided buffer, so the buffer must be valid when
  // the record is used.
  Status Get(const ReadOptions& options,
             const BlobIndex& index,
             BlobRecord* record, std::string* buffer);

  // Creates a new blob file reader for the specified file number.
  // If successful, sets "*result" to the new blob file reader.
  Status NewReader(const ReadOptions& options,
                   uint64_t file_number,
                   std::unique_ptr<BlobFileReader>* result);

 private:
  friend class VersionSet;
  friend class VersionTest;
  friend class VersionBuilder;

  ~Version();

  // Finds the file meta for the specified file number.
  // If successful, sets "*file" to the specified file meta.
  Status FindFile(uint64_t file_number,
                  const BlobFileMeta** file);

  int refs_ = 0;
  Version* prev_;
  Version* next_;
  VersionSet* vset_;
  std::map<uint64_t, BlobFileMeta> files_;
};

}  // namespace titandb
}  // namespace rocksdb
