#pragma once

#include "rocksdb/options.h"
#include "utilities/titandb/options.h"
#include "utilities/titandb/blob_format.h"
#include "utilities/titandb/blob_file_reader.h"

namespace rocksdb {
namespace titandb {

class BlobFileCache {
 public:
  // Constructs a blob file cache to cache opened files.
  BlobFileCache(const DBOptions& db_options,
                const TitanDBOptions& tdb_options);

  // Gets the blob record pointed by the handle in the speficied file
  // number. The corresponding file size must be exactly "file_size"
  // bytes. The provided buffer is used to store the record data, so
  // the buffer must be valid when the record is used.
  Status Get(const ReadOptions& options,
             uint64_t file_number,
             uint64_t file_size,
             const BlobHandle& handle,
             BlobRecord* record, std::string* buffer);

  // Creates a new blob file reader for the specified file number. The
  // corresponding file size must be exactly "file_size" bytes.
  // If successful, sets "*result" to the new blob file reader.
  Status NewReader(const ReadOptions& options,
                   uint64_t file_number,
                   uint64_t file_size,
                   std::unique_ptr<BlobFileReader>* result);

  // Evicts the file cache for the specified file number.
  void Evict(uint64_t file_number);

 private:
  // Finds the file for the specified file number. Opens the file if
  // the file is not found in the cache and caches it.
  // If successful, sets "*handle" to the cached file.
  Status FindFile(uint64_t file_number,
                  uint64_t file_size,
                  Cache::Handle** handle);

  Status NewRandomAccessReader(uint64_t file_number,
                               uint64_t readahead_size,
                               std::unique_ptr<RandomAccessFileReader>* result);

  DBOptions db_options_;
  TitanDBOptions tdb_options_;
  Env* env_;
  EnvOptions env_options_;
  std::shared_ptr<Cache> cache_;
};

}  // namespace titandb
}  // namespace rocksdb
