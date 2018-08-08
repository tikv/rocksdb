#pragma once

#include "rocksdb/options.h"

namespace rocksdb {
namespace titandb {

struct TitanDBOptions {
  // The directory to store data specific to TitanDB alongside with
  // the base DB.
  //
  // Default: {dbname}/titandb
  std::string dirname;

  // The smallest value to store in blob files. Value smaller than
  // this threshold will be inlined in base DB.
  //
  // Default: 4096
  uint64_t min_blob_size {4096};

  // The maximum open blob files in the blob file cache.
  //
  // Default: 1 << 20
  uint64_t max_open_files {1 << 20};

  // The compression algorithm used to compress blob records in blob files.
  //
  // Default: kNoCompression
  CompressionType blob_file_compression {kNoCompression};

  // The desirable blob file size. This is not a hard limit but a wish.
  //
  // Default: 256MB
  uint64_t blob_file_target_size {256 << 20};

  std::string ToString() const;
};

}  // namespace titandb
}  // namespace rocksdb
