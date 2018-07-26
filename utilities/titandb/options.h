#pragma once

#include "rocksdb/options.h"

namespace rocksdb {
namespace titandb {

struct TitanDBOptions {
  // Default: {dbname}/titandb
  std::string dirname;

  // The smallest value to store in blob files. Value smaller than
  // this threshold will be inlined in base DB.
  //
  // Default: 4096
  uint64_t min_blob_size {4096};

  uint64_t max_open_files {1 << 20};

  CompressionType blob_file_compression {kNoCompression};

  uint64_t blob_file_target_size {256 << 20};

  std::string ToString() const;
};

}  // namespace titandb
}  // namespace rocksdb
