#include "utilities/titandb/options.h"

#include <inttypes.h>

#include "rocksdb/convenience.h"

namespace rocksdb {
namespace titandb {

std::string TitanDBOptions::ToString() const {
  char buf[256];
  std::string str;
  std::string res = "[titandb]\n";
  snprintf(buf, sizeof(buf), "dirname = %s\n", dirname.c_str());
  res += buf;
  snprintf(buf, sizeof(buf), "min_blob_size = %" PRIu64 "\n", min_blob_size);
  res += buf;
  snprintf(buf, sizeof(buf), "max_open_files = %" PRIu64 "\n", max_open_files);
  res += buf;
  GetStringFromCompressionType(&str, blob_file_compression);
  snprintf(buf, sizeof(buf), "blob_file_compression = %s\n", str.c_str());
  res += buf;
  snprintf(buf, sizeof(buf), "blob_file_target_size = %" PRIu64 "\n",
           blob_file_target_size);
  res += buf;
  return res;
}

}  // namespace titandb
}  // namespace rocksdb
