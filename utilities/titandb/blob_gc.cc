#include "utilities/titandb/blob_gc.h"

namespace rocksdb {
namespace titandb {

BlobGC::BlobGC(std::vector<BlobFileMeta*>&& blob_files,
               TitanCFOptions&& _titan_cf_options)
    : candidate_files_(std::move(blob_files)),
      titan_cf_options_(std::move(_titan_cf_options)) {}

BlobGC::~BlobGC() {}

}  // namespace titandb
}  // namespace rocksdb
