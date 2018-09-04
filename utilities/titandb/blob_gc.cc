#include "utilities/titandb/blob_gc.h"

namespace rocksdb {
namespace titandb {

BlobGC::BlobGC(std::vector<BlobFileMeta*>&& blob_files)
    : candidate_files_(std::move(blob_files)) {}

BlobGC::~BlobGC() {}

}  // namespace titandb
}  // namespace rocksdb
