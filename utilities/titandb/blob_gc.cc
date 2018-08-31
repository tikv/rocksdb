//
// Created by 郑志铨 on 2018/8/8.
//

#include "utilities/titandb/blob_gc.h"

namespace rocksdb {
namespace titandb {

BlobGC::BlobGC(std::vector<std::shared_ptr<BlobFileMeta>>&& blob_files)
    : candidate_files_(std::move(blob_files)) {}

BlobGC::~BlobGC() {}

}  // namespace titandb
}  // namespace rocksdb
