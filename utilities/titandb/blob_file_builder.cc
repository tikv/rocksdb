#include "utilities/titandb/blob_file_builder.h"

namespace rocksdb {
namespace titandb {

void BlobFileBuilder::Add(const BlobRecord& record, BlobHandle* handle) {
  if (!ok()) return;

  buffer_.clear();
  record.EncodeTo(&buffer_);
  handle->offset = file_->GetFileSize();
  handle->size = buffer_.size();
  status_ = file_->Append(buffer_);
}

Status BlobFileBuilder::Finish() {
  if (!ok()) return status();

  BlobFileFooter footer;
  footer.compression = options_.blob_file_compression;
  buffer_.clear();
  footer.EncodeTo(&buffer_);

  status_ = file_->Append(buffer_);
  if (ok()) {
    status_ = file_->Flush();
  }
  return status();
}

void BlobFileBuilder::Abandon() {}

}  // namespace titandb
}  // namespace rocksdb
