#include "utilities/titandb/blob_file_builder.h"

#include "util/coding.h"
#include "util/crc32c.h"
#include "utilities/titandb/util.h"

namespace rocksdb {
namespace titandb {

// HEADER:  8 bytes length
// BODY:    variable length
// TAIL:    5 bytes length
void BlobFileBuilder::Add(const BlobRecord& record, BlobHandle* handle) {
  if (!ok()) return;

  buffer_.clear();
  assert(!record.key.empty());
  assert(!record.value.empty());
  record.EncodeTo(&buffer_);

  CompressionType compression = options_.blob_file_compression;
  auto output = Compress(&compression, buffer_, &compressed_buffer_);

  uint64_t body_length = output.size();
  status_ = file_->Append(
      Slice{reinterpret_cast<const char*>(&body_length), kBlobHeaderSize});
  if (!ok()) return;

  handle->offset = file_->GetFileSize();
  handle->size = output.size();

  status_ = file_->Append(output);
  if (ok()) {
    char tailer[kBlobTailerSize];
    tailer[0] = compression;
    EncodeFixed32(tailer+1, crc32c::Value(output.data(), output.size()));
    status_ = file_->Append(Slice(tailer, sizeof(tailer)));
  }
}

Status BlobFileBuilder::Finish() {
  if (!ok()) return status();

  BlobFileFooter footer;
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
