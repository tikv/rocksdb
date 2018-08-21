#include "utilities/titandb/blob_file_reader.h"

namespace rocksdb {
namespace titandb {

Status BlobFileReader::Open(const TitanCFOptions& options,
                            std::unique_ptr<RandomAccessFileReader> file,
                            uint64_t file_size,
                            std::unique_ptr<BlobFileReader>* result) {
  if (file_size < BlobFileFooter::kEncodedLength) {
    return Status::Corruption("file is too short to be a blob file");
  }

  char footer_space[BlobFileFooter::kEncodedLength];
  Slice footer_input;
  Status s = file->Read(file_size - BlobFileFooter::kEncodedLength,
                        BlobFileFooter::kEncodedLength,
                        &footer_input, footer_space);
  if (!s.ok()) return s;

  std::shared_ptr<BlobFile> blob_file(new BlobFile);
  s = DecodeInto(footer_input, &blob_file->footer_);
  if (!s.ok()) return s;

  result->reset(new BlobFileReader(options, blob_file, std::move(file)));
  return s;
}

Status BlobFileReader::Get(const ReadOptions& /*options*/,
                           const BlobHandle& handle,
                           BlobRecord* record, std::string* buffer) {
  Slice blob;
  buffer->resize(handle.size);
  Status s = file_->Read(handle.offset, handle.size, &blob, &(*buffer)[0]);
  if (!s.ok()) return s;
  return DecodeInto(blob, record);
}

}  // namespace titandb
}  // namespace rocksdb
