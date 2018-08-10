#pragma once

#include "util/file_reader_writer.h"
#include "utilities/titandb/options.h"
#include "utilities/titandb/blob_format.h"

namespace rocksdb {
namespace titandb {

// Represents the information of a blob file read from the file.
class BlobFile {
 public:
  const BlobFileFooter& footer() const { return footer_; }

 private:
  friend class BlobFileReader;

  BlobFile() = default;

  BlobFileFooter footer_;
};

class BlobFileReader {
 public:
  // Opens a blob file and read the necessary metadata from it.
  // If successful, sets "*result" to the newly opened file reader.
  static Status Open(const TitanDBOptions& options,
                     std::unique_ptr<RandomAccessFileReader> file,
                     uint64_t file_size,
                     std::unique_ptr<BlobFileReader>* result);

  // Constructs a reader with the shared blob file. The provided blob
  // file must be corresponding to the "file".
  BlobFileReader(const TitanDBOptions& options,
                 std::shared_ptr<BlobFile> _blob_file,
                 std::unique_ptr<RandomAccessFileReader> file)
      : options_(options),
        blob_file_(_blob_file),
        file_(std::move(file)) {}

  // Gets the blob record pointed by the handle in this file. The data
  // of the record is stored in the provided buffer, so the buffer
  // must be valid when the record is used.
  Status Get(const ReadOptions& options,
             const BlobHandle& handle,
             BlobRecord* record, std::string* buffer);

  // Returns a shared reference to the blob file.
  std::shared_ptr<BlobFile> blob_file() const { return blob_file_; }

 private:
  TitanDBOptions options_;
  std::shared_ptr<BlobFile> blob_file_;
  std::unique_ptr<RandomAccessFileReader> file_;
};

}  // namespace titandb
}  // namespace rocksdb
