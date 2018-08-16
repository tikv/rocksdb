#pragma once

#include "util/file_reader_writer.h"
#include "utilities/titandb/blob_format.h"

namespace rocksdb {
namespace titandb {

// Contains information to complete a blob file creation.
class BlobFileHandle {
 public:
  virtual ~BlobFileHandle() {}

  virtual uint64_t GetNumber() const = 0;

  virtual const std::string& GetName() const = 0;

  virtual WritableFileWriter* GetFile() const = 0;
};

// Manages the process of blob files creation.
class BlobFileManager {
 public:
  virtual ~BlobFileManager() {}

  // Creates a new file. The new file should not be accessed until
  // FinishFile() has been called.
  // If successful, sets "*handle* to the new file handle.
  virtual Status NewFile(std::unique_ptr<BlobFileHandle>* handle) = 0;

  // Finishes the file with the provided metadata. Stops writting to
  // the file anymore.
  // REQUIRES: FinishFile(), DeleteFile() have not been called.
  virtual Status FinishFile(uint32_t cf_id,
                            const BlobFileMeta& file,
                            std::unique_ptr<BlobFileHandle> handle) = 0;

  // Deletes the file. If the caller is not going to call
  // FinishFile(), it must call DeleteFile() to release the handle.
  // REQUIRES: FinishFile(), DeleteFile() have not been called.
  virtual Status DeleteFile(std::unique_ptr<BlobFileHandle> handle) = 0;
};

}  // namespace titandb
}  // namespace rocksdb
