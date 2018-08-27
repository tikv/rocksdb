//
// Created by 郑志铨 on 2018/8/9.
//

#ifndef ROCKSDB_BLOB_FILE_ITERATOR_H
#define ROCKSDB_BLOB_FILE_ITERATOR_H

#include <stdint.h>
#include "include/rocksdb/slice.h"
#include "include/rocksdb/status.h"
#include "table/internal_iterator.h"
#include "util/file_reader_writer.h"
#include "utilities/titandb/blob_format.h"

namespace rocksdb {
namespace titandb {

class BlobFileIterator final : public InternalIterator {
 public:
  static const std::string PROPERTY_FILE_NAME;
  static const std::string PROPERTY_FILE_OFFSET;

  BlobFileIterator(std::unique_ptr<RandomAccessFileReader>&& file,
                   uint64_t file_name, uint64_t file_size);
  ~BlobFileIterator() override;

  bool Valid() const override;
  void SeekToFirst() override;
  void SeekToLast() override { assert(false); }
  void Seek(const Slice& /* target */) override { assert(false); }
  void SeekForPrev(const Slice& /* target */) override { assert(false); }
  void Next() override;
  void Prev() override { assert(false); }
  Slice key() const override;
  Slice value() const override;
  Status status() const override { return status_; }
  Status GetProperty(std::string prop_name, std::string* prop) override;

  void IterateForPrev(uint64_t offset);

 private:
  const std::unique_ptr<RandomAccessFileReader> file_;
  const uint64_t file_name_;
  const uint64_t file_size_;
  Status status_;
  uint64_t iterate_offset_ = 0;
  uint64_t iterate_size_ = 0;
  uint64_t blocks_size_ = 0;
  std::vector<char> buffer_;
  BlobRecord current_blob_record_;
  uint64_t current_blob_offset_;

  void GetOneBlock();
};

}  // namespace titandb
}  // namespace rocksdb

#endif  // ROCKSDB_BLOB_FILE_ITERATOR_H
