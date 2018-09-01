#ifndef ROCKSDB_BLOB_FILE_ITERATOR_H
#define ROCKSDB_BLOB_FILE_ITERATOR_H

#include <stdint.h>
#include "include/rocksdb/slice.h"
#include "include/rocksdb/status.h"
#include "utilities/titandb/options.h"
#include "table/internal_iterator.h"
#include "util/file_reader_writer.h"
#include "utilities/titandb/blob_format.h"

namespace rocksdb {
namespace titandb {

class BlobFileIterator final : public InternalIterator {
 public:
  static const std::string PROPERTIES_FILE_NUMBER;
  static const std::string PROPERTIES_BLOB_OFFSET;
  static const std::string PROPERTIES_BLOB_SIZE;

  const uint64_t kMinReadaheadSize = 4 << 10;
  const uint64_t kMaxReadaheadSize = 256 << 10;

  static Status GetBlobIndex(InternalIterator* iter, BlobIndex* blob_index);

  BlobFileIterator(std::unique_ptr<RandomAccessFileReader>&& file,
                   uint64_t file_name, uint64_t file_size,
                   const TitanCFOptions& titan_cf_options);
  ~BlobFileIterator() override;

  bool Init();
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

  void Iterate(uint64_t) { assert(false); }
  void IterateForPrev(uint64_t);

 private:
  // Blob file info
  const std::unique_ptr<RandomAccessFileReader> file_;
  const uint64_t file_number_;
  const uint64_t file_size_;
  TitanCFOptions titan_cf_options_;

  bool init_{false};
  uint64_t total_blocks_size_{0};

  // Iterator status
  Status status_;
  bool valid_{false};

  uint64_t iterate_offset_{0};
  std::vector<char> buffer_;
  BlobRecord cur_blob_record_;
  uint64_t cur_record_offset_;
  uint64_t cur_record_size_;

  uint64_t readahead_begin_offset_{0};
  uint64_t readahead_end_offset_{0};
  uint64_t readahead_size_{kMinReadaheadSize};

  void PrefetchAndGet();
  void GetBlobRecord();
};

}  // namespace titandb
}  // namespace rocksdb

#endif  // ROCKSDB_BLOB_FILE_ITERATOR_H
