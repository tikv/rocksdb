#include "utilities/titandb/blob_file_iterator.h"

#include "utilities/titandb/util.h"

#include "util/crc32c.h"

namespace rocksdb {
namespace titandb {

BlobFileIterator::BlobFileIterator(
    std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_name,
    uint64_t file_size, const TitanCFOptions& titan_cf_options)
    : file_(std::move(file)),
      file_number_(file_name),
      file_size_(file_size),
      titan_cf_options_(titan_cf_options) {}

BlobFileIterator::~BlobFileIterator() {}

bool BlobFileIterator::Init() {
  char buf[BlobFileFooter::kEncodedLength];
  Slice slice;
  status_ = file_->Read(file_size_ - BlobFileFooter::kEncodedLength,
                        BlobFileFooter::kEncodedLength, &slice, buf);
  if (!status_.ok()) return false;
  BlobFileFooter blob_file_footer;
  blob_file_footer.DecodeFrom(&slice);
  total_blocks_size_ = file_size_ - BlobFileFooter::kEncodedLength -
                       blob_file_footer.meta_index_handle.size();
  assert(total_blocks_size_ > 0);
  init_ = true;
  return true;
}

void BlobFileIterator::SeekToFirst() {
  if (!init_) Init();
  iterate_offset_ = 0;
  PrefetchAndGet();
}

bool BlobFileIterator::Valid() const { return valid_; }

void BlobFileIterator::Next() {
  assert(init_);
  PrefetchAndGet();
}

Slice BlobFileIterator::key() const { return cur_blob_record_.key; }

Slice BlobFileIterator::value() const { return cur_blob_record_.value; }

void BlobFileIterator::IterateForPrev(uint64_t offset) {
  if (!init_) Init();

  if (offset >= total_blocks_size_) {
    iterate_offset_ = offset;
    status_ = Status::InvalidArgument("Out of bound");
    return;
  }

  Slice slice;
  uint64_t body_length;
  uint64_t total_length;
  for (iterate_offset_ = 0; iterate_offset_ < offset;
       iterate_offset_ += total_length) {
    Status s = file_->Read(iterate_offset_, kBlobHeaderSize, &slice,
                           reinterpret_cast<char*>(&body_length));
    if (!s.ok()) {
      status_ = s;
      return;
    }
    total_length = kBlobHeaderSize + body_length + kBlobTailerSize;
  }

  if (iterate_offset_ > offset) iterate_offset_ -= total_length;
  valid_ = false;
}

void BlobFileIterator::GetBlobRecord() {
  // read header
  Slice slice;
  uint64_t body_length;
  status_ = file_->Read(iterate_offset_, kBlobHeaderSize, &slice,
                        reinterpret_cast<char*>(&body_length));
  if (!status_.ok()) return;
  body_length = *reinterpret_cast<const uint64_t*>(slice.data());
  assert(body_length > 0);
  iterate_offset_ += kBlobHeaderSize;

  // read body and tailer
  uint64_t left_size = body_length + kBlobTailerSize;
  buffer_.reserve(left_size);
  status_ = file_->Read(iterate_offset_, left_size, &slice, buffer_.data());
  if (!status_.ok()) return;

  // parse body and tailer
  auto tailer = buffer_.data() + body_length;
  auto checksum = DecodeFixed32(tailer + 1);
  if (crc32c::Value(buffer_.data(), body_length) != checksum) {
    status_ = Status::Corruption("BlobRecord", "checksum");
    return;
  }
  auto compression = static_cast<CompressionType>(*tailer);
  std::unique_ptr<char[]> uncompressed;
  if (compression == kNoCompression) {
    slice = {buffer_.data(), body_length};
  } else {
    UncompressionContext ctx(compression);
    status_ =
        Uncompress(ctx, {buffer_.data(), body_length}, &slice, &uncompressed);
    if (!status_.ok()) return;
  }
  status_ = DecodeInto(slice, &cur_blob_record_);
  if (!status_.ok()) return;

  cur_record_offset_ = iterate_offset_;
  cur_record_size_ = body_length;
  iterate_offset_ += left_size;
  valid_ = true;
}

void BlobFileIterator::PrefetchAndGet() {
  if (iterate_offset_ >= total_blocks_size_) {
    valid_ = false;
    return;
  }

  if (readahead_begin_offset_ > iterate_offset_ ||
      readahead_end_offset_ < iterate_offset_) {
    // alignment
    readahead_begin_offset_ =
        iterate_offset_ - (iterate_offset_ & (kDefaultPageSize - 1));
    readahead_end_offset_ = readahead_begin_offset_;
    readahead_size_ = kMinReadaheadSize;
  }
  auto min_blob_size =
      iterate_offset_ + kBlobFixedSize + titan_cf_options_.min_blob_size;
  if (readahead_end_offset_ <= min_blob_size) {
    while (readahead_end_offset_ + readahead_size_ <= min_blob_size &&
           readahead_size_ < kMaxReadaheadSize)
      readahead_size_ <<= 1;
    file_->Prefetch(readahead_end_offset_, readahead_size_);
    readahead_end_offset_ += readahead_size_;
    readahead_size_ = std::min(kMaxReadaheadSize, readahead_size_ << 1);
  }

  GetBlobRecord();

  if (readahead_end_offset_ < iterate_offset_) {
    readahead_end_offset_ = iterate_offset_;
  }
}

BlobFileMergeIterator::BlobFileMergeIterator(
    std::vector<std::unique_ptr<BlobFileIterator>>&& blob_file_iterators)
    : blob_file_iterators_(std::move(blob_file_iterators)) {}

bool BlobFileMergeIterator::Valid() const {
  if (current_ == nullptr) return false;
  return current_->Valid();
}

void BlobFileMergeIterator::SeekToFirst() {
  for (auto& iter : blob_file_iterators_) {
    iter->SeekToFirst();
    if (iter->status().ok() && iter->Valid()) min_heap_.push(iter.get());
  }
  if (!min_heap_.empty()) {
    current_ = min_heap_.top();
    min_heap_.pop();
  } else {
    status_ = Status::Aborted("No iterator is valid");
  }
}

void BlobFileMergeIterator::Next() {
  assert(current_ != nullptr);
  current_->Next();
  if (current_->status().ok() && current_->Valid()) min_heap_.push(current_);
  current_ = min_heap_.top();
  min_heap_.pop();
}

Slice BlobFileMergeIterator::key() const {
  assert(current_ != nullptr);
  return current_->key();
}

Slice BlobFileMergeIterator::value() const {
  assert(current_ != nullptr);
  return current_->value();
}

}  // namespace titandb
}  // namespace rocksdb
