//
// Created by 郑志铨 on 2018/8/9.
//

#include "utilities/titandb/blob_file_iterator.h"

#include "utilities/titandb/blob_format.h"
#include "utilities/titandb/util.h"

#include "util/crc32c.h"

namespace rocksdb {
namespace titandb {

const std::string BlobFileIterator::PROPERTIES_FILE_NUMBER =
    "PropertiesFileNumber";
const std::string BlobFileIterator::PROPERTIES_BLOB_OFFSET =
    "PropertiesBlobOffset";
const std::string BlobFileIterator::PROPERTIES_BLOB_SIZE = "PropertiesBlobSize";

BlobFileIterator::BlobFileIterator(
    std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_name,
    uint64_t file_size)
    : file_(std::move(file)), file_number_(file_name), file_size_(file_size) {}

BlobFileIterator::~BlobFileIterator() {}

Status BlobFileIterator::GetBlobIndex(InternalIterator* iter,
                                      BlobIndex* blob_index) {
  Status s;
  std::string prop;
  s = iter->GetProperty(BlobFileIterator::PROPERTIES_FILE_NUMBER, &prop);
  if (!s.ok()) return s;
  blob_index->file_number = *reinterpret_cast<const uint64_t*>(prop.data());
  s = iter->GetProperty(BlobFileIterator::PROPERTIES_BLOB_OFFSET, &prop);
  if (!s.ok()) return s;
  blob_index->blob_handle.offset =
      *reinterpret_cast<const uint64_t*>(prop.data());
  s = iter->GetProperty(BlobFileIterator::PROPERTIES_BLOB_SIZE, &prop);
  if (!s.ok()) return s;
  blob_index->blob_handle.size =
      *reinterpret_cast<const uint64_t*>(prop.data());

  return Status::OK();
}

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

  GetOneBlobRecord();
}

bool BlobFileIterator::Valid() const { return valid_; }

void BlobFileIterator::Next() {
  assert(init_);
  GetOneBlobRecord();
}

Slice BlobFileIterator::key() const { return cur_blob_record_.key; }

Slice BlobFileIterator::value() const { return cur_blob_record_.value; }

Status BlobFileIterator::GetProperty(std::string prop_name, std::string* prop) {
  assert(Valid());

  prop->clear();

  if (prop_name == PROPERTIES_FILE_NUMBER) {
    prop->assign(reinterpret_cast<const char*>(&file_number_),
                 sizeof(file_number_));
  } else if (prop_name == PROPERTIES_BLOB_OFFSET) {
    prop->assign(reinterpret_cast<char*>(&cur_record_offset_),
                 sizeof(cur_record_offset_));
  } else if (prop_name == PROPERTIES_BLOB_SIZE) {
    prop->assign(reinterpret_cast<char*>(&cur_record_size_),
                 sizeof(cur_record_size_));
  } else {
    return Status::InvalidArgument("Unknown prop_name: " + prop_name);
  }

  return Status::OK();
}

void BlobFileIterator::IterateForPrev(uint64_t offset) {
  if (!init_) Init();

  if (offset >= total_blocks_size_) {
    iterate_offset_ = offset;
    status_ = Status::InvalidArgument("Out of bound");
    return;
  }

  Slice slice;
  uint64_t length;
  for (iterate_offset_ = 0;
       iterate_offset_ < total_blocks_size_ && iterate_offset_ < offset;
       iterate_offset_ += kBlobHeaderSize + length + kBlobTailerSize) {
    Status s = file_->Read(cur_record_offset_, kBlobHeaderSize, &slice,
                           reinterpret_cast<char*>(&length));
    if (!s.ok()) {
      status_ = s;
      return;
    }
  }

  if (iterate_offset_ > offset) iterate_offset_ -= length;
}

void BlobFileIterator::Prefetch() {
  readahead_size_ =
      std::min(total_blocks_size_ - iterate_offset_, readahead_size_);
  file_->Prefetch(iterate_offset_, readahead_size_);
  readahead_offset = iterate_offset_ + readahead_size_;
}

void BlobFileIterator::GetOneBlobRecord() {
  if (iterate_offset_ >= total_blocks_size_) {
    valid_ = false;
    return;
  }

  Prefetch();

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

}  // namespace titandb
}  // namespace rocksdb
