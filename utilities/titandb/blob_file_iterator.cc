//
// Created by 郑志铨 on 2018/8/9.
//

#include "blob_file_iterator.h"
#include "blob_format.h"

namespace rocksdb {
namespace titandb {

const std::string BlobFileIterator::PROPERTY_FILE_NAME = "PropFileName";
const std::string BlobFileIterator::PROPERTY_FILE_OFFSET = "PropFileOffset";

BlobFileIterator::BlobFileIterator(
    std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_name,
    uint64_t file_size)
    : file_(std::move(file)), file_name_(file_name), file_size_(file_size) {}

BlobFileIterator::~BlobFileIterator() {}

void BlobFileIterator::SeekToFirst() {
  std::vector<char> buf;
  buf.reserve(BlobFileFooter::kEncodedLength);
  Slice result;
  status_ = file_->Read(file_size_ - BlobFileFooter::kEncodedLength,
                        BlobFileFooter::kEncodedLength, &result, buf.data());
  if (!status_.ok()) return;
  BlobFileFooter blob_file_footer;
  blob_file_footer.DecodeFrom(&result);
  blocks_size_ = file_size_ - BlobFileFooter::kEncodedLength -
                 blob_file_footer.meta_index_handle.size();

  // Init first block
  GetOneBlock();
}

bool BlobFileIterator::Valid() const { return iterate_size_ <= blocks_size_; }

void BlobFileIterator::Next() {
  if (iterate_size_ >= blocks_size_) {
    status_ = Status::Aborted("The end of blob file blocks");
    return;
  }

  GetOneBlock();
}

Slice BlobFileIterator::key() const { return current_blob_record_.key; }

Slice BlobFileIterator::value() const { return current_blob_record_.value; }

void BlobFileIterator::GetOneBlock() {
  Slice result;
  char buf[4];
  status_ = file_->Read(iterate_offset_, 4, &result, buf);
  if (!status_.ok()) return;
  uint32_t length = *reinterpret_cast<const uint32_t*>(result.data());
  current_blob_offset_ = iterate_offset_;
  iterate_offset_ += 4;
  iterate_size_ += 4;
  assert(length > 0);
  buffer_.reserve(length);
  status_ = file_->Read(iterate_offset_, length, &result, buffer_.data());
  if (!status_.ok()) return;
  current_blob_record_.DecodeFrom(&result);
  iterate_offset_ += length;
  iterate_size_ += length;
}

Status BlobFileIterator::GetProperty(std::string prop_name, std::string* prop) {
  if (prop_name == PROPERTY_FILE_NAME) {
    prop->assign(reinterpret_cast<const char*>(&file_name_),
                 sizeof(file_name_));
  } else if (prop_name == PROPERTY_FILE_OFFSET) {
    prop->assign(reinterpret_cast<char*>(&current_blob_offset_),
                 sizeof(current_blob_offset_));
  } else {
    return Status::InvalidArgument("Unknown prop_name: " + prop_name);
  }

  return Status::OK();
}

void BlobFileIterator::IterateForPrev(uint64_t offset) {
  if (offset >= blocks_size_) {
    iterate_size_ = blocks_size_ + 1;
    return;
  }

  Slice result;
  uint32_t length = 0;
  for (current_blob_offset_ = 0;
       current_blob_offset_ < blocks_size_ && current_blob_offset_ < offset;
       current_blob_offset_ += length) {
    file_->Read(current_blob_offset_, 4, &result,
                reinterpret_cast<char*>(&length));
  }
  if (current_blob_offset_ > offset) current_blob_offset_ -= length;
}

}  // namespace titandb
}  // namespace rocksdb
