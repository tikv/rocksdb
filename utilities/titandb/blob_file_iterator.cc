//
// Created by 郑志铨 on 2018/8/9.
//

#include "blob_file_iterator.h"
#include "blob_format.h"

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
    : file_(std::move(file)), file_name_(file_name), file_size_(file_size) {}

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
  assert(blocks_size_ > 0);

  // Init first block
  GetOneBlock();
}

bool BlobFileIterator::Valid() const { return valid_; }

void BlobFileIterator::Next() { GetOneBlock(); }

Slice BlobFileIterator::key() const { return current_blob_record_.key; }

Slice BlobFileIterator::value() const { return current_blob_record_.value; }

void BlobFileIterator::GetOneBlock() {
  if (iterate_size_ >= blocks_size_) {
    valid_ = false;
    return;
  }

  Slice result;
  char buf[8];
  status_ = file_->Read(iterate_offset_, 8, &result, buf);
  if (!status_.ok()) return;
  uint64_t length = *reinterpret_cast<const uint64_t*>(result.data());

  current_blob_offset_ = iterate_offset_;
  current_blob_size_ = 8 + length;

  iterate_offset_ += 8;
  iterate_size_ += 8;
  assert(length > 0);
  buffer_.reserve(length);
  status_ = file_->Read(iterate_offset_, length, &result, buffer_.data());
  if (!status_.ok()) return;
  status_ = current_blob_record_.DecodeFrom(&result);
  if (!status_.ok()) return;
  iterate_offset_ += length;
  iterate_size_ += length;
}

Status BlobFileIterator::GetProperty(std::string prop_name, std::string* prop) {
  assert(Valid());

  prop->clear();

  if (prop_name == PROPERTIES_FILE_NUMBER) {
    prop->assign(reinterpret_cast<const char*>(&file_name_),
                 sizeof(file_name_));
  } else if (prop_name == PROPERTIES_BLOB_OFFSET) {
    prop->assign(reinterpret_cast<char*>(&current_blob_offset_),
                 sizeof(current_blob_offset_));
  } else if (prop_name == PROPERTIES_BLOB_SIZE) {
    prop->assign(reinterpret_cast<char*>(&current_blob_size_),
                 sizeof(current_blob_size_));
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
