#pragma once

#include "rocksdb/iterator.h"
#include "db/db_iter.h"
#include "utilities/titandb/blob_format.h"
#include "utilities/titandb/version_set.h"

namespace rocksdb {
namespace titandb {

class TitanDBIterator : public Iterator {
 public:
  TitanDBIterator(std::shared_ptr<ReadContext> ctx,
                  std::unique_ptr<ArenaWrappedDBIter> iter)
      : ctx_(ctx),
        iter_(std::move(iter)) {}

  bool Valid() const override {
    return iter_->Valid() && status_.ok();
  }

  Status status() const override {
    Status s = iter_->status();
    if (s.ok()) s = status_;
    return s;
  }

  void SeekToFirst() override {
    iter_->SeekToFirst();
    GetBlobValue();
  }

  void SeekToLast() override {
    iter_->SeekToLast();
    GetBlobValue();
  }

  void Seek(const Slice& target) override {
    iter_->Seek(target);
    GetBlobValue();
  }

  void SeekForPrev(const Slice& target) override {
    iter_->SeekForPrev(target);
    GetBlobValue();
  }

  void Next() override {
    iter_->Next();
    GetBlobValue();
  }

  void Prev() override {
    iter_->Prev();
    GetBlobValue();
  }

  Slice key() const override {
    return iter_->key();
  }

  Slice value() const override {
    if (!iter_->IsBlob()) return iter_->value();
    return record_.value;
  }

 private:
  void GetBlobValue() {
    if (!iter_->Valid() || !iter_->IsBlob()) return;

    BlobIndex index;
    status_ = DecodeInto(iter_->value(), &index);
    if (!status_.ok()) return;

    auto current = ctx_->current();
    auto options = ctx_->options();
    if (!options.readahead_size) {
      status_ = current->Get(options, index, &record_, &buffer_);
      return;
    }

    auto it = cache_.find(index.file_number);
    if (it == cache_.end()) {
      std::unique_ptr<BlobFileReader> reader;
      status_ = current->NewReader(options, index.file_number, &reader);
      if (!status_.ok()) return;
      it = cache_.emplace(index.file_number, std::move(reader)).first;
    }
    status_ = it->second->Get(options, index.blob_handle, &record_, &buffer_);
  }

  Status status_;
  BlobRecord record_;
  std::string buffer_;
  std::shared_ptr<ReadContext> ctx_;
  std::unique_ptr<ArenaWrappedDBIter> iter_;
  std::map<uint64_t, std::unique_ptr<BlobFileReader>> cache_;
};

}  // namespace titandb
}  // namespace rocksdb
