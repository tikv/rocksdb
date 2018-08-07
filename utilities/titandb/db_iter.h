#pragma once

#include "rocksdb/iterator.h"
#include "db/db_iter.h"
#include "utilities/titandb/version.h"
#include "utilities/titandb/blob_format.h"
#include "utilities/titandb/blob_file_reader.h"

namespace rocksdb {
namespace titandb {

// Wraps the current version together with the snapshot from base DB
// so that we can safely recycle a steal version when it is dropped.
// This also implies a guarantee that the current version must contain
// all the data accessable from base DB.
class TitanSnapshot : public Snapshot {
 public:
  TitanSnapshot(Version* current, const Snapshot* snapshot)
      : current_(current), snapshot_(snapshot) {}

  Version* current() const { return current_; }

  const Snapshot* snapshot() const { return snapshot_; }

  SequenceNumber GetSequenceNumber() const override {
    return snapshot_->GetSequenceNumber();
  }

 private:
  Version* current_;
  const Snapshot* snapshot_;
};

class TitanDBIterator : public Iterator {
 public:
  TitanDBIterator(const ReadOptions& options,
                  std::shared_ptr<ManagedSnapshot> snap,
                  std::unique_ptr<ArenaWrappedDBIter> iter)
      : options_(options),
        snap_(snap),
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

    auto snapshot = reinterpret_cast<const TitanSnapshot*>(options_.snapshot);
    auto current = snapshot->current();
    if (!options_.readahead_size) {
      status_ = current->Get(options_, index, &record_, &buffer_);
      return;
    }

    auto it = cache_.find(index.file_number);
    if (it == cache_.end()) {
      std::unique_ptr<BlobFileReader> reader;
      status_ = current->NewReader(options_, index.file_number, &reader);
      if (!status_.ok()) return;
      it = cache_.emplace(index.file_number, std::move(reader)).first;
    }
    status_ = it->second->Get(options_, index.blob_handle, &record_, &buffer_);
  }

  Status status_;
  BlobRecord record_;
  std::string buffer_;
  ReadOptions options_;
  std::shared_ptr<ManagedSnapshot> snap_;
  std::unique_ptr<ArenaWrappedDBIter> iter_;
  std::map<uint64_t, std::unique_ptr<BlobFileReader>> cache_;
};

}  // namespace titandb
}  // namespace rocksdb
