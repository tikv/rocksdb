//
// Created by 郑志铨 on 2018/8/18.
//

#include "utilities/titandb/blob_gc_job.h"
#include "db/column_family.h"
#include "db/db_impl.h"
#include "table/internal_iterator.h"
#include "table/merging_iterator.h"
#include "utilities/titandb/blob_file_builder.h"
#include "utilities/titandb/blob_file_iterator.h"
#include "utilities/titandb/blob_file_manager.h"
#include "utilities/titandb/blob_file_reader.h"
#include "utilities/titandb/version.h"
#include "utilities/titandb/version_edit.h"

namespace rocksdb {
namespace titandb {

// Write callback for garbage collection to check if key has been updated
// since last read. Similar to how OptimisticTransaction works.
class BlobGCJob::GarbageCollectionWriteCallback : public WriteCallback {
 public:
  GarbageCollectionWriteCallback(ColumnFamilyData* cfd, const Slice& key,
                                 SequenceNumber upper_bound)
      : cfd_(cfd), key_(key), upper_bound_(upper_bound) {}

  virtual Status Callback(DB* db) override {
    auto* db_impl = reinterpret_cast<DBImpl*>(db);
    auto* sv = db_impl->GetAndRefSuperVersion(cfd_);
    SequenceNumber latest_seq = 0;
    bool found_record_for_key = false;
    bool is_blob_index = false;
    Status s = db_impl->GetLatestSequenceForKey(
        sv, key_, false /*cache_only*/, &latest_seq, &found_record_for_key,
        &is_blob_index);
    db_impl->ReturnAndCleanupSuperVersion(cfd_, sv);
    if (!s.ok() && !s.IsNotFound()) {
      // Error.
      assert(!s.IsBusy());
      return s;
    }
    if (s.IsNotFound()) {
      assert(!found_record_for_key);
      return Status::Busy("Key deleted");
    }
    assert(found_record_for_key);
    if (latest_seq > upper_bound_ || !is_blob_index) {
      return Status::Busy("Key overwritten");
    }
    return s;
  }

  virtual bool AllowWriteBatching() override { return false; }

 private:
  ColumnFamilyData* cfd_;
  // Key to check
  Slice key_;
  // Upper bound of sequence number to proceed.
  SequenceNumber upper_bound_;
};

BlobGCJob::BlobGCJob(rocksdb::titandb::BlobGC* blob_gc,
                     const TitanDBOptions& titan_db_options,
                     const TitanCFOptions& titan_cf_options, Env* env,
                     const EnvOptions& env_options,
                     BlobFileManager* blob_file_manager,
                     VersionSet* version_set, DB* db, uint32_t cf_id,
                     ColumnFamilyHandle* cfh, port::Mutex* mutex)
    : blob_gc_(blob_gc),
      db_(db),
      cf_id_(cf_id),
      cfh_(cfh),
      db_mutex_(mutex),
      titan_db_options_(titan_db_options),
      titan_cf_options_(titan_cf_options),
      env_(env),
      env_options_(env_options),
      blob_file_manager_(blob_file_manager),
      version_set_(version_set) {}

BlobGCJob::~BlobGCJob() {}

Status BlobGCJob::Prepare() { return Status::OK(); }

Status BlobGCJob::Run() {
  Status s;
  SampleCandidates();

  InternalIterator* gc_iter = nullptr;
  s = BuildIterator(&gc_iter);
  if (!s.ok()) {
    return s;
  }
  if (gc_iter == nullptr) {
    return Status::Aborted("BuildIterator failed");
  }

  s = RunGC(gc_iter);
  if (!s.ok()) {
    return s;
  }

  return Status::OK();
}

Status BlobGCJob::RunGC(InternalIterator* gc_iter) {
  Status s;
  // Create new blob file for rewrite valid KV pairs
  {
    unique_ptr<BlobFileHandle> blob_file_handle;
    s = this->blob_file_manager_->NewFile(&blob_file_handle);
    if (!s.ok()) {
      return s;
    }
    auto blob_file_builder = unique_ptr<BlobFileBuilder>(
        new BlobFileBuilder(titan_cf_options_, blob_file_handle->GetFile()));
    blob_file_builders_.emplace_back(std::make_pair(
        std::move(blob_file_handle), std::move(blob_file_builder)));
  }

  // Similar to OptimisticTransaction, we obtain latest_seq from
  // base DB, which is guaranteed to be no smaller than the sequence of
  // current key. We use a WriteCallback on write to check the key sequence
  // on write. If the key sequence is larger than latest_seq, we know
  // a new versions is inserted and the old blob can be disgard.
  //
  // We cannot use OptimisticTransaction because we need to pass
  // is_blob_index flag to GetImpl.
  std::vector<GarbageCollectionWriteCallback> callbacks;

  auto* cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(this->cfh_)->cfd();
  auto* db_impl = reinterpret_cast<DBImpl*>(this->db_);
  PinnableSlice index_entry;
  bool is_blob_index;
  for (gc_iter->SeekToFirst(); gc_iter->status().ok() && gc_iter->Valid();
       gc_iter->Next()) {
    // This API is very lightweight
    SequenceNumber latest_seq = this->db_->GetLatestSequenceNumber();

    // Read Key-Index pairs from LSM
    s = db_impl->GetImpl(ReadOptions(), this->cfh_, gc_iter->key(),
                         &index_entry, nullptr /*value_found*/,
                         nullptr /*read_callback*/, &is_blob_index);
    if (!s.ok() && !s.IsNotFound()) {
      // error
      return s;
    }
    if (s.IsNotFound() || !is_blob_index) {
      // Either the key is deleted or updated with a newer version which is
      // inlined in LSM.
      continue;
    }

    // Decode index_entry
    BlobIndex blob_index;
    s = blob_index.DecodeFrom(&index_entry);
    if (!s.ok()) {
      return s;
    }

    // Judge if blob index still hold by valid key
    std::string prop;
    gc_iter->GetProperty(BlobFileIterator::PROPERTY_FILE_NAME, &prop);
    uint64_t file_name = *reinterpret_cast<const uint64_t*>(prop.data());
    gc_iter->GetProperty(BlobFileIterator::PROPERTY_FILE_OFFSET, &prop);
    uint64_t file_offset = *reinterpret_cast<const uint64_t*>(prop.data());
    if (blob_index.file_number != file_name ||
        blob_index.blob_handle.offset != file_offset) {
      continue;
    }

    // New a WriteBatch for rewriting new Key-Index pairs to LSM
    BlobIndex index;
    BlobRecord record;
    record.key = gc_iter->key();
    record.value = gc_iter->value();
    index.file_number = blob_file_builders_.back().first->GetNumber();
    this->blob_file_builders_.back().second->Add(record, &index.blob_handle);
    std::string new_index_entry;
    index.EncodeTo(&new_index_entry);

    rewrite_batches_.emplace_back(std::make_pair(
        WriteBatch(),
        GarbageCollectionWriteCallback{cfd, record.key, latest_seq}));
    s = WriteBatchInternal::PutBlobIndex(&this->rewrite_batches_.back().first,
                                         this->cfh_->GetID(), record.key,
                                         new_index_entry);
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

Status BlobGCJob::BuildIterator(InternalIterator** result) const {
  Status s;
  const auto& inputs = blob_gc_->selected();
  // Build iterator
  auto list = new InternalIterator*[inputs.size()];
  for (size_t i = 0; i < inputs.size(); ++i) {
    unique_ptr<RandomAccessFileReader> file;
    s = NewBlobFileReader(inputs[i]->file_number, 0, this->titan_db_options_,
                          this->env_options_, this->env_, &file);
    // TODO memory leak here
    if (!s.ok()) {
      return s;
    }
    list[i] = new BlobFileIterator(move(file), inputs[i]->file_number,
                                   inputs[i]->file_size);
  }
  InternalKeyComparator* cmp = nullptr;
  *result = NewMergingIterator(cmp, list,
                               static_cast<int>(blob_gc_->candidates().size()));
  return Status::OK();
}

// We have to make sure crash consistency, but LSM db MANIFEST and BLOB db
// MANIFEST are separate, so we need to make sure all new blob file have
// added to db before we rewrite any key to LSM
Status BlobGCJob::Finish() {
  Status s;

  // Install output blob file to db
  for (auto& builder : blob_file_builders_) {
    s = builder.second->Finish();
    if (!s.ok()) {
      break;
    }
  }
  if (s.ok()) {
    std::vector<std::pair<std::shared_ptr<BlobFileMeta>,
                          std::unique_ptr<BlobFileHandle>>>
        files;
    for (auto& builder : blob_file_builders_) {
      auto file = std::make_shared<BlobFileMeta>();
      file->file_number = builder.first->GetNumber();
      file->file_size = builder.first->GetFile()->GetFileSize();
      files.emplace_back(
          std::make_pair(std::move(file), std::move(builder.first)));
    }
    blob_file_manager_->BatchFinishFiles(cfh_->GetID(), files);
  } else {
    std::vector<std::unique_ptr<BlobFileHandle>> handles;
    for (auto& builder : blob_file_builders_)
      handles.emplace_back(std::move(builder.first));
    blob_file_manager_->BatchDeleteFiles(handles);
    return s;
  }

  // Rewrite all valid keys to LSM
  {
    db_mutex_->Unlock();
    int i = 0;
    auto* db_impl = reinterpret_cast<DBImpl*>(db_);
    for (auto& write_batch : rewrite_batches_) {
      auto rewrite_status = db_impl->WriteWithCallback(
          WriteOptions(), &write_batch.first, &write_batch.second);
      if (rewrite_status.ok()) {
      } else if (rewrite_status.IsBusy()) {
        // The key is overwritten in the meanwhile. Drop the blob record.
      } else {
        // We hit an error.
      }
      i++;
    }
    db_mutex_->Lock();
  }

  // TODO cal discardable size for new blob file

  // Delete input blob file
  VersionEdit edit;
  edit.SetColumnFamilyID(cf_id_);
  for (const auto& tmp : blob_gc_->candidates())
    edit.DeleteBlobFile(tmp->file_number);
  {
    s = version_set_->LogAndApply(&edit, db_mutex_);
    // TODO what is this????
    // db_->pending_outputs_.erase(handle->GetNumber());
  }
  return Status::OK();
}

void BlobGCJob::SampleCandidates() {
  std::vector<std::shared_ptr<BlobFileMeta>> result;
  for (const auto& file : blob_gc_->candidates()) {
    if (!file->marked_for_sample || SampleOne(file)) {
      result.push_back(file);
    }
  }
  blob_gc_->set_selected(std::move(result));
}

bool BlobGCJob::SampleOne(
    const std::shared_ptr<rocksdb::titandb::BlobFileMeta>& file) {
  static const float kSampleSizeWindowRatio = 0.1;
  uint64_t sample_size_window = file->file_size * kSampleSizeWindowRatio;
  Random64 random64(file->file_size);
  uint64_t sample_begin_offset = random64.OneIn(file->file_size);
  std::unique_ptr<RandomAccessFileReader> file_reader;
  auto s = NewBlobFileReader(file->file_number, 0, titan_db_options_,
                             env_options_, env_, &file_reader);
  if (!s.ok()) {
    return false;
  }
  BlobFileIterator iter(std::move(file_reader), file->file_number,
                        file->file_size);
  iter.IterateForPrev(sample_begin_offset);
  bool is_blob_index;
  PinnableSlice index_entry;
  auto* db_impl = reinterpret_cast<DBImpl*>(db_);
  uint64_t iterated_size = 0;
  uint64_t discardable_size = 0;
  for (;
       iterated_size < sample_size_window && iter.status().ok() && iter.Valid();
       iter.Next()) {
    std::string prop;
    iter.GetProperty(BlobFileIterator::PROPERTY_FILE_NAME, &prop);
    uint64_t file_name = *reinterpret_cast<const uint64_t*>(prop.data());
    iter.GetProperty(BlobFileIterator::PROPERTY_FILE_OFFSET, &prop);
    uint64_t file_offset = *reinterpret_cast<const uint64_t*>(prop.data());
    // Read Key-Index pairs from LSM
    Status get_status = db_impl->GetImpl(
        ReadOptions(), cfh_, iter.key(), &index_entry, nullptr /*value_found*/,
        nullptr /*read_callback*/, &is_blob_index);
    if (is_blob_index) {
      discardable_size += iter.key().size() + iter.value().size();
      continue;
    }
    // Decode index
    BlobIndex blob_index;
    s = blob_index.DecodeFrom(&index_entry);
    if (!s.ok()) {
      discardable_size += iter.key().size() + iter.value().size();
      continue;
    }
    if (blob_index.file_number != file_name ||
        blob_index.blob_handle.offset != file_offset) {
      discardable_size += iter.key().size() + iter.value().size();
      continue;
    }
  }

  if (discardable_size >= sample_size_window * 0.5) {
    return true;
  }

  return false;
}

}  // namespace titandb
}  // namespace rocksdb
