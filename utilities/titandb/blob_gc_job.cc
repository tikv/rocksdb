#include "utilities/titandb/blob_gc_job.h"

namespace rocksdb {
namespace titandb {

// Write callback for garbage collection to check if key has been updated
// since last read. Similar to how OptimisticTransaction works.
class BlobGCJob::GarbageCollectionWriteCallback : public WriteCallback {
 public:
  GarbageCollectionWriteCallback(ColumnFamilyHandle* cfh, std::string&& _key,
                                 BlobIndex&& blob_index)
      : cfh_(cfh), key_(std::move(_key)), blob_index_(blob_index) {
    assert(!key_.empty());
  }

  std::string value;

  virtual Status Callback(DB* db) override {
    auto* db_impl = reinterpret_cast<DBImpl*>(db);
    PinnableSlice index_entry;
    bool is_blob_index;
    auto s = db_impl->GetImpl(ReadOptions(), cfh_, key_, &index_entry,
                              nullptr /*value_found*/,
                              nullptr /*read_callback*/, &is_blob_index);
    if (!s.ok() && !s.IsNotFound()) {
      fprintf(stderr, "GetImpl err, status:%s\n", s.ToString().c_str());
      abort();
    }
    if (s.IsNotFound()) {
      // Either the key is deleted or updated with a newer version which is
      // inlined in LSM.
      s = Status::Busy("key deleted");
    } else if (!is_blob_index) {
      s = Status::Busy("key overwritten with other value");
    }

    if (s.ok()) {
      BlobIndex other_blob_index;
      s = other_blob_index.DecodeFrom(&index_entry);
      assert(s.ok());
      if (!(blob_index_ == other_blob_index)) {
        s = Status::Busy("key overwritten with other blob");
      }
    }

    return s;
  }

  virtual bool AllowWriteBatching() override { return false; }

  std::string key() { return key_; }

 private:
  ColumnFamilyHandle* cfh_;
  // Key to check
  std::string key_;
  BlobIndex blob_index_;
};

BlobGCJob::BlobGCJob(BlobGC* blob_gc, DB* db, ColumnFamilyHandle* cfh,
                     port::Mutex* mutex, const TitanDBOptions& titan_db_options,
                     Env* env, const EnvOptions& env_options,
                     BlobFileManager* blob_file_manager,
                     VersionSet* version_set)
    : blob_gc_(blob_gc),
      base_db_(db),
      base_db_impl_(reinterpret_cast<DBImpl*>(base_db_)),
      cfh_(cfh),
      tdb_mutex_(mutex),
      titan_db_options_(titan_db_options),
      env_(env),
      env_options_(env_options),
      blob_file_manager_(blob_file_manager),
      version_set_(version_set) {}

BlobGCJob::~BlobGCJob() {
  if (cmp_) delete cmp_;
}

Status BlobGCJob::Prepare() { return Status::OK(); }

Status BlobGCJob::Run() {
  Status s;

  s = SampleCandidateFiles();
  if (!s.ok()) return s;

  s = DoRunGC();
  if (!s.ok()) return s;

  return Status::OK();
}

Status BlobGCJob::SampleCandidateFiles() {
  std::vector<BlobFileMeta*> result;
  for (const auto& file : blob_gc_->candidate_files()) {
    if (!file->marked_for_sample || DoSample(file)) {
      result.push_back(file);
    }
  }

  if (result.empty()) return Status::Aborted("No blob file need to be gc");

  blob_gc_->set_selected_files(std::move(result));

  return Status::OK();
}

bool BlobGCJob::DoSample(const BlobFileMeta* file) {
  Status s;
  uint64_t sample_size_window = static_cast<uint64_t>(
      file->file_size * blob_gc_->titan_cf_options().sample_file_size_ratio);
  Random64 random64(file->file_size);
  uint64_t sample_begin_offset =
      random64.Uniform(file->file_size - sample_size_window);

  std::unique_ptr<RandomAccessFileReader> file_reader;
  const int readahead = 256 << 10;
  s = NewBlobFileReader(file->file_number, readahead, titan_db_options_,
                        env_options_, env_, &file_reader);
  assert(s.ok());
  BlobFileIterator iter(std::move(file_reader), file->file_number,
                        file->file_size, blob_gc_->titan_cf_options());
  iter.IterateForPrev(sample_begin_offset);
  assert(iter.status().ok());

  uint64_t iterated_size{0};
  uint64_t discardable_size{0};
  for (iter.Next();
       iterated_size < sample_size_window && iter.status().ok() && iter.Valid();
       iter.Next()) {
    BlobIndex blob_index = iter.GetBlobIndex();
    uint64_t total_length = blob_index.blob_handle.size;
    iterated_size += total_length;
    if (DiscardEntry(iter.key(), blob_index)) {
      discardable_size += total_length;
    }
  }
  assert(iter.status().ok());

  return discardable_size >=
         sample_size_window *
             blob_gc_->titan_cf_options().blob_file_discardable_ratio;
}

Status BlobGCJob::DoRunGC() {
  Status s;

  std::unique_ptr<BlobFileMergeIterator> gc_iter;
  s = BuildIterator(&gc_iter);
  if (!s.ok()) return s;
  if (!gc_iter) return Status::Aborted("Build iterator for gc failed");

  // Similar to OptimisticTransaction, we obtain latest_seq from
  // base DB, which is guaranteed to be no smaller than the sequence of
  // current key. We use a WriteCallback on write to check the key sequence
  // on write. If the key sequence is larger than latest_seq, we know
  // a new versions is inserted and the old blob can be discard.
  //
  // We cannot use OptimisticTransaction because we need to pass
  // is_blob_index flag to GetImpl.
  std::unique_ptr<BlobFileHandle> blob_file_handle;
  std::unique_ptr<BlobFileBuilder> blob_file_builder;

  std::string last_key;
  bool last_key_valid = false;
  for (gc_iter->SeekToFirst(); gc_iter->status().ok() && gc_iter->Valid();
       gc_iter->Next()) {
    if (!last_key.empty() && !gc_iter->key().compare(last_key)) {
      if (last_key_valid) {
        continue;
      }
    } else {
      last_key = gc_iter->key().ToString();
      last_key_valid = false;
    }

    BlobIndex blob_index = gc_iter->GetBlobIndex();
    if (DiscardEntry(gc_iter->key(), blob_index)) {
      continue;
    }

    last_key_valid = true;

    // Rewrite entry to new blob file
    if (!blob_file_handle && !blob_file_builder) {
      s = blob_file_manager_->NewFile(&blob_file_handle);
      if (!s.ok()) {
        break;
      }
      blob_file_builder = unique_ptr<BlobFileBuilder>(new BlobFileBuilder(
          blob_gc_->titan_cf_options(), blob_file_handle->GetFile()));
    }
    assert(blob_file_handle);
    assert(blob_file_builder);

    BlobRecord blob_record;
    blob_record.key = gc_iter->key();
    blob_record.value = gc_iter->value();

    BlobIndex new_blob_index;
    new_blob_index.file_number = blob_file_handle->GetNumber();
    blob_file_builder->Add(blob_record, &new_blob_index.blob_handle);
    std::string index_entry;
    new_blob_index.EncodeTo(&index_entry);

    // Store WriteBatch for rewriting new Key-Index pairs to LSM
    GarbageCollectionWriteCallback callback(cfh_, blob_record.key.ToString(),
                                            std::move(blob_index));
    callback.value = index_entry;
    rewrite_batches_.emplace_back(
        std::make_pair(WriteBatch(), std::move(callback)));
    auto& wb = rewrite_batches_.back().first;
    s = WriteBatchInternal::PutBlobIndex(&wb, cfh_->GetID(), blob_record.key,
                                         index_entry);
    if (!s.ok()) {
      break;
    }
  }

  if (gc_iter->status().ok() && s.ok()) {
    if (blob_file_builder && blob_file_handle) {
      assert(blob_file_builder->status().ok());
      blob_file_builders_.emplace_back(std::make_pair(
          std::move(blob_file_handle), std::move(blob_file_builder)));
    } else {
      assert(!blob_file_builder);
      assert(!blob_file_handle);
    }
  } else if (!gc_iter->status().ok()) {
    return gc_iter->status();
  }

  return s;
}

Status BlobGCJob::BuildIterator(unique_ptr<BlobFileMergeIterator>* result) {
  Status s;
  const auto& inputs = blob_gc_->selected_files();
  assert(!inputs.empty());
  std::vector<std::unique_ptr<BlobFileIterator>> list;
  for (std::size_t i = 0; i < inputs.size(); ++i) {
    std::unique_ptr<RandomAccessFileReader> file;
    // TODO(@DorianZheng) set read ahead size
    s = NewBlobFileReader(inputs[i]->file_number, 0, titan_db_options_,
                          env_options_, env_, &file);
    if (!s.ok()) {
      break;
    }
    list.emplace_back(std::unique_ptr<BlobFileIterator>(new BlobFileIterator(
        std::move(file), inputs[i]->file_number, inputs[i]->file_size,
        blob_gc_->titan_cf_options())));
  }

  if (s.ok()) result->reset(new BlobFileMergeIterator(std::move(list)));

  return s;
}

bool BlobGCJob::DiscardEntry(const Slice& key, const BlobIndex& blob_index) {
  PinnableSlice index_entry;
  bool is_blob_index;
  auto s = base_db_impl_->GetImpl(ReadOptions(), cfh_, key, &index_entry,
                                  nullptr /*value_found*/,
                                  nullptr /*read_callback*/, &is_blob_index);
  if (!s.ok() && !s.IsNotFound()) {
    fprintf(stderr, "GetImpl err, status:%s\n", s.ToString().c_str());
    abort();
  }
  if (s.IsNotFound() || !is_blob_index) {
    // Either the key is deleted or updated with a newer version which is
    // inlined in LSM.
    return true;
  }

  BlobIndex other_blob_index;
  s = other_blob_index.DecodeFrom(&index_entry);
  if (!s.ok()) {
    return true;
  }

  return !(blob_index == other_blob_index);
}

// We have to make sure crash consistency, but LSM db MANIFEST and BLOB db
// MANIFEST are separate, so we need to make sure all new blob file have
// added to db before we rewrite any key to LSM
Status BlobGCJob::Finish() {
  Status s;
  {
    tdb_mutex_->Unlock();
    s = InstallOutputBlobFiles();
    if (s.ok()) s = RewriteValidKeyToLSM();
    if (!s.ok()) blob_gc_->ClearSelectedFiles();
    tdb_mutex_->Lock();
  }

  // TODO(@DorianZheng) cal discardable size for new blob file

  if (s.ok()) {
    s = DeleteInputBlobFiles();
  }

  // We need to unset being_gc mark for unselected candidate blob files
  for (auto& file : blob_gc_->candidate_files()) {
    bool selected = false;
    for (std::size_t i = 0; i < blob_gc_->selected_files().size(); i++) {
      if (*blob_gc_->selected_files()[i] == *file) {
        selected = true;
        break;
      }
    }
    if (!selected) file->being_gc.store(false, std::memory_order_release);
  }

  return s;
}

Status BlobGCJob::InstallOutputBlobFiles() {
  Status s;
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
    for (auto& builder : this->blob_file_builders_) {
      auto file = std::make_shared<BlobFileMeta>();
      file->file_number = builder.first->GetNumber();
      file->file_size = builder.first->GetFile()->GetFileSize();
      files.emplace_back(make_pair(move(file), std::move(builder.first)));
    }
    this->blob_file_manager_->BatchFinishFiles(this->cfh_->GetID(), files);
  } else {
    std::vector<unique_ptr<BlobFileHandle>> handles;
    for (auto& builder : this->blob_file_builders_)
      handles.emplace_back(std::move(builder.first));
    this->blob_file_manager_->BatchDeleteFiles(handles);
  }
  return s;
}

Status BlobGCJob::RewriteValidKeyToLSM() {
  Status s;
  auto* db_impl = reinterpret_cast<DBImpl*>(this->base_db_);
  WriteOptions wo;
  wo.low_pri = true;
  // TODO(@DorianZheng) more elegant solution to deal with missing column families
  wo.ignore_missing_column_families = true;
  for (auto& write_batch : this->rewrite_batches_) {
    s = db_impl->WriteWithCallback(wo, &write_batch.first, &write_batch.second);
    if (s.ok()) {
      // Key is successfully written to LSM
    } else if (s.IsBusy()) {
      // The key is overwritten in the meanwhile. Drop the blob record.
    } else {
      // We hit an error.
      break;
    }
  }
  if (s.IsBusy()) {
    s = Status::OK();
  }

  if (s.ok()) {
    db_impl->FlushWAL(true);
  }

  return s;
}

Status BlobGCJob::DeleteInputBlobFiles() const {
  Status s;
  VersionEdit edit;
  edit.SetColumnFamilyID(cfh_->GetID());
  for (const auto& file : blob_gc_->selected_files()) {
    edit.DeleteBlobFile(file->file_number);
  }
  s = version_set_->LogAndApply(&edit, this->tdb_mutex_);
  // TODO(@DorianZheng) Purge pending outputs
  // base_db_->pending_outputs_.erase(handle->GetNumber());
  return s;
}

}  // namespace titandb
}  // namespace rocksdb
