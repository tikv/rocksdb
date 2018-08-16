#include "utilities/titandb/db_impl.h"

#include "utilities/titandb/db_iter.h"
#include "utilities/titandb/table_factory.h"

namespace rocksdb {
namespace titandb {

class TitanDBImpl::FileManager : public BlobFileManager {
 public:
  FileManager(TitanDBImpl* db) : db_(db) {}

  Status NewFile(std::unique_ptr<BlobFileHandle>* handle) {
    auto number = db_->vset_->NewFileNumber();
    auto name = BlobFileName(db_->dirname_, number);

    Status s;
    std::unique_ptr<WritableFileWriter> file;
    {
      std::unique_ptr<WritableFile> f;
      s = db_->env_->NewWritableFile(name, &f, db_->env_options_);
      if (!s.ok()) return s;
      file.reset(new WritableFileWriter(std::move(f), db_->env_options_));
    }

    handle->reset(new FileHandle(number, name, std::move(file)));
    {
      MutexLock l(&db_->mutex_);
      db_->pending_outputs_.insert(number);
    }
    return s;
  }

  Status FinishFile(uint32_t cf_id,
                    const BlobFileMeta& file,
                    std::unique_ptr<BlobFileHandle> handle) {
    Status s = handle->GetFile()->Sync(false);
    if (s.ok()) {
      s = handle->GetFile()->Close();
    }
    if (!s.ok()) return s;

    VersionEdit edit;
    edit.SetColumnFamilyID(cf_id);
    edit.AddBlobFile(file);

    {
      MutexLock l(&db_->mutex_);
      s = db_->vset_->LogAndApply(&edit, &db_->mutex_);
      db_->pending_outputs_.erase(handle->GetNumber());
    }
    return s;
  }

  Status DeleteFile(std::unique_ptr<BlobFileHandle> handle) {
    Status s = db_->env_->DeleteFile(handle->GetName());
    {
      MutexLock l(&db_->mutex_);
      db_->pending_outputs_.erase(handle->GetNumber());
    }
    return s;
  }

 private:
  class FileHandle : public BlobFileHandle {
   public:
    FileHandle(uint64_t number,
               const std::string& name,
               std::unique_ptr<WritableFileWriter> file)
        : number_(number),
          name_(name),
          file_(std::move(file)) {}

    uint64_t GetNumber() const override { return number_; }

    const std::string& GetName() const override { return name_; }

    WritableFileWriter* GetFile() const override { return file_.get(); }

   private:
    uint64_t number_;
    std::string name_;
    std::unique_ptr<WritableFileWriter> file_;
  };

  TitanDBImpl* db_;
};

TitanDBImpl::TitanDBImpl(const TitanDBOptions& options, const std::string& dbname)
    : TitanDB(),
      dbname_(dbname),
      env_(options.env),
      env_options_(options),
      db_options_(options) {
  if (db_options_.dirname.empty()) {
    db_options_.dirname = dbname_ + "/titandb";
  }
  dirname_ = db_options_.dirname;
  vset_.reset(new VersionSet(db_options_));
  blob_manager_.reset(new FileManager(this));
}

TitanDBImpl::~TitanDBImpl() {
  Close();
}

Status TitanDBImpl::Open(const std::vector<TitanCFDescriptor>& descs,
                         std::vector<ColumnFamilyHandle*>* handles) {
  // Sets up directories for base DB and TitanDB.
  Status s = env_->CreateDirIfMissing(dbname_);
  if (!s.ok()) return s;
  if (!db_options_.info_log) {
    s = CreateLoggerFromOptions(dbname_, db_options_, &db_options_.info_log);
    if (!s.ok()) return s;
  }
  s = env_->CreateDirIfMissing(dirname_);
  if (!s.ok()) return s;
  s = env_->LockFile(LockFileName(dirname_), &lock_);
  if (!s.ok()) return s;

  std::vector<ColumnFamilyDescriptor> base_descs;
  for (auto& desc : descs) {
    base_descs.emplace_back(desc.name, desc.options);
  }
  std::map<uint32_t, TitanCFOptions> column_families;

  // Opens the base DB first to collect the column families information.
  s = DB::Open(db_options_, dbname_, base_descs, handles, &db_);
  if (s.ok()) {
    for (size_t i = 0; i < descs.size(); i++) {
      auto handle = (*handles)[i];
      column_families.emplace(handle->GetID(), descs[i].options);
      db_->DestroyColumnFamilyHandle(handle);
      // Replaces the provided table factory with TitanTableFactory.
      base_descs[i].options.table_factory.reset(
          new TitanTableFactory(descs[i].options, blob_manager_));
    }
    handles->clear();
    s = db_->Close();
    delete db_;
  }
  if (!s.ok()) return s;

  s = vset_->Open(column_families);
  if (!s.ok()) return s;

  s = DB::Open(db_options_, dbname_, base_descs, handles, &db_);
  if (s.ok()) {
    db_impl_ = reinterpret_cast<DBImpl*>(db_->GetRootDB());
  }
  return s;
}

Status TitanDBImpl::Close() {
  Status s;
  if (db_) {
    s = db_->Close();
    delete db_;
    db_ = nullptr;
    db_impl_ = nullptr;
  }
  if (lock_) {
    env_->UnlockFile(lock_);
    lock_ = nullptr;
  }
  return s;
}

Status TitanDBImpl::Get(const ReadOptions& options,
                        ColumnFamilyHandle* handle,
                        const Slice& key, PinnableSlice* value) {
  if (options.snapshot) {
    return GetImpl(options, handle, key, value);
  }
  ReadOptions ro(options);
  ManagedSnapshot snapshot(this);
  ro.snapshot = snapshot.snapshot();
  return GetImpl(ro, handle, key, value);
}

Status TitanDBImpl::GetImpl(const ReadOptions& options,
                            ColumnFamilyHandle* handle,
                            const Slice& key, PinnableSlice* value) {
  auto snap = reinterpret_cast<const TitanSnapshot*>(options.snapshot);
  auto storage = snap->current()->GetBlobStorage(handle->GetID());

  Status s;
  bool is_blob_index = false;
  s = db_impl_->GetImpl(options, handle, key, value,
                        nullptr /*value_found*/,
                        nullptr /*read_callback*/,
                        &is_blob_index);
  if (!s.ok() || !is_blob_index) return s;

  BlobIndex index;
  s = index.DecodeFrom(value);
  if (!s.ok()) return s;

  BlobRecord record;
  std::string buffer;
  s = storage->Get(options, index, &record, &buffer);
  if (s.ok()) {
    value->Reset();
    value->PinSelf(record.value);
  }
  return s;
}

std::vector<Status> TitanDBImpl::MultiGet(
    const ReadOptions& options,
    const std::vector<ColumnFamilyHandle*>& handles,
    const std::vector<Slice>& keys, std::vector<std::string>* values) {
  if (options.snapshot) {
    return MultiGetImpl(options, handles, keys, values);
  }
  ReadOptions ro(options);
  ManagedSnapshot snapshot(this);
  ro.snapshot = snapshot.snapshot();
  return MultiGetImpl(ro, handles, keys, values);
}

std::vector<Status> TitanDBImpl::MultiGetImpl(
    const ReadOptions& options,
    const std::vector<ColumnFamilyHandle*>& handles,
    const std::vector<Slice>& keys, std::vector<std::string>* values) {
  std::vector<Status> res;
  res.reserve(keys.size());
  values->clear();
  values->reserve(keys.size());
  for (size_t i = 0; i < keys.size(); i++) {
    Status s;
    std::string value;
    PinnableSlice pinnable_value(&value);
    s = GetImpl(options, handles[i], keys[i], &pinnable_value);
    res.emplace_back(s);
    values->emplace_back(value);
  }
  return res;
}

Iterator* TitanDBImpl::NewIterator(const ReadOptions& options,
                                   ColumnFamilyHandle* handle) {
  std::shared_ptr<ManagedSnapshot> snapshot;
  if (options.snapshot) {
    return NewIteratorImpl(options, handle, snapshot);
  }
  ReadOptions ro(options);
  snapshot.reset(new ManagedSnapshot(this));
  ro.snapshot = snapshot->snapshot();
  return NewIteratorImpl(ro, handle, snapshot);
}

Iterator* TitanDBImpl::NewIteratorImpl(
    const ReadOptions& options,
    ColumnFamilyHandle* handle,
    std::shared_ptr<ManagedSnapshot> snapshot) {
  auto cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(handle)->cfd();
  auto snap = reinterpret_cast<const TitanSnapshot*>(options.snapshot);
  auto storage = snap->current()->GetBlobStorage(handle->GetID());
  std::unique_ptr<ArenaWrappedDBIter> iter(
      db_impl_->NewIteratorImpl(
          options, cfd, snap->GetSequenceNumber(),
          nullptr /*read_callback*/, true /*allow_blob*/));
  return new TitanDBIterator(options, storage, snapshot, std::move(iter));
}

Status TitanDBImpl::NewIterators(
    const ReadOptions& options,
    const std::vector<ColumnFamilyHandle*>& handles,
    std::vector<Iterator*>* iterators) {
  ReadOptions ro(options);
  std::shared_ptr<ManagedSnapshot> snapshot;
  if (!ro.snapshot) {
    snapshot.reset(new ManagedSnapshot(this));
    ro.snapshot = snapshot->snapshot();
  }
  iterators->clear();
  iterators->reserve(handles.size());
  for (auto& handle : handles) {
    iterators->emplace_back(NewIteratorImpl(ro, handle, snapshot));
  }
  return Status::OK();
}

const Snapshot* TitanDBImpl::GetSnapshot() {
  Version* current;
  const Snapshot* snapshot;
  {
    MutexLock l(&mutex_);
    current = vset_->current();
    current->Ref();
    snapshot = db_->GetSnapshot();
  }
  return new TitanSnapshot(current, snapshot);
}

void TitanDBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
  auto s = reinterpret_cast<const TitanSnapshot*>(snapshot);
  {
    MutexLock l(&mutex_);
    s->current()->Unref();
    db_->ReleaseSnapshot(s->snapshot());
  }
  delete s;
}

}  // namespace titandb
}  // namespace rocksdb
