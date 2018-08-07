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

  Status FinishFile(const BlobFileMeta& file,
                    std::unique_ptr<BlobFileHandle> handle) {
    Status s = handle->file()->Sync(false);
    if (s.ok()) {
      s = handle->file()->Close();
    }
    if (!s.ok()) return s;

    VersionEdit edit;
    edit.AddBlobFile(file);

    {
      MutexLock l(&db_->mutex_);
      s = db_->vset_->LogAndApply(&edit, &db_->mutex_);
      db_->pending_outputs_.erase(handle->number());
    }
    return s;
  }

  Status DeleteFile(std::unique_ptr<BlobFileHandle> handle) {
    Status s = db_->env_->DeleteFile(handle->name());
    {
      MutexLock l(&db_->mutex_);
      db_->pending_outputs_.erase(handle->number());
    }
    return s;
  }

 private:
  class FileHandle : public BlobFileHandle {
   public:
    FileHandle(uint64_t _number,
               const std::string& _name,
               std::unique_ptr<WritableFileWriter> _file)
        : number_(_number),
          name_(_name),
          file_(std::move(_file)) {}

    uint64_t number() const override { return number_; }

    const std::string& name() const override { return name_; }

    WritableFileWriter* file() const override { return file_.get(); }

   private:
    uint64_t number_;
    std::string name_;
    std::unique_ptr<WritableFileWriter> file_;
  };

  TitanDBImpl* db_;
};

TitanDBImpl::TitanDBImpl(const std::string& dbname,
                         const DBOptions& db_options,
                         const TitanDBOptions& tdb_options)
    : TitanDB(),
      env_(db_options.env),
      env_options_(db_options),
      db_options_(db_options),
      tdb_options_(tdb_options),
      dbname_(dbname) {
  if (tdb_options_.dirname.empty()) {
    tdb_options_.dirname = dbname_ + "/titandb";
  }
  dirname_ = tdb_options_.dirname;
}

TitanDBImpl::~TitanDBImpl() {
  Status s = Close();
  assert(s.ok());
}

Status TitanDBImpl::Open(
    const std::vector<ColumnFamilyDescriptor>& cf_descs,
    std::vector<ColumnFamilyHandle*>* cf_handles) {
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

  vset_ = new VersionSet(db_options_, tdb_options_);
  s = vset_->Open();
  if (!s.ok()) return s;

  blob_manager_.reset(new FileManager(this));

  // Replaces the provided table factory with TitanTableFactory.
  auto titan_cfs = cf_descs;
  {
    for (auto& cf : titan_cfs) {
      auto factory = new TitanTableFactory(
          tdb_options_, cf.options.table_factory, blob_manager_);
      cf.options.table_factory.reset(factory);
    }
  }

  s = DB::Open(db_options_, dbname_, titan_cfs, cf_handles, &db_);
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
  if (vset_) {
    delete vset_;
    vset_ = nullptr;
  }
  if (lock_) {
    env_->UnlockFile(lock_);
    lock_ = nullptr;
  }
  return s;
}

Status TitanDBImpl::Get(const ReadOptions& options,
                        ColumnFamilyHandle* cf_handle,
                        const Slice& key, PinnableSlice* value) {
  if (options.snapshot) {
    return GetImpl(options, cf_handle, key, value);
  }
  ReadOptions ro(options);
  ManagedSnapshot snapshot(this);
  ro.snapshot = snapshot.snapshot();
  return GetImpl(ro, cf_handle, key, value);
}

Status TitanDBImpl::GetImpl(const ReadOptions& options,
                            ColumnFamilyHandle* cf_handle,
                            const Slice& key, PinnableSlice* value) {
  auto snapshot = reinterpret_cast<const TitanSnapshot*>(options.snapshot);
  auto current = snapshot->current();

  Status s;
  bool is_blob_index = false;
  s = db_impl_->GetImpl(options, cf_handle, key, value,
                        nullptr /*value_found*/,
                        nullptr /*read_callback*/,
                        &is_blob_index);
  if (!s.ok() || !is_blob_index) return s;

  BlobIndex index;
  s = index.DecodeFrom(value);
  if (!s.ok()) return s;

  BlobRecord record;
  std::string buffer;
  s = current->Get(options, index, &record, &buffer);
  if (s.ok()) {
    value->Reset();
    value->PinSelf(record.value);
  }
  return s;
}

std::vector<Status> TitanDBImpl::MultiGet(
    const ReadOptions& options,
    const std::vector<ColumnFamilyHandle*>& cf_handles,
    const std::vector<Slice>& keys, std::vector<std::string>* values) {
  if (options.snapshot) {
    return MultiGetImpl(options, cf_handles, keys, values);
  }
  ReadOptions ro(options);
  ManagedSnapshot snapshot(this);
  ro.snapshot = snapshot.snapshot();
  return MultiGetImpl(ro, cf_handles, keys, values);
}

std::vector<Status> TitanDBImpl::MultiGetImpl(
    const ReadOptions& options,
    const std::vector<ColumnFamilyHandle*>& cf_handles,
    const std::vector<Slice>& keys, std::vector<std::string>* values) {
  std::vector<Status> res;
  res.reserve(keys.size());
  values->clear();
  values->reserve(keys.size());
  for (size_t i = 0; i < keys.size(); i++) {
    Status s;
    std::string value;
    PinnableSlice pinnable_value(&value);
    s = GetImpl(options, cf_handles[i], keys[i], &pinnable_value);
    res.emplace_back(s);
    values->emplace_back(value);
  }
  return res;
}

Iterator* TitanDBImpl::NewIterator(const ReadOptions& options,
                                   ColumnFamilyHandle* cf_handle) {
  std::shared_ptr<ManagedSnapshot> snapshot;
  if (options.snapshot) {
    return NewIteratorImpl(options, cf_handle, snapshot);
  }
  ReadOptions ro(options);
  snapshot.reset(new ManagedSnapshot(this));
  ro.snapshot = snapshot->snapshot();
  return NewIteratorImpl(ro, cf_handle, snapshot);
}

Iterator* TitanDBImpl::NewIteratorImpl(
    const ReadOptions& options,
    ColumnFamilyHandle* cf_handle,
    std::shared_ptr<ManagedSnapshot> snapshot) {
  assert(snapshot->snapshot() == nullptr ||
         snapshot->snapshot() == options.snapshot);

  auto cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(cf_handle)->cfd();
  std::unique_ptr<ArenaWrappedDBIter> iter(
      db_impl_->NewIteratorImpl(
          options, cfd,
          options.snapshot->GetSequenceNumber(),
          nullptr /*read_callback*/, true /*allow_blob*/));
  return new TitanDBIterator(options, snapshot, std::move(iter));
}

Status TitanDBImpl::NewIterators(
    const ReadOptions& options,
    const std::vector<ColumnFamilyHandle*>& cf_handles,
    std::vector<Iterator*>* iterators) {
  ReadOptions ro(options);
  std::shared_ptr<ManagedSnapshot> snapshot;
  if (!ro.snapshot) {
    snapshot.reset(new ManagedSnapshot(this));
    ro.snapshot = snapshot->snapshot();
  }
  iterators->clear();
  iterators->reserve(cf_handles.size());
  for (auto& cf_handle : cf_handles) {
    iterators->emplace_back(NewIteratorImpl(ro, cf_handle, snapshot));
  }
  return Status::OK();
}

const Snapshot* TitanDBImpl::GetSnapshot() {
  MutexLock l(&mutex_);
  auto current = vset_->current();
  current->Ref();
  auto snapshot = db_->GetSnapshot();
  return new TitanSnapshot(current, snapshot);
}

void TitanDBImpl::ReleaseSnapshot(const Snapshot* snapshot) {
  auto s = reinterpret_cast<const TitanSnapshot*>(snapshot);
  MutexLock l(&mutex_);
  s->current()->Unref();
  db_->ReleaseSnapshot(s->snapshot());
  delete s;
}

}  // namespace titandb
}  // namespace rocksdb
