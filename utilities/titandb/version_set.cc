#include "utilities/titandb/version_set.h"

#include <inttypes.h>

#include "util/filename.h"

namespace rocksdb {
namespace titandb {

VersionSet::VersionSet(const DBOptions& db_options,
                       const TitanDBOptions& tdb_options)
    : dirname_(tdb_options.dirname),
      env_(db_options.env),
      db_options_(db_options),
      env_options_(db_options),
      tdb_options_(tdb_options),
      version_list_(nullptr) {
  AppendVersion(new Version(this));
  file_cache_.reset(new BlobFileCache(db_options, tdb_options));
}

VersionSet::~VersionSet() {
  current_->Unref();
  assert(version_list_.next_ == &version_list_);
  assert(version_list_.prev_ == &version_list_);
}

Status VersionSet::Open() {
  Status s = env_->FileExists(CurrentFileName(dirname_));
  if (s.ok()) {
    return Recover();
  }
  if (!s.IsNotFound()) {
    return s;
  }
  if (!db_options_.create_if_missing) {
    return Status::InvalidArgument(
        dirname_, "does't exist (create_if_missing is false)");
  }
  return OpenManifest(NewFileNumber());
}

Status VersionSet::Recover() {
  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    void Corruption(size_t, const Status& s) override {
      if (status->ok()) *status = s;
    }
  };

  // Reads "CURRENT" file, which contains the name of the current manifest file.
  std::string manifest;
  Status s = ReadFileToString(env_, CurrentFileName(dirname_), &manifest);
  if (!s.ok()) return s;
  if (manifest.empty() || manifest.back() != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  manifest.resize(manifest.size() - 1);

  // Opens the current manifest file.
  auto file_name = dirname_ + "/" + manifest;
  std::unique_ptr<SequentialFileReader> file;
  {
    std::unique_ptr<SequentialFile> f;
    s = env_->NewSequentialFile(file_name, &f,
                                env_->OptimizeForManifestRead(env_options_));
    if (!s.ok()) return s;
    file.reset(new SequentialFileReader(std::move(f), file_name));
  }

  bool has_next_file_number = false;
  uint64_t next_file_number = 0;

  // Reads edits from the manifest and applies them one by one.
  VersionBuilder builder(current_);
  {
    LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(nullptr, std::move(file), &reporter,
                       true /*checksum*/, 0 /*initial_offset*/, 0);
    Slice record;
    std::string scratch;
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
      VersionEdit edit;
      s = DecodeInto(record, &edit);
      if (!s.ok()) return s;
      builder.Apply(&edit);
      if (edit.has_next_file_number_) {
        next_file_number = edit.next_file_number_;
        has_next_file_number = true;
      }
    }
  }

  if (!has_next_file_number) {
    return Status::Corruption("no next file number in manifest file");
  }
  next_file_number_.store(next_file_number);

  Version* v = new Version(this);
  {
    builder.SaveTo(v);
    AppendVersion(v);
  }
  return OpenManifest(NewFileNumber());
}

Status VersionSet::OpenManifest(uint64_t file_number) {
  Status s;

  auto file_name = DescriptorFileName(dirname_, file_number);
  std::unique_ptr<WritableFileWriter> file;
  {
    std::unique_ptr<WritableFile> f;
    s = env_->NewWritableFile(file_name, &f, env_options_);
    if (!s.ok()) return s;
    file.reset(new WritableFileWriter(std::move(f), env_options_));
  }

  manifest_log_.reset(new log::Writer(std::move(file), 0, false));

  // Saves current snapshot
  s = WriteSnapshot(manifest_log_.get());
  if (s.ok()) {
    ImmutableDBOptions ioptions(db_options_);
    s = SyncManifest(env_, &ioptions, manifest_log_->file());
  }
  if (s.ok()) {
    // Makes "CURRENT" file that points to the new manifest file.
    s = SetCurrentFile(env_, dirname_, file_number, nullptr);
  }

  if (!s.ok()) {
    manifest_log_.reset();
    env_->DeleteFile(file_name);
  }
  return s;
}

Status VersionSet::WriteSnapshot(log::Writer* log) {
  VersionEdit edit;
  edit.SetNextFileNumber(next_file_number_.load());
  for (auto& file : current_->files_) {
    edit.AddBlobFile(file.second);
  }
  std::string record;
  edit.EncodeTo(&record);
  return log->AddRecord(record);
}

Status VersionSet::LogAndApply(VersionEdit* edit, port::Mutex* mutex) {
  mutex->AssertHeld();

  // TODO(@huachao): write manifest file unlocked
  std::string record;
  edit->SetNextFileNumber(next_file_number_.load());
  edit->EncodeTo(&record);
  Status s = manifest_log_->AddRecord(record);
  if (s.ok()) {
    ImmutableDBOptions ioptions(db_options_);
    s = SyncManifest(env_, &ioptions, manifest_log_->file());
  }
  if (!s.ok()) return s;

  Version* v = new Version(this);
  {
    VersionBuilder builder(current_);
    builder.Apply(edit);
    builder.SaveTo(v);
    AppendVersion(v);
  }
  return s;
}

void VersionSet::AppendVersion(Version* v) {
  assert(v->refs_ == 0);
  assert(v != current_);

  if (current_) {
    current_->Unref();
  }
  current_ = v;
  current_->Ref();

  v->next_ = &version_list_;
  v->prev_ = version_list_.prev_;
  v->next_->prev_ = v;
  v->prev_->next_ = v;
}

uint64_t VersionSet::NewFileNumber() {
  return next_file_number_.fetch_add(1);
}

}  // namespace titandb
}  // namespace rocksdb
