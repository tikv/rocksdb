#pragma once

namespace rocksdb {
namespace titandb {

class ReadContext {
 public:
  ReadContext(DB* db,
              VersionSet* vset,
              port::Mutex* mutex,
              const ReadOptions& ropts)
      : db_(db),
        mutex_(mutex),
        options_(ropts) {
    if (!options_.snapshot) {
      snapshot_ = db_->GetSnapshot();
      options_.snapshot = snapshot_;
    }
    MutexLock l(mutex_);
    current_ = vset->current();
    current_->Ref();
  }

  ~ReadContext() {
    if (snapshot_) {
      db_->ReleaseSnapshot(snapshot_);
    }
    MutexLock l(mutex_);
    current_->Unref();
  }

  Version* current() const { return current_; }

  const ReadOptions& options() const { return options_; }

 private:
  DB* db_;
  Version* current_;
  port::Mutex* mutex_;
  ReadOptions options_;
  const Snapshot* snapshot_ {nullptr};
};

}  // namespace titandb
}  // namespace rocksdb
