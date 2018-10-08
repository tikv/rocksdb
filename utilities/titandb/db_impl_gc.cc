#include "utilities/titandb/db_impl.h"

#include "utilities/titandb/blob_file_iterator.h"
#include "utilities/titandb/blob_gc_job.h"
#include "utilities/titandb/blob_gc_picker.h"

namespace rocksdb {
namespace titandb {

void TitanDBImpl::MaybeScheduleGC() {
  if (db_options_.disable_background_gc) return;
  if (bg_gc_scheduled_.load(std::memory_order_acquire) >=
      db_options_.max_background_gc)
    return;

  bg_gc_scheduled_.fetch_add(1, std::memory_order_release);
  env_->Schedule(&TitanDBImpl::BGWorkGC, this, Env::Priority::LOW, this);
}

void TitanDBImpl::BGWorkGC(void* db) {
  reinterpret_cast<TitanDBImpl*>(db)->BackgroundCallGC();
}

void TitanDBImpl::BackgroundCallGC() {
  // Is this legal? call bg_cv_.SignalAll() maybe
  MutexLock l(&mutex_);
  assert(bg_gc_scheduled_ > 0);
  BackgroundGC();

  PurgeObsoleteFiles();

  bg_gc_scheduled_--;
  if (bg_gc_scheduled_ == 0) {
    // signal if
    // * bg_gc_scheduled_ == 0 -- need to wakeup ~TitanDBImpl
    // If none of this is true, there is no need to signal since nobody is
    // waiting for it
    bg_cv_.SignalAll();
  }
  // IMPORTANT: there should be no code after calling SignalAll. This call may
  // signal the DB destructor that it's OK to proceed with destruction. In
  // that case, all DB variables will be dealloacated and referencing them
  // will cause trouble.
}

Status TitanDBImpl::BackgroundGC() {
  mutex_.AssertHeld();

  std::unique_ptr<BlobGC> blob_gc;
  std::unique_ptr<ColumnFamilyHandle> cfh;
  Status s;

  if (!gc_queue_.empty()) {
    uint32_t column_family_id = PopFirstFromGCQueue();

    auto current = vset_->current();
    auto bs = current->GetBlobStorage(column_family_id).lock().get();
    const auto& titan_cf_options = bs->titan_cf_options();
    std::shared_ptr<BlobGCPicker> blob_gc_picker =
        std::make_shared<BasicBlobGCPicker>(titan_cf_options);
    blob_gc = blob_gc_picker->PickBlobGC(bs);

    cfh = db_impl_->GetColumnFamilyHandleUnlocked(column_family_id);
  }

  if (blob_gc) {
    assert(cfh);
    BlobGCJob blob_gc_job(blob_gc.get(), db_, cfh.get(), &mutex_, db_options_,
                          env_, env_options_, blob_manager_.get(), vset_.get());
    s = blob_gc_job.Prepare();

    if (s.ok()) {
      mutex_.Unlock();
      s = blob_gc_job.Run();
      mutex_.Lock();
    }

    if (s.ok()) {
      s = blob_gc_job.Finish();
    }
  } else {
    // Nothing to do
  }

  return s;
}

}  // namespace titandb
}  // namespace rocksdb
