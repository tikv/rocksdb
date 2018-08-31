//
// Created by 郑志铨 on 2018/8/20.
//

#include "utilities/titandb/titan_db_impl.h"

#include "blob_file_builder.h"
#include "blob_file_iterator.h"
#include "blob_gc_job.h"
#include "table/merging_iterator.h"
#include "utilities/titandb/blob_file_size_collector.h"
#include "utilities/titandb/blob_gc.h"
#include "utilities/titandb/blob_gc_picker.h"
#include "utilities/titandb/db_iter.h"
#include "utilities/titandb/table_factory.h"

namespace rocksdb {
namespace titandb {

void TitanDBImpl::MaybeScheduleGC() {
  if (db_options_.disable_background_gc)
    return;
  bg_gc_scheduled_++;
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
  Status s;
  if (!gc_queue_.empty()) {
    uint32_t column_family_id = PopFirstFromGCQueue();
    auto* cfh = db_impl_->GetColumnFamilyHandleUnlocked(column_family_id);
    assert(cfh != nullptr);

    // Build BlobGC
    std::unique_ptr<BlobGC> blob_gc;
    std::shared_ptr<BlobGCPicker> blob_gc_picker =
        std::make_shared<BasicBlobGCPicker>(
            titan_cfs_options_[column_family_id]);
    blob_gc = blob_gc_picker->PickBlobGC(
        vset_->current()->GetBlobStorage(column_family_id).get());
    if (!blob_gc) return Status::Corruption("Build BlobGC failed");

    BlobGCJob blob_gc_job(blob_gc.get(), db_, cfh, &mutex_, db_options_,
                          titan_cfs_options_[column_family_id], env_,
                          env_options_, blob_manager_.get(), vset_.get());
    s = blob_gc_job.Prepare();
    if (!s.ok()) return s;

    {
      mutex_.Unlock();
      s = blob_gc_job.Run();
      mutex_.Lock();
    }
    if (!s.ok()) return s;

    s = blob_gc_job.Finish();
    if (!s.ok()) return s;
  }

  return Status::OK();
}

}  // namespace titandb
}  // namespace rocksdb
