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

void TitanDBImpl::BGWorkGCScheduler(void* db) {
  reinterpret_cast<TitanDBImpl*>(db)->BackgroundCallGCScheduler();
}

void TitanDBImpl::BGWorkGC(void* db) {
  reinterpret_cast<TitanDBImpl*>(db)->BackgroundCallGC();
}

void TitanDBImpl::BackgroundCallGCScheduler() {
  for (;;) {
    env_->Schedule(TitanDBImpl::BGWorkGC, this, Env::LOW);
    env_->SleepForMicroseconds(5 * 1000 * 1000);
  }
}

void TitanDBImpl::BackgroundCallGC() {
  MutexLock l(&mutex_);
  auto column_family_id = PopFirstFromGCQueue();
  if (column_family_id == 0) return;
  BackgroundGC(column_family_id);
}

// TODO remove
Status TitanDBImpl::NewRandomAccessReader(
    uint64_t file_number, uint64_t readahead_size,
    std::unique_ptr<RandomAccessFileReader>* result) {
  std::unique_ptr<RandomAccessFile> file;
  auto file_name = BlobFileName(db_options_.dirname, file_number);
  Status s = env_->NewRandomAccessFile(file_name, &file, env_options_);
  if (!s.ok()) return s;

  if (readahead_size > 0) {
    file = NewReadaheadRandomAccessFile(std::move(file), readahead_size);
  }
  result->reset(new RandomAccessFileReader(std::move(file), file_name));
  return s;
}

Status TitanDBImpl::BackgroundGC(uint32_t column_family_id) {
  auto* cfh = db_impl_->GetColumnFamilyHandleUnlocked(column_family_id);

  // Build BlobGC
  std::unique_ptr<BlobGC> blob_gc;
  {
    MutexLock l(&mutex_);
    std::shared_ptr<BlobGCPicker> blob_gc_picker =
        std::make_shared<BasicBlobGCPicker>();
    blob_gc = blob_gc_picker->PickBlobGC(
        vset_->current()->GetBlobStorage(column_family_id).get());
  }
  if (!blob_gc) return Status::Corruption("Build BlobGC failed");

  BlobGCJob blob_gc_job(blob_gc.get(), db_options_,
                        titan_cfs_options_[column_family_id], env_,
                        env_options_, blob_manager_.get(), vset_.get(), this,
                        column_family_id, cfh, &mutex_);

  blob_gc_job.Prepare();

  {
    mutex_.Unlock();
    blob_gc_job.Run();
    mutex_.Lock();
  }

  blob_gc_job.Finish();

  return Status::OK();
}

}  // namespace titandb
}  // namespace rocksdb
