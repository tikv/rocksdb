//
// Created by 郑志铨 on 2018/8/8.
//

#ifndef ROCKSDB_BLOB_GC_PICKER_H
#define ROCKSDB_BLOB_GC_PICKER_H

#include <memory>

#include "utilities/titandb/blob_file_manager.h"
#include "utilities/titandb/blob_format.h"
#include "utilities/titandb/blob_gc.h"
#include "utilities/titandb/version.h"

#include "db/column_family.h"
#include "db/write_callback.h"
#include "include/rocksdb/status.h"
#include "util/filename.h"

namespace rocksdb {
namespace titandb {

class BlobGCPicker {
 public:
  BlobGCPicker(){};
  virtual ~BlobGCPicker(){};

  // Pick candidate blob files for a new gc.
  // Returns nullptr if there is no gc to be done.
  // Otherwise returns a pointer to a heap-allocated object that
  // describes the gc.  Caller should delete the result.
  virtual std::unique_ptr<BlobGC> PickBlobGC(BlobStorage* blob_storage) = 0;
};

class BasicBlobGCPicker final : public BlobGCPicker {
 public:
  BasicBlobGCPicker(TitanCFOptions);
  ~BasicBlobGCPicker();

  std::unique_ptr<BlobGC> PickBlobGC(BlobStorage* blob_storage) override;

 private:
  TitanCFOptions titan_cf_options_;

  // Check if blob_file needs to gc, return true means we need pick this
  // file for gc
  bool CheckForPick(const std::shared_ptr<BlobFileMeta>& blob_file,
                    const GCScore& gc_score) const;
  // Mark that blob_file is picked for gc
  // REQUIRE: call CheckForPick first
  void MarkedForPick(std::shared_ptr<BlobFileMeta> blob_file);
};

}  // namespace titandb
}  // namespace rocksdb

#endif  // ROCKSDB_BLOB_GC_PICKER_H
