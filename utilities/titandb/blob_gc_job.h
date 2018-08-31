//
// Created by 郑志铨 on 2018/8/18.
//

#ifndef ROCKSDB_BLOB_GC_JOB_H
#define ROCKSDB_BLOB_GC_JOB_H

#include <db/db_impl.h>
#include "include/rocksdb/status.h"
#include "table/internal_iterator.h"
#include "utilities/titandb/blob_file_builder.h"
#include "utilities/titandb/blob_file_manager.h"
#include "utilities/titandb/blob_gc.h"
#include "utilities/titandb/options.h"
#include "utilities/titandb/version_set.h"

namespace rocksdb {
namespace titandb {

class BlobGCJob {
 public:
  BlobGCJob(BlobGC* blob_gc, DB* db, ColumnFamilyHandle* cfh,
            port::Mutex* mutex, const TitanDBOptions& titan_db_options,
            const TitanCFOptions& titan_cf_options, Env* env,
            const EnvOptions& env_options, BlobFileManager* blob_file_manager,
            VersionSet* version_set);
  ~BlobGCJob();

  // REQUIRE: mutex held
  Status Prepare();
  // REQUIRE: mutex not held
  Status Run();
  // REQUIRE: mutex held
  Status Finish();

 private:
  class GarbageCollectionWriteCallback;
  class PlainInternalKeyComparator;
  friend class BlobGCJobTest;

  Status SampleCandidateFiles();

  bool SampleOne(const std::shared_ptr<BlobFileMeta>& file);

  Status DoRunGC();

  Status BuildIterator(std::unique_ptr<InternalIterator>* result);

  bool DiscardEntry(const Slice& key, const BlobIndex& blob_index);

  BlobGC* blob_gc_;

  DB* base_db_;
  DBImpl* base_db_impl_;
  ColumnFamilyHandle* cfh_;
  port::Mutex* tdb_mutex_;
  TitanDBOptions titan_db_options_;
  TitanCFOptions titan_cf_options_;
  Env* env_;
  EnvOptions env_options_;
  BlobFileManager* blob_file_manager_;
  titandb::VersionSet* version_set_;

  std::vector<std::pair<std::unique_ptr<BlobFileHandle>,
                        std::unique_ptr<BlobFileBuilder>>>
      blob_file_builders_;
  std::vector<std::pair<WriteBatch, GarbageCollectionWriteCallback>>
      rewrite_batches_;
};

}  // namespace titandb
}  // namespace rocksdb

#endif  // ROCKSDB_BLOB_GC_JOB_H
