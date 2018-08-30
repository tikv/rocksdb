//
// Created by 郑志铨 on 2018/8/14.
//

#ifndef ROCKSDB_BLOB_GC_STATISTIS_H
#define ROCKSDB_BLOB_GC_STATISTIS_H

#include "include/rocksdb/listener.h"
#include "include/rocksdb/table_properties.h"
#include "util/coding.h"
#include "utilities/titandb/titan_db_impl.h"
#include "utilities/titandb/version.h"
#include "utilities/titandb/version_set.h"

namespace rocksdb {
namespace titandb {

class BlobFileSizeCollectorFactory final
    : public TablePropertiesCollectorFactory {
 public:
  TablePropertiesCollector* CreateTablePropertiesCollector(
      TablePropertiesCollectorFactory::Context context) override;

  const char* Name() const override { return "BlobFileSizeCollector"; }
};

class BlobFileSizeCollector final : public TablePropertiesCollector {
 public:
  const static std::string PROPERTIES_NAME;

  static bool Encode(const std::map<uint64_t, uint64_t>& blob_files_size,
                     std::string* result);
  static bool Decode(Slice* slice,
                     std::map<uint64_t, uint64_t>* blob_files_size);

  Status AddUserKey(const Slice& key, const Slice& value, EntryType type,
                    SequenceNumber seq, uint64_t file_size) override;
  Status Finish(UserCollectedProperties* properties) override;
  UserCollectedProperties GetReadableProperties() const override {
    return UserCollectedProperties();
  }
  const char* Name() const override { return "BlobFileSizeCollector"; }

 private:
  std::map<uint64_t, uint64_t> blob_files_size_;
};

class BlobDiscardableSizeListener final : public EventListener {
 public:
  BlobDiscardableSizeListener(TitanDBImpl* db, port::Mutex* db_mutex,
                              VersionSet* versions);
  ~BlobDiscardableSizeListener();

  void OnCompactionCompleted(DB* db, const CompactionJobInfo& ci) override;

 private:
  TitanDBImpl* db_;
  port::Mutex* db_mutex_;
  VersionSet* versions_;
};

}  // namespace titandb
}  // namespace rocksdb

#endif  // ROCKSDB_BLOB_GC_STATISTIS_H
