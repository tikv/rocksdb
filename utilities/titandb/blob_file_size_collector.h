//
// Created by 郑志铨 on 2018/8/14.
//

#ifndef ROCKSDB_BLOB_GC_STATISTIS_H
#define ROCKSDB_BLOB_GC_STATISTIS_H

#include "include/rocksdb/listener.h"
#include "include/rocksdb/table_properties.h"
#include "util/coding.h"
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

  Status AddUserKey(const Slice& key, const Slice& value, EntryType type,
                    SequenceNumber seq, uint64_t file_size) override;
  Status Finish(UserCollectedProperties* properties) override;
  UserCollectedProperties GetReadableProperties() const override {
    return UserCollectedProperties();
  }
  const char* Name() const override { return "BlobFileSizeCollector"; }

  static bool Encode(const std::map<uint64_t, uint64_t>& blob_files_size,
                     std::string* result) {
    PutVarint64(result, blob_files_size.size());
    for (const auto& bfs : blob_files_size) {
      // TODO Maybe check return value here
      PutVarint64(result, bfs.first);
      PutVarint64(result, bfs.second);
    }
    return true;
  }
  static bool Decode(const std::string& buffer,
                     std::map<uint64_t, uint64_t>* blob_files_size) {
    Slice slice{buffer};
    uint64_t num = 0;
    GetVarint64(&slice, &num);
    uint64_t file_number;
    uint64_t size;
    for (uint32_t i = 0; i < num; ++i) {
      // TODO Maybe check return value here
      GetVarint64(&slice, &file_number);
      GetVarint64(&slice, &size);
      (*blob_files_size)[file_number] = size;
    }
    return true;
  }

 private:
  std::map<uint64_t, uint64_t> blob_files_size_;
};

class BlobDiscardableSizeListener final : public EventListener {
 public:
  BlobDiscardableSizeListener(port::Mutex* mutex, VersionSet* versions);
  ~BlobDiscardableSizeListener();

  void OnCompactionCompleted(DB* db, const CompactionJobInfo& ci) override;

 private:
  port::Mutex* db_mutex_;
  VersionSet* versions_;
};

}  // namespace titandb
}  // namespace rocksdb

#endif  // ROCKSDB_BLOB_GC_STATISTIS_H
