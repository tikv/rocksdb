//
// Created by 郑志铨 on 2018/8/14.
//

#include "utilities/titandb/blob_file_size_collector.h"
#include <db/db_impl.h>
#include <monitoring/instrumented_mutex.h>
#include "utilities/titandb/blob_format.h"
#include "utilities/titandb/version_set.h"

namespace rocksdb {
namespace titandb {

TablePropertiesCollector*
BlobFileSizeCollectorFactory::CreateTablePropertiesCollector(
    rocksdb::TablePropertiesCollectorFactory::Context /* context */) {
  return new BlobFileSizeCollector();
}

const std::string BlobFileSizeCollector::PROPERTIES_NAME =
    "TitanDB.blob_discardable_size";

Status BlobFileSizeCollector::AddUserKey(const Slice& /* key */,
                                         const Slice& value, EntryType type,
                                         SequenceNumber /* seq */,
                                         uint64_t /* file_size */) {
  if (type != kEntryBlobIndex) return Status::OK();

  BlobIndex index;
  auto s = index.DecodeFrom(const_cast<Slice*>(&value));
  if (!s.ok()) return s;

  auto iter = blob_files_size_.find(index.file_number);
  if (iter == blob_files_size_.end()) {
    blob_files_size_[index.file_number] = index.blob_handle.size;
  } else {
    iter->second += index.blob_handle.size;
  }


  return Status::OK();
}

Status BlobFileSizeCollector::Finish(UserCollectedProperties* properties) {
  std::string res;
  Encode(blob_files_size_, &res);
  *properties = UserCollectedProperties{{PROPERTIES_NAME, res}};
  return Status::OK();
}

BlobDiscardableSizeListener::BlobDiscardableSizeListener(port::Mutex* mutex,
                                                         VersionSet* versions)
    : db_mutex_(mutex), versions_(versions) {}

BlobDiscardableSizeListener::~BlobDiscardableSizeListener() {}

void BlobDiscardableSizeListener::OnCompactionCompleted(
    rocksdb::DB* /* db */, const CompactionJobInfo& ci) {
  std::map<uint64_t, int64_t> blob_files_size;
  for (const auto& f : ci.input_files) {
    auto iter = ci.table_properties.find(f);
    if (iter == ci.table_properties.end()) {
      continue;
    }
    auto uiter = iter->second->user_collected_properties.find(
        BlobFileSizeCollector::PROPERTIES_NAME);
    if (uiter == iter->second->user_collected_properties.end()) continue;
    std::map<uint64_t, uint64_t> tmp;
    std::string s = uiter->second;
    BlobFileSizeCollector::Decode(s, &tmp);
    for (const auto& bfs : tmp) {
      auto bfsiter = blob_files_size.find(bfs.first);
      if (bfsiter == blob_files_size.end())
        blob_files_size[bfs.first] = -bfs.second;
      else
        bfsiter->second -= bfs.second;
    }
  }
  for (const auto& f : ci.output_files) {
    auto iter = ci.table_properties.find(f);
    if (iter == ci.table_properties.end()) {
      continue;
    }
    auto uiter = iter->second->user_collected_properties.find(
        BlobFileSizeCollector::PROPERTIES_NAME);
    if (uiter == iter->second->user_collected_properties.end()) continue;
    std::map<uint64_t, uint64_t> tmp;
    std::string s = uiter->second;
    BlobFileSizeCollector::Decode(s, &tmp);
    for (const auto& bfs : tmp) {
      auto bfsiter = blob_files_size.find(bfs.first);
      if (bfsiter == blob_files_size.end())
        blob_files_size[bfs.first] = bfs.second;
      else
        bfsiter->second += bfs.second;
    }
  }

  {
    MutexLock l(db_mutex_);
    Version* current = versions_->current();
    current->Ref();
    auto& files = *current->GetBlobStorage(ci.cf_id)->mutable_files();
    for (const auto& bfs : blob_files_size) {
      if (bfs.second > 0) {
        continue;
      }

      auto iter = files.find(bfs.first);
      if (iter == files.end()) continue;

      iter->second->discardable_size += -bfs.second;
    }
    current->Unref();
  }
}

}  // namespace titandb
}  // namespace rocksdb
