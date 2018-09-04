#include "utilities/titandb/blob_file_size_collector.h"

namespace rocksdb {
namespace titandb {

TablePropertiesCollector*
BlobFileSizeCollectorFactory::CreateTablePropertiesCollector(
    rocksdb::TablePropertiesCollectorFactory::Context /* context */) {
  return new BlobFileSizeCollector();
}

const std::string BlobFileSizeCollector::PROPERTIES_NAME =
    "TitanDB.blob_discardable_size";

bool BlobFileSizeCollector::Encode(
    const std::map<uint64_t, uint64_t>& blob_files_size, std::string* result) {
  PutVarint32(result, static_cast<uint32_t>(blob_files_size.size()));
  for (const auto& bfs : blob_files_size) {
    PutVarint64(result, bfs.first);
    PutVarint64(result, bfs.second);
  }
  return true;
}
bool BlobFileSizeCollector::Decode(
    Slice* slice, std::map<uint64_t, uint64_t>* blob_files_size) {
  uint32_t num = 0;
  if (!GetVarint32(slice, &num)) {
    return false;
  }
  uint64_t file_number;
  uint64_t size;
  for (uint32_t i = 0; i < num; ++i) {
    if (!GetVarint64(slice, &file_number)) {
      return false;
    }
    if (!GetVarint64(slice, &size)) {
      return false;
    }
    (*blob_files_size)[file_number] = size;
  }
  return true;
}

Status BlobFileSizeCollector::AddUserKey(const Slice& /* key */,
                                         const Slice& value, EntryType type,
                                         SequenceNumber /* seq */,
                                         uint64_t /* file_size */) {
  if (type != kEntryBlobIndex) {
    return Status::OK();
  }

  BlobIndex index;
  auto s = index.DecodeFrom(const_cast<Slice*>(&value));
  if (!s.ok()) {
    return s;
  }

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

BlobDiscardableSizeListener::BlobDiscardableSizeListener(TitanDBImpl* db,
                                                         port::Mutex* db_mutex,
                                                         VersionSet* versions)
    : db_(db), db_mutex_(db_mutex), versions_(versions) {}

BlobDiscardableSizeListener::~BlobDiscardableSizeListener() {}

void BlobDiscardableSizeListener::OnCompactionCompleted(
    rocksdb::DB* /* db */, const CompactionJobInfo& ci) {
  std::map<uint64_t, int64_t> blob_files_size;
  auto calc_bfs = [&ci, &blob_files_size](const std::vector<std::string>& files,
                                          int coefficient) {
    for (const auto& file : files) {
      auto tp_iter = ci.table_properties.find(file);
      if (tp_iter == ci.table_properties.end()) {
        continue;
      }
      auto ucp_iter = tp_iter->second->user_collected_properties.find(
          BlobFileSizeCollector::PROPERTIES_NAME);
      if (ucp_iter == tp_iter->second->user_collected_properties.end()) {
        continue;
      }
      std::map<uint64_t, uint64_t> input_blob_files_size;
      std::string s = ucp_iter->second;
      Slice slice{s};
      BlobFileSizeCollector::Decode(&slice, &input_blob_files_size);
      for (const auto& input_bfs : input_blob_files_size) {
        auto bfs_iter = blob_files_size.find(input_bfs.first);
        if (bfs_iter == blob_files_size.end()) {
          blob_files_size[input_bfs.first] = coefficient * input_bfs.second;
        } else {
          bfs_iter->second += coefficient * input_bfs.second;
        }
      }
    }
  };

  calc_bfs(ci.input_files, -1);
  calc_bfs(ci.output_files, 1);

  {
    MutexLock l(db_mutex_);
    Version* current = versions_->current();
    current->Ref();
    auto bs = current->GetBlobStorage(ci.cf_id).lock();
    for (const auto& bfs : blob_files_size) {
      // blob file size < 0 means discardable size > 0
      if (bfs.second > 0) {
        continue;
      }
      auto file = bs->FindFile(bfs.first).lock();
      file->discardable_size += static_cast<uint64_t>(-bfs.second);
    }
    bs->ComputeGCScore();
    current->Unref();
    if (db_ != nullptr) {
      db_->AddToGCQueue(ci.cf_id);
      db_->MaybeScheduleGC();
    }
  }
}

}  // namespace titandb
}  // namespace rocksdb
