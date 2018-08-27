#pragma once

#include "db/db_impl.h"
#include "utilities/titandb/blob_file_manager.h"
#include "utilities/titandb/titan_db.h"
#include "utilities/titandb/version_set.h"

namespace rocksdb {
namespace titandb {

class TitanDBImpl : public TitanDB {
 public:
  TitanDBImpl(const TitanDBOptions& options, const std::string& dbname);

  ~TitanDBImpl();

  Status Open(const std::vector<TitanCFDescriptor>& descs,
              std::vector<ColumnFamilyHandle*>* handles);

  Status Close() override;

  using TitanDB::CreateColumnFamilies;
  Status CreateColumnFamilies(
      const std::vector<TitanCFDescriptor>& descs,
      std::vector<ColumnFamilyHandle*>* handles) override;

  Status DropColumnFamilies(
      const std::vector<ColumnFamilyHandle*>& handles) override;

  using TitanDB::Get;
  Status Get(const ReadOptions& options, ColumnFamilyHandle* handle,
             const Slice& key, PinnableSlice* value) override;

  using TitanDB::MultiGet;
  std::vector<Status> MultiGet(const ReadOptions& options,
                               const std::vector<ColumnFamilyHandle*>& handles,
                               const std::vector<Slice>& keys,
                               std::vector<std::string>* values) override;

  using TitanDB::NewIterator;
  Iterator* NewIterator(const ReadOptions& options,
                        ColumnFamilyHandle* handle) override;

  Status NewIterators(const ReadOptions& options,
                      const std::vector<ColumnFamilyHandle*>& handles,
                      std::vector<Iterator*>* iterators) override;

  const Snapshot* GetSnapshot() override;

  void ReleaseSnapshot(const Snapshot* snapshot) override;

 private:
  class FileManager;
  friend class FileManager;
  friend class BlobGCJobTest;

  Status GetImpl(const ReadOptions& options, ColumnFamilyHandle* handle,
                 const Slice& key, PinnableSlice* value);

  std::vector<Status> MultiGetImpl(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& handles,
      const std::vector<Slice>& keys, std::vector<std::string>* values);

  Iterator* NewIteratorImpl(const ReadOptions& options,
                            ColumnFamilyHandle* handle,
                            std::shared_ptr<ManagedSnapshot> snapshot);

  Status NewRandomAccessReader(uint64_t file_number, uint64_t readahead_size,
                               std::unique_ptr<RandomAccessFileReader>* result);

  Status BackgroundGC(uint32_t column_family_id);

  void AddToGCQueue(uint32_t column_family_id) {
    if (pending_gc_.find(column_family_id) != pending_gc_.end()) return;
    gc_queue_.push_back(column_family_id);
    pending_gc_.insert(column_family_id);
  }

  uint32_t PopFirstFromGCQueue() {
    if (gc_queue_.empty()) {
      assert(pending_gc_.empty());
      return 0;
    }
    auto column_family_id = *gc_queue_.begin();
    gc_queue_.pop_front();
    pending_gc_.erase(column_family_id);
    return column_family_id;
  }

  static void BGWorkGCScheduler(void* db);
  static void BGWorkGC(void* db);
  void BackgroundCallGCScheduler();
  void BackgroundCallGC();

  FileLock* lock_{nullptr};
  port::Mutex mutex_;
  std::string dbname_;
  std::string dirname_;
  Env* env_;
  EnvOptions env_options_;
  DBImpl* db_impl_;
  TitanDBOptions db_options_;
  // TODO Need to deal with cf destroy and options update
  std::map<uint32_t, TitanCFOptions> titan_cfs_options_;

  std::unique_ptr<VersionSet> vset_;
  std::set<uint64_t> pending_outputs_;
  std::shared_ptr<BlobFileManager> blob_manager_;

  // gc_queue_ hold column families that we need to gc.
  // pending_gc_ hold column families that already on gc_queue_.
  std::deque<uint32_t> gc_queue_;
  std::set<uint32_t> pending_gc_;
};

}  // namespace titandb
}  // namespace rocksdb
