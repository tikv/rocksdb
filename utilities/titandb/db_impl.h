#pragma once

#include "db/db_impl.h"
#include "utilities/titandb/db.h"
#include "utilities/titandb/version_set.h"
#include "utilities/titandb/blob_file_manager.h"

namespace rocksdb {
namespace titandb {

class TitanDBImpl : public TitanDB {
 public:
  TitanDBImpl(const std::string& dbname,
              const DBOptions& db_options,
              const TitanDBOptions& tdb_options);

  ~TitanDBImpl();

  Status Open(const std::vector<ColumnFamilyDescriptor>& cf_descs,
              std::vector<ColumnFamilyHandle*>* cf_handles);

  Status Close() override;

  using TitanDB::Get;
  Status Get(const ReadOptions& options,
             ColumnFamilyHandle* cf_handle,
             const Slice& key, PinnableSlice* value) override;

  using TitanDB::MultiGet;
  std::vector<Status> MultiGet(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& cf_handles,
      const std::vector<Slice>& keys, std::vector<std::string>* values) override;

  using TitanDB::NewIterator;
  Iterator* NewIterator(const ReadOptions& options,
                        ColumnFamilyHandle* cf_handle) override;

  Status NewIterators(const ReadOptions& options,
                      const std::vector<ColumnFamilyHandle*>& cf_handles,
                      std::vector<Iterator*>* iterators) override;

  const Snapshot* GetSnapshot() override;

  void ReleaseSnapshot(const Snapshot* snapshot) override;

 private:
  class FileManager;
  friend class FileManager;

  Status GetImpl(const ReadOptions& options,
                 ColumnFamilyHandle* cf_handle,
                 const Slice& key, PinnableSlice* value);

  std::vector<Status> MultiGetImpl(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& cf_handles,
      const std::vector<Slice>& keys, std::vector<std::string>* values);

  Iterator* NewIteratorImpl(const ReadOptions& options,
                            ColumnFamilyHandle* cf_handle,
                            std::shared_ptr<ManagedSnapshot> snapshot);

  Env* env_;
  EnvOptions env_options_;
  DBImpl* db_impl_;
  DBOptions db_options_;
  TitanDBOptions tdb_options_;

  FileLock* lock_ {nullptr};
  port::Mutex mutex_;
  std::string dbname_;
  std::string dirname_;
  VersionSet* vset_ {nullptr};
  std::set<uint64_t> pending_outputs_;
  std::shared_ptr<BlobFileManager> blob_manager_;
};

}  // namespace titandb
}  // namespace rocksdb
