#pragma once

#include "db/db_impl.h"
#include "utilities/titandb/db.h"
#include "utilities/titandb/version_set.h"
#include "utilities/titandb/blob_file_manager.h"

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
  Status Get(const ReadOptions& options,
             ColumnFamilyHandle* handle,
             const Slice& key, PinnableSlice* value) override;

  using TitanDB::MultiGet;
  std::vector<Status> MultiGet(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& handles,
      const std::vector<Slice>& keys, std::vector<std::string>* values) override;

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

  Status GetImpl(const ReadOptions& options,
                 ColumnFamilyHandle* handle,
                 const Slice& key, PinnableSlice* value);

  std::vector<Status> MultiGetImpl(
      const ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& handles,
      const std::vector<Slice>& keys, std::vector<std::string>* values);

  Iterator* NewIteratorImpl(const ReadOptions& options,
                            ColumnFamilyHandle* handle,
                            std::shared_ptr<ManagedSnapshot> snapshot);

  FileLock* lock_ {nullptr};
  port::Mutex mutex_;
  std::string dbname_;
  std::string dirname_;
  Env* env_;
  EnvOptions env_options_;
  DBImpl* db_impl_;
  TitanDBOptions db_options_;

  std::unique_ptr<VersionSet> vset_;
  std::set<uint64_t> pending_outputs_;
  std::shared_ptr<BlobFileManager> blob_manager_;
};

}  // namespace titandb
}  // namespace rocksdb
