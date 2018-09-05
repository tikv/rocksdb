#pragma once

#include "rocksdb/utilities/stackable_db.h"
#include "utilities/titandb/options.h"

namespace rocksdb {
namespace titandb {

struct TitanCFDescriptor {
  std::string name;
  TitanCFOptions options;
  TitanCFDescriptor()
      : name(kDefaultColumnFamilyName), options(TitanCFOptions()) {}
  TitanCFDescriptor(const std::string& _name, const TitanCFOptions& _options)
      : name(_name), options(_options) {}
};

class TitanDB : public StackableDB {
 public:
  static Status Open(const TitanOptions& options, const std::string& dbname,
                     TitanDB** db);

  static Status Open(const TitanDBOptions& db_options,
                     const std::string& dbname,
                     const std::vector<TitanCFDescriptor>& descs,
                     std::vector<ColumnFamilyHandle*>* handles, TitanDB** db);

  TitanDB() : StackableDB(nullptr) {}

  using StackableDB::CreateColumnFamily;
  Status CreateColumnFamily(
      const TitanCFDescriptor& desc, ColumnFamilyHandle** handle) {
    std::vector<ColumnFamilyHandle*> handles;
    Status s = CreateColumnFamilies({desc}, &handles);
    if (s.ok()) {
      *handle = handles[0];
    }
    return s;
  }

  using StackableDB::CreateColumnFamilies;
  virtual Status CreateColumnFamilies(
      const std::vector<TitanCFDescriptor>& descs,
      std::vector<ColumnFamilyHandle*>* handles) = 0;

  Status DropColumnFamily(ColumnFamilyHandle* handle) override {
    return DropColumnFamilies({handle});
  }

  virtual Status DropColumnFamilies(
      const std::vector<ColumnFamilyHandle*>& handles) = 0;

  using StackableDB::Merge;
  Status Merge(const WriteOptions&, ColumnFamilyHandle*, const Slice& /*key*/,
               const Slice& /*value*/) override {
    return Status::NotSupported("TitanDB doesn't support this operation");
  }
};

}  // namespace titandb
}  // namespace rocksdb
