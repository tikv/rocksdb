#pragma once

#include "rocksdb/utilities/stackable_db.h"
#include "utilities/titandb/options.h"

namespace rocksdb {
namespace titandb {

class TitanDB : public StackableDB {
 public:
  static Status Open(const std::string& dbname,
                     const Options& options,
                     const TitanDBOptions& tdb_options,
                     TitanDB** tdb);

  static Status Open(const std::string& dbname,
                     const DBOptions& db_options,
                     const TitanDBOptions& tdb_options,
                     const std::vector<ColumnFamilyDescriptor>& cf_descs,
                     std::vector<ColumnFamilyHandle*>* cf_handles,
                     TitanDB** tdb);

  TitanDB() : StackableDB(nullptr) {}

  using StackableDB::Merge;
  Status Merge(const WriteOptions&, ColumnFamilyHandle*,
               const Slice& /*key*/, const Slice& /*value*/) override {
    return Status::NotSupported("TitanDB doesn't support this operation");
  }
};

}  // namespace titandb
}  // namespace rocksdb
