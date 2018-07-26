#include "utilities/titandb/db.h"

#include "utilities/titandb/db_impl.h"

namespace rocksdb {
namespace titandb {

Status TitanDB::Open(const std::string& dbname,
                     const Options& options,
                     const TitanDBOptions& tdb_options,
                     TitanDB** tdb) {
  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> cf_descs = {
    ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options),
  };
  std::vector<ColumnFamilyHandle*> cf_handles;
  Status s = TitanDB::Open(dbname, db_options, tdb_options, cf_descs,
                           &cf_handles, tdb);
  if (s.ok()) {
    assert(cf_handles.size() == 1);
    // DBImpl is always holding the default handle.
    delete cf_handles[0];
  }
  return s;
}

Status TitanDB::Open(const std::string& dbname,
                     const DBOptions& db_options,
                     const TitanDBOptions& tdb_options,
                     const std::vector<ColumnFamilyDescriptor>& cf_descs,
                     std::vector<ColumnFamilyHandle*>* cf_handles,
                     TitanDB** tdb) {
  auto impl = new TitanDBImpl(dbname, db_options, tdb_options);
  auto s = impl->Open(cf_descs, cf_handles);
  if (s.ok()) {
    *tdb = impl;
  } else {
    *tdb = nullptr;
    delete impl;
  }
  return s;
}

}  // namespace titandb
}  // namespace rocksdb
