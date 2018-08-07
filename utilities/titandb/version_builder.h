#pragma once

#include "utilities/titandb/version.h"
#include "utilities/titandb/version_edit.h"

namespace rocksdb {
namespace titandb {

class VersionSet;

class VersionBuilder {
 public:
  // Constructs a builder to build on the base version. The
  // intermediate result is kept in the builder and the base version
  // is left unchanged.
  VersionBuilder(Version* base);

  ~VersionBuilder();

  // Applies "*edit" on the current state.
  void Apply(VersionEdit* edit);

  // Saves the current state to the version "*v".
  void SaveTo(Version* v);

 private:
  Version* base_;
  std::map<uint64_t, BlobFileMeta> added_files_;
  std::set<uint64_t> deleted_files_;
};

}  // namespace titandb
}  // namespace rocksdb
