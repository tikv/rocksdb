// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#pragma once
#ifndef ROCKSDB_LITE

#include <memory>
#include <string>

#include "rocksdb/env.h"

namespace rocksdb {

// Interface to manage encryption keys for files. FileSystemInspectedEnv
// will query KeyManager for the key being used for each file, and update
// KeyManager when it creates a new file or moving files around.
class FileSystemInspector {
 public:
  virtual ~FileSystemInspector() = default;

  virtual size_t Read(Env::IOType io_type, size_t len) = 0;
  virtual size_t Write(Env::IOType io_type, size_t len) = 0;
};

// An Env with underlying files being encrypted. It holds a reference to an
// external KeyManager for encryption key management.
class FileSystemInspectedEnv : public EnvWrapper {
 public:
  FileSystemInspectedEnv(Env* base_env,
                         std::shared_ptr<FileSystemInspector>& inspector);

  virtual ~FileSystemInspectedEnv();

  Status NewSequentialFile(const std::string& fname,
                           std::unique_ptr<SequentialFile>* result,
                           const EnvOptions& options) override;
  Status NewRandomAccessFile(const std::string& fname,
                             std::unique_ptr<RandomAccessFile>* result,
                             const EnvOptions& options) override;
  Status NewWritableFile(const std::string& fname,
                         std::unique_ptr<WritableFile>* result,
                         const EnvOptions& options) override;
  Status ReopenWritableFile(const std::string& fname,
                            std::unique_ptr<WritableFile>* result,
                            const EnvOptions& options) override;
  Status ReuseWritableFile(const std::string& fname,
                           const std::string& old_fname,
                           std::unique_ptr<WritableFile>* result,
                           const EnvOptions& options) override;
  Status NewRandomRWFile(const std::string& fname,
                         std::unique_ptr<RandomRWFile>* result,
                         const EnvOptions& options) override;

 private:
  const std::shared_ptr<FileSystemInspector> inspector_;
};

extern Env* NewFileSystemInspectedEnv(
    Env* base_env, std::shared_ptr<FileSystemInspector>& inspector);

}  // namespace rocksdb

#endif  // !ROCKSDB_LITE
