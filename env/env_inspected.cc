// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#ifndef ROCKSDB_LITE
#include "rocksdb/env_inspected.h"

namespace rocksdb {

class InspectedSequentialFile : public SequentialFileWrapper {
 public:
  InspectedSequentialFile(std::unique_ptr<SequentialFile>&& target,
                          FileSystemInspector* inspector)
      : SequentialFileWrapper(target.get()),
        owner_(std::move(target)),
        inspector_(inspector) {}

  Status Read(size_t n, Slice* result, char* scratch) override {
    assert(inspector_);
    Status s;
    size_t offset = 0;
    size_t allowed = 0;
    while (offset < n) {
      allowed = inspector_->Read(n - offset);
      if (allowed > 0) {
        s = SequentialFileWrapper::Read(allowed, result, scratch + offset);
        if (!s.ok()) {
          break;
        }
        size_t actual_read = result->size();
        if (result->data() != scratch + offset) {
          memcpy(scratch + offset, result->data(), actual_read);
        }
        offset += actual_read;
        if (actual_read < allowed) {
          break;
        }
      } else {
        s = Status::IOError("Failed file system inspection");
        break;
      }
    }
    *result = Slice(scratch, offset);
    return s;
  }

  Status PositionedRead(uint64_t offset, size_t n, Slice* result,
                        char* scratch) override {
    assert(inspector_);
    Status s;
    size_t roffset = 0;
    size_t allowed = 0;
    while (roffset < n) {
      allowed = inspector_->Read(n - roffset);
      if (allowed > 0) {
        s = SequentialFileWrapper::PositionedRead(offset + roffset, allowed,
                                                  result, scratch + roffset);
        if (!s.ok()) {
          break;
        }
        size_t actual_read = result->size();
        if (result->data() != scratch + roffset) {
          memcpy(scratch + roffset, result->data(), actual_read);
        }
        roffset += actual_read;
        if (actual_read < allowed) {
          break;
        }
      } else {
        s = Status::IOError("Failed file system inspection");
        break;
      }
    }
    *result = Slice(scratch, roffset);
    return s;
  }

 private:
  std::unique_ptr<SequentialFile> owner_;
  FileSystemInspector* inspector_;
};

class InspectedRandomAccessFile : public RandomAccessFileWrapper {
 public:
  InspectedRandomAccessFile(std::unique_ptr<RandomAccessFile>&& target,
                            FileSystemInspector* inspector)
      : RandomAccessFileWrapper(target.get()),
        owner_(std::move(target)),
        inspector_(inspector) {}

  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    assert(inspector_);
    Status s;
    size_t roffset = 0;
    size_t allowed = 0;
    while (roffset < n) {
      allowed = inspector_->Read(n - roffset);
      if (allowed > 0) {
        s = RandomAccessFileWrapper::Read(offset + roffset, allowed, result,
                                          scratch + roffset);
        if (!s.ok()) {
          break;
        }
        size_t actual_read = result->size();
        if (result->data() != scratch + roffset) {
          memcpy(scratch + roffset, result->data(), actual_read);
        }
        roffset += actual_read;
        if (actual_read < allowed) {
          break;
        }
      } else {
        s = Status::IOError("Failed file system inspection");
        break;
      }
    }
    *result = Slice(scratch, roffset);
    return s;
  }

  Status MultiRead(ReadRequest* reqs, size_t num_reqs) override {
    assert(reqs != nullptr);
    for (size_t i = 0; i < num_reqs; ++i) {
      ReadRequest& req = reqs[i];
      req.status = Read(req.offset, req.len, &req.result, req.scratch);
    }
    return Status::OK();
  }

 private:
  std::unique_ptr<RandomAccessFile> owner_;
  FileSystemInspector* inspector_;
};

class InspectedWritableFile : public WritableFileWrapper {
 public:
  InspectedWritableFile(std::unique_ptr<WritableFile>&& target,
                        FileSystemInspector* inspector)
      : WritableFileWrapper(target.get()),
        owner_(std::move(target)),
        inspector_(inspector) {}

  Status Append(const Slice& data) override {
    assert(inspector_);
    Status s;
    size_t size = data.size();
    size_t offset = 0;
    size_t allowed = 0;
    while (offset < size) {
      allowed = inspector_->Write(size - offset);
      if (allowed > 0) {
        s = WritableFileWrapper::Append(Slice(data.data() + offset, allowed));
        if (!s.ok()) {
          break;
        }
      } else {
        s = Status::IOError("Failed file system inspection");
        break;
      }
      offset += allowed;
    }
    return s;
  }

  Status PositionedAppend(const Slice& data, uint64_t offset) override {
    assert(inspector_);
    Status s;
    size_t size = data.size();
    size_t roffset = 0;
    size_t allowed = 0;
    while (roffset < size) {
      allowed = inspector_->Write(size - roffset);
      if (allowed > 0) {
        s = WritableFileWrapper::PositionedAppend(
            Slice(data.data() + roffset, allowed), offset + roffset);
        if (!s.ok()) {
          break;
        }
      } else {
        s = Status::IOError("Failed file system inspection");
        break;
      }
      roffset += allowed;
    }
    return s;
  }

 private:
  std::unique_ptr<WritableFile> owner_;
  FileSystemInspector* inspector_;
};

class InspectedRandomRWFile : public RandomRWFileWrapper {
 public:
  InspectedRandomRWFile(std::unique_ptr<RandomRWFile>&& target,
                        FileSystemInspector* inspector)
      : RandomRWFileWrapper(target.get()),
        owner_(std::move(target)),
        inspector_(inspector) {}

  Status Write(uint64_t offset, const Slice& data) override {
    assert(inspector_);
    Status s;
    size_t size = data.size();
    size_t roffset = 0;
    size_t allowed = 0;
    while (roffset < size) {
      allowed = inspector_->Write(size - roffset);
      if (allowed > 0) {
        s = RandomRWFileWrapper::Write(offset + roffset,
                                       Slice(data.data() + roffset, allowed));
        if (!s.ok()) {
          break;
        }
      } else {
        s = Status::IOError("Failed file system inspection");
        break;
      }
      roffset += allowed;
    }
    return s;
  }

  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    assert(inspector_);
    Status s;
    size_t roffset = 0;
    size_t allowed = 0;
    while (roffset < n) {
      allowed = inspector_->Read(n - roffset);
      if (allowed > 0) {
        s = RandomRWFileWrapper::Read(offset + roffset, allowed, result,
                                      scratch + roffset);
        if (!s.ok()) {
          return s;
        }
        size_t actual_read = result->size();
        if (result->data() != scratch + roffset) {
          memcpy(scratch + roffset, result->data(), actual_read);
        }
        roffset += actual_read;
        if (actual_read < allowed) {
          break;
        }
      } else {
        s = Status::IOError("Failed file system inspection");
        break;
      }
    }
    *result = Slice(scratch, roffset);
    return s;
  }

 private:
  std::unique_ptr<RandomRWFile> owner_;
  FileSystemInspector* inspector_;
};

FileSystemInspectedEnv::FileSystemInspectedEnv(
    Env* base_env, std::shared_ptr<FileSystemInspector>& inspector)
    : EnvWrapper(base_env), inspector_(inspector) {}

Status FileSystemInspectedEnv::NewSequentialFile(
    const std::string& fname, std::unique_ptr<SequentialFile>* result,
    const EnvOptions& options) {
  auto s = EnvWrapper::NewSequentialFile(fname, result, options);
  if (!s.ok()) {
    return s;
  }
  result->reset(
      new InspectedSequentialFile(std::move(*result), inspector_.get()));
  return s;
}

Status FileSystemInspectedEnv::NewRandomAccessFile(
    const std::string& fname, std::unique_ptr<RandomAccessFile>* result,
    const EnvOptions& options) {
  auto s = EnvWrapper::NewRandomAccessFile(fname, result, options);
  if (!s.ok()) {
    return s;
  }
  result->reset(
      new InspectedRandomAccessFile(std::move(*result), inspector_.get()));
  return s;
}

Status FileSystemInspectedEnv::NewWritableFile(
    const std::string& fname, std::unique_ptr<WritableFile>* result,
    const EnvOptions& options) {
  auto s = EnvWrapper::NewWritableFile(fname, result, options);
  if (!s.ok()) {
    return s;
  }
  result->reset(
      new InspectedWritableFile(std::move(*result), inspector_.get()));
  return s;
}

Status FileSystemInspectedEnv::ReopenWritableFile(
    const std::string& fname, std::unique_ptr<WritableFile>* result,
    const EnvOptions& options) {
  auto s = EnvWrapper::ReopenWritableFile(fname, result, options);
  if (!s.ok()) {
    return s;
  }
  result->reset(
      new InspectedWritableFile(std::move(*result), inspector_.get()));
  return s;
}

Status FileSystemInspectedEnv::ReuseWritableFile(
    const std::string& fname, const std::string& old_fname,
    std::unique_ptr<WritableFile>* result, const EnvOptions& options) {
  auto s = EnvWrapper::ReuseWritableFile(fname, old_fname, result, options);
  if (!s.ok()) {
    return s;
  }
  result->reset(
      new InspectedWritableFile(std::move(*result), inspector_.get()));
  return s;
}

Status FileSystemInspectedEnv::NewRandomRWFile(
    const std::string& fname, std::unique_ptr<RandomRWFile>* result,
    const EnvOptions& options) {
  auto s = EnvWrapper::NewRandomRWFile(fname, result, options);
  if (!s.ok()) {
    return s;
  }
  result->reset(
      new InspectedRandomRWFile(std::move(*result), inspector_.get()));
  return s;
}

Env* NewFileSystemInspectedEnv(Env* base_env,
                               std::shared_ptr<FileSystemInspector> inspector) {
  return new FileSystemInspectedEnv(base_env, inspector);
}

}  // namespace rocksdb
#endif  // !ROCKSDB_LITE
