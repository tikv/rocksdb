// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#ifndef ROCKSDB_LITE
#include "rocksdb/env_inspected.h"

namespace rocksdb {

class InspectedSequentialFile : public SequentialFileWrapper {
 public:
  InspectedSequentialFile(SequentialFile* target,
                          FileSystemInspector* inspector)
      : SequentialFileWrapper(target), inspector_(inspector) {}

  Status Read(size_t n, Slice* result, char* scratch) override {
    assert(inspector_);
    Status s;
    size_t offset = 0;
    size_t allowed = 0;
    while (offset + 1 < n) {
      allowed = inspector_->Read(Env::IO_UNCATEGORIZED, n - offset);
      if (allowed > 0) {
        s = SequentialFileWrapper::Read(allowed, result, scratch + offset);
        if (!s.ok()) {
          break;
        }
        if (result->data() != scratch + offset) {
          memcpy(scratch + offset, result->data(), allowed);
        }
      } else {
        s = Status::IOError("Failed file system inspection");
        break;
      }
      offset += allowed;
    }
    *result = Slice(scratch, n);
    return s;
  }

  Status PositionedRead(uint64_t offset, size_t n, Slice* result,
                        char* scratch) override {
    assert(inspector_);
    Status s;
    size_t roffset = 0;
    size_t allowed = 0;
    while (roffset + 1 < n) {
      allowed = inspector_->Read(Env::IO_UNCATEGORIZED, n - roffset);
      if (allowed > 0) {
        s = SequentialFileWrapper::PositionedRead(offset + roffset, allowed,
                                                  result, scratch + roffset);
        if (!s.ok()) {
          break;
        }
        if (result->data() != scratch + roffset) {
          memcpy(scratch + roffset, result->data(), allowed);
        }
      } else {
        s = Status::IOError("Failed file system inspection");
        break;
      }
      roffset += allowed;
    }
    *result = Slice(scratch, n);
    return s;
  }

 private:
  FileSystemInspector* inspector_;
};

class InspectedRandomAccessFile : public RandomAccessFileWrapper {
 public:
  InspectedRandomAccessFile(RandomAccessFile* target,
                            FileSystemInspector* inspector)
      : RandomAccessFileWrapper(target), inspector_(inspector) {}

  Status Read(uint64_t offset, size_t n, Slice* result,
              char* scratch) const override {
    assert(inspector_);
    Status s;
    size_t roffset = 0;
    size_t allowed = 0;
    while (roffset + 1 < n) {
      allowed = inspector_->Read(Env::IO_UNCATEGORIZED, n - roffset);
      if (allowed > 0) {
        s = RandomAccessFileWrapper::Read(offset + roffset, allowed, result,
                                          scratch + roffset);
        if (!s.ok()) {
          break;
        }
        if (result->data() != scratch + roffset) {
          memcpy(scratch + roffset, result->data(), allowed);
        }
      } else {
        s = Status::IOError("Failed file system inspection");
        break;
      }
      roffset += allowed;
    }
    *result = Slice(scratch, n);
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
  FileSystemInspector* inspector_;
};

class InspectedWritableFile : public WritableFileWrapper {
 public:
  InspectedWritableFile(WritableFile* target, FileSystemInspector* inspector)
      : WritableFileWrapper(target), inspector_(inspector) {}

  Status Append(const Slice& data) override {
    assert(inspector_);
    Status s;
    size_t size = data.size();
    size_t offset = 0;
    size_t allowed = 0;
    while (offset + 1 < size) {
      allowed = inspector_->Write(GetIOType(), size - offset);
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
    while (roffset + 1 < size) {
      allowed = inspector_->Write(GetIOType(), size - roffset);
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
  FileSystemInspector* inspector_;
};

class InspectedRandomRWFile : public RandomRWFileWrapper {
 public:
  InspectedRandomRWFile(RandomRWFile* target, FileSystemInspector* inspector)
      : RandomRWFileWrapper(target), inspector_(inspector) {}

  Status Write(uint64_t offset, const Slice& data) override {
    assert(inspector_);
    Status s;
    size_t size = data.size();
    size_t roffset = 0;
    size_t allowed = 0;
    while (roffset + 1 < size) {
      allowed = inspector_->Write(Env::IO_UNCATEGORIZED, size - roffset);
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
    while (roffset + 1 < n) {
      allowed = inspector_->Read(Env::IO_UNCATEGORIZED, n - roffset);
      if (allowed > 0) {
        s = RandomRWFileWrapper::Read(offset + roffset, allowed, result,
                                      scratch + roffset);
        if (!s.ok()) {
          return s;
        }
        if (result->data() != scratch + roffset) {
          memcpy(scratch + roffset, result->data(), allowed);
        }
      } else {
        s = Status::IOError("Failed file system inspection");
        break;
      }
    }
    *result = Slice(scratch, n);
    return s;
  }

 private:
  FileSystemInspector* inspector_;
};

FileSystemInspectedEnv::FileSystemInspectedEnv(
    Env* base_env, std::shared_ptr<FileSystemInspector>& inspector)
    : EnvWrapper(base_env), inspector_(inspector) {}

FileSystemInspectedEnv::~FileSystemInspectedEnv() = default;

Status FileSystemInspectedEnv::NewSequentialFile(
    const std::string& fname, std::unique_ptr<SequentialFile>* result,
    const EnvOptions& options) {
  auto s = EnvWrapper::NewSequentialFile(fname, result, options);
  if (!s.ok()) {
    return s;
  }
  result->reset(new InspectedSequentialFile(result->get(), inspector_.get()));
  return s;
}

Status FileSystemInspectedEnv::NewRandomAccessFile(
    const std::string& fname, std::unique_ptr<RandomAccessFile>* result,
    const EnvOptions& options) {
  auto s = EnvWrapper::NewRandomAccessFile(fname, result, options);
  if (!s.ok()) {
    return s;
  }
  result->reset(new InspectedRandomAccessFile(result->get(), inspector_.get()));
  return s;
}

Status FileSystemInspectedEnv::NewWritableFile(
    const std::string& fname, std::unique_ptr<WritableFile>* result,
    const EnvOptions& options) {
  auto s = EnvWrapper::NewWritableFile(fname, result, options);
  if (!s.ok()) {
    return s;
  }
  result->reset(new InspectedWritableFile(result->get(), inspector_.get()));
  return s;
}

Status FileSystemInspectedEnv::ReopenWritableFile(
    const std::string& fname, std::unique_ptr<WritableFile>* result,
    const EnvOptions& options) {
  auto s = EnvWrapper::ReopenWritableFile(fname, result, options);
  if (!s.ok()) {
    return s;
  }
  result->reset(new InspectedWritableFile(result->get(), inspector_.get()));
  return s;
}

Status FileSystemInspectedEnv::ReuseWritableFile(
    const std::string& fname, const std::string& old_fname,
    std::unique_ptr<WritableFile>* result, const EnvOptions& options) {
  auto s = EnvWrapper::ReuseWritableFile(fname, old_fname, result, options);
  if (!s.ok()) {
    return s;
  }
  result->reset(new InspectedWritableFile(result->get(), inspector_.get()));
  return s;
}

Status FileSystemInspectedEnv::NewRandomRWFile(
    const std::string& fname, std::unique_ptr<RandomRWFile>* result,
    const EnvOptions& options) {
  auto s = EnvWrapper::NewRandomRWFile(fname, result, options);
  if (!s.ok()) {
    return s;
  }
  result->reset(new InspectedRandomRWFile(result->get(), inspector_.get()));
  return s;
}

}  // namespace rocksdb
#endif  // !ROCKSDB_LITE
