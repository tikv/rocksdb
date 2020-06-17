//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/slice.h"
#include "rocksdb/sst_partitioner.h"

namespace rocksdb {
class SstPartitionerFixedPrefix : public SstPartitioner {
 public:
  SstPartitionerFixedPrefix(size_t len) : len_(len) {}

  virtual ~SstPartitionerFixedPrefix(){};

  const char* Name() const override { return "SstPartitionerFixedPrefix"; }

  bool ShouldPartition(const State& state) override {
    if (last_key_.empty()) {
      return false;
    }
    Slice key_fixed(state.next_key.data_, std::min(state.next_key.size_, len_));
    return key_fixed.compare(last_key_) != 0;
  }

  void Reset(const Slice& key) override {
    last_key_.assign(key.data_, std::min(key.size_, len_));
  }

 private:
  size_t len_;
  std::string last_key_;
};

class SstPartitionerFixedPrefixFactory : public SstPartitionerFactory {
 public:
  SstPartitionerFixedPrefixFactory(size_t len) : len_(len) {}

  virtual ~SstPartitionerFixedPrefixFactory() {}

  const char* Name() const override {
    return "SstPartitionerFixedPrefixFactory";
  }

  std::unique_ptr<SstPartitioner> CreatePartitioner(
      const SstPartitioner::Context& /* context */) const override {
    return std::unique_ptr<SstPartitioner>(new SstPartitionerFixedPrefix(len_));
  }

 private:
  size_t len_;
};

std::shared_ptr<SstPartitionerFactory> NewSstPartitionerFixedPrefixFactory(
    size_t prefix_len) {
  return std::make_shared<SstPartitionerFixedPrefixFactory>(prefix_len);
}

} // namespace rocksdb
