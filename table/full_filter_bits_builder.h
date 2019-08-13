// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "rocksdb/filter_policy.h"

namespace rocksdb {

class Slice;

class FullFilterBitsBuilder : public FilterBitsBuilder {
 public:
  explicit FullFilterBitsBuilder(const size_t bits_per_key,
                                 const size_t num_probes);

  ~FullFilterBitsBuilder();

  virtual void AddKey(const Slice& key) override;

  // Create a filter that for hashes [0, n-1], the filter is allocated here
  // When creating filter, it is ensured that
  // total_bits = num_lines * CACHE_LINE_SIZE * 8
  // dst len is >= 5, 1 for num_probes, 4 for num_lines
  // Then total_bits = (len - 5) * 8, and cache_line_size could be calculated
  // +----------------------------------------------------------------+
  // |              filter data with length total_bits/8              |
  // +----------------------------------------------------------------+
  // |                                                                |
  // | ...                                                            |
  // |                                                                |
  // +----------------------------------------------------------------+
  // | ...                | num_probes : 1 byte | num_lines : 4 bytes |
  // +----------------------------------------------------------------+
  virtual Slice Finish(std::unique_ptr<const char[]>* buf) override;
  // added by ElasticBF
  virtual Slice Finish(std::unique_ptr<const char[]>* buf, int id) {
    buf->reset();
    fprintf(stderr, "error call FullFilterBitsBuilder::Finish(buf,id), %d\n", id);
    exit(1);
    return Slice();
  }

  // Calculate num of entries fit into a space.
  virtual int CalculateNumEntry(const uint32_t space) override;
  // added by ElasticBF
  int id_;
  // Calculate space for new filter. This is reverse of CalculateNumEntry.
  uint32_t CalculateSpace(const int num_entry, uint32_t* total_bits,
                          uint32_t* num_lines);

 private:
  friend class FullFilterBlockTest_DuplicateEntries_Test;
  size_t bits_per_key_;
  size_t num_probes_;
  std::vector<uint32_t> hash_entries_;

  // Get totalbits that optimized for cpu cache line
  uint32_t GetTotalBitsForLocality(uint32_t total_bits);

  // Reserve space for new filter
  char* ReserveSpace(const int num_entry, uint32_t* total_bits,
                     uint32_t* num_lines);

  // Assuming single threaded access to this function.
  void AddHash(uint32_t h, char* data, uint32_t num_lines, uint32_t total_bits);

  // No Copy allowed
  FullFilterBitsBuilder(const FullFilterBitsBuilder&);
  void operator=(const FullFilterBitsBuilder&);
};
// added by ElasticBF
class MultiFullFilterBitsBuilder : public FullFilterBitsBuilder {
 public:
  explicit MultiFullFilterBitsBuilder(const std::vector<int> &bits_per_keys,
                                 const size_t num_probe);

  ~MultiFullFilterBitsBuilder();

  virtual void AddKey(const Slice& key) override;

  virtual Slice Finish(std::unique_ptr<const char[]>* buf) {
    buf->reset();
    fprintf(stderr, "error call MultiFullFilterBitsBuilder::Finish(buf)\n");
    exit(1);
    return Slice();
  }

  virtual Slice Finish(std::unique_ptr<const char[]>* buf, int id);

 private:
  friend class FullFilterBlockTest_DuplicateEntries_Test;
  // std::vector<int> bits_per_keys_;
  std::vector<FullFilterBitsBuilder*> fullFilterBitsBuilders;


  // Assuming single threaded access to this function.
  void AddHash(uint32_t h, char* data, uint32_t num_lines, uint32_t total_bits);

  // No Copy allowed
  MultiFullFilterBitsBuilder(const MultiFullFilterBitsBuilder&);
  void operator=(const MultiFullFilterBitsBuilder&);
};
}  // namespace rocksdb
