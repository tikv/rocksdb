// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Class for specifying user-defined functions which perform a
// transformation on a slice.  It is not required that every slice
// belong to the domain and/or range of a function.  Subclasses should
// define InDomain and InRange to determine which slices are in either
// of these sets respectively.

#pragma once

#include <memory>
#include <string>

namespace rocksdb {

class Slice;

/*
 * A SstPartitioner is a generic pluggable way of defining the partition
 * of SST files. Compaction job will split the SST files on partition boundary
 * to lower the write amplification during SST file promote to higher level.
 */
class SstPartitioner {
 public:
  // Context information of a compaction run
  struct Context {
    // Does this compaction run include all data files
    bool is_full_compaction;
    // Is this compaction requested by the client (true),
    // or is it occurring as an automatic compaction process
    bool is_manual_compaction;
    // Output level for this compaction
    int output_level;
    // Smallest key in the compaction
    std::string smallest_key;
    // Largest key in the compaction
    std::string largest_key;
  };

  // State of compaction.
  struct State {
    Slice next_key;
    uint64_t current_output_file_size;
  };

  virtual ~SstPartitioner(){};

  // Return the name of this partitioner.
  virtual const char* Name() const = 0;

  // Called with key that is right after the key that was stored into the SST
  // Returns true of partition boundary was detected and compaction should
  // create new file.
  virtual bool ShouldPartition(const State& state) = 0;

  // Called for key that was stored into the SST
  virtual void Reset(const Slice& key) = 0;
};

class SstPartitionerFactory {
 public:
  virtual ~SstPartitionerFactory() {}

  virtual std::unique_ptr<SstPartitioner> CreatePartitioner(
      const SstPartitioner::Context& context) const = 0;

  // Returns a name that identifies this partitioner factory.
  virtual const char* Name() const = 0;
};

extern std::shared_ptr<SstPartitionerFactory>
NewSstPartitionerFixedPrefixFactory(size_t prefix_len);

}  // namespace rocksdb
