//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Simple hash function used for internal data structures

#pragma once
#include <stddef.h>
#include <stdint.h>

#include "rocksdb/slice.h"

namespace rocksdb {

extern uint32_t Hash(const char* data, size_t n, uint32_t seed);

inline uint32_t BloomHash(const Slice& key) {
  return Hash(key.data(), key.size(), 0xbc9f1d34);
}

inline uint32_t GetSliceHash(const Slice& s) {
  return Hash(s.data(), s.size(), 397);
}
// added by ElasticBF
inline uint32_t BloomHashId(const Slice& key, int id) {
  switch(id){
    case 0:
    return Hash(key.data(), key.size(), 0xbc9f1d34);
    case 1:
    return Hash(key.data(), key.size(), 0x34f1d34b);
    case 2:
    return Hash(key.data(), key.size(), 0x251d34bc);  
    case 3:
    return Hash(key.data(), key.size(), 0x01d34bc9);  
    case 4:
    return Hash(key.data(), key.size(), 0x1934bc9f);  
    case 5:
    return Hash(key.data(), key.size(), 0x934bc9f1);  
    case 6:
    return Hash(key.data(), key.size(), 0x4bc9f193);  
    case 7:
    return Hash(key.data(), key.size(), 0x51c2578a);  
    case 8:
    return Hash(key.data(), key.size(), 0xda23562f);  
    case 9:
    return Hash(key.data(), key.size(), 0x135254f2);  
    case 10:
    return Hash(key.data(), key.size(), 0xea1e4a48);  
    case 11:
    return Hash(key.data(), key.size(), 0x567925f1);  
    default:
      fprintf(stderr, "BloomHash id error\n");
      exit(1);
  }
}
// std::hash compatible interface.
struct SliceHasher {
  uint32_t operator()(const Slice& s) const { return GetSliceHash(s); }
};

}  // namespace rocksdb
