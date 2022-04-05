//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.  Use of
// this source code is governed by a BSD-style license that can be found
// in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <algorithm>
#include <array>
#include <cassert>
#include <cstring>
#include <iostream>
#include <atomic>
#include <iterator>
#include <stdexcept>

namespace rocksdb {

class InnerNode;

struct Node {
  Node() {}

  bool is_leaf() const {
    return value.load(std::memory_order_acquire) != nullptr;
  }

  int check_prefix(const char* key, int depth, int key_len) const {
    int l = std::min(prefix_len, key_len - depth);
    for (int i = 0; i < l; i++) {
      if (key[i + depth] != prefix[i]) {
        return i;
      }
    }
    return l;
  }

  void set_value(const char* leaf) {
    value.store(leaf, std::memory_order_release);
  }

  const char* get_value() const {
    return value.load(std::memory_order_acquire);
  }

  InnerNode* inner;
  std::atomic<const char*> value;
  int prefix_len;
  const char* prefix;
};

} // namespace rocksdb

