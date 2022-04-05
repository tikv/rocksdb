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
#include <iterator>
#include <stdexcept>

namespace rocksdb {

class InnerNode;

struct Node {
  Node() {}

  bool is_leaf() const {
      return value != nullptr;
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

  InnerNode* inner;
  const char* value;
  int prefix_len;
  const char* prefix;
};

} // namespace rocksdb

