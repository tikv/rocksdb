/**
 * @file trie Nodes header.
 * @author Rafael Kallis <rk@rafaelkallis.com>
 */

#pragma once

#include <algorithm>
#include <array>
#include <cassert>
#include <cstring>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include "memtable/art_inner_node.h"

namespace rocksdb {

struct Node {
  Node() {}

  /**
   * Determines if this Node is a leaf Node, i.e., contains a value.
   * Needed for downcasting a Node instance to a leaf_Node or inner_Node instance.
   */
  bool is_leaf() const {
      return value != nullptr;
  }

  /**
   * Determines the number of matching bytes between the Node's prefix and the key.
   *
   * Given a Node with prefix: "abbbd", a key "abbbccc",
   * check_prefix returns 4, since byte 4 of the prefix ('d') does not
   * match byte 4 of the key ('c').
   *
   * key:     "abbbccc"
   * prefix:  "abbbd"
   *           ^^^^*
   * index:    01234
   */
  int check_prefix(const char *key, int depth, int key_len) const;

  InnerNode* inner;
  const char* value;
  uint16_t prefix_len;
  const char* prefix;
};

int Node::check_prefix(const char *key, int depth, int key_len) const {
  int l = std::min((int)prefix_len, key_len - depth);
  for (int i = 0; i < l; i ++) {
    if (key[i + depth] != prefix[i]) {
      return i;
    }
  }
  return l;
}

} // namespace rocksdb

