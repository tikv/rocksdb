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

namespace rocksdb {

class Node {
public:
  virtual ~Node() = default;

  Node() = default;
  Node(const Node &other) = default;
  Node(Node &&other) noexcept = default;
  Node &operator=(const Node &other) = default;
  Node &operator=(Node &&other) noexcept = default;

  /**
   * Determines if this Node is a leaf Node, i.e., contains a value.
   * Needed for downcasting a Node instance to a leaf_Node or inner_Node instance.
   */
  virtual bool is_leaf() const = 0;

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
  int check_prefix(const char *key, int key_len) const;

  char *prefix_ = nullptr;
  uint16_t prefix_len_ = 0;
};

int Node::check_prefix(const char *key, int key_len) const {
  key_len = std::min(key_len, (int)prefix_len_);
  return std::mismatch(prefix_, prefix_ + key_len, key).second - key;
}

} // namespace rocksdb

