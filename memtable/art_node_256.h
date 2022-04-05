//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "memtable/art_inner_node.h"
#include <array>
#include <stdexcept>

namespace rocksdb {

class Node256 : public InnerNode {
 public:
  Node256() {
    n_children_.store(0, std::memory_order_relaxed);
    for (int i = 0; i < 256; i++) {
      children_[i].store(nullptr, std::memory_order_relaxed);
    }
  }
  virtual ~Node256() {}

  std::atomic<Node *> *find_child(uint8_t partial_key) override;
  void set_child(uint8_t partial_key, Node *child) override;
  const char *node_type() const override { return "Node256"; }
  InnerNode *grow(Allocator* allocator) override;
  bool is_full() const override;

  uint8_t next_partial_key(uint8_t partial_key) const override;

  uint8_t prev_partial_key(uint8_t partial_key) const override;

 private:
  std::atomic<uint32_t> n_children_;
  std::atomic<Node *>  children_[256];
};

std::atomic<Node *> *Node256::find_child(uint8_t key) {
  return &children_[key];
}

void Node256::set_child(uint8_t partial_key, Node *child) {
  uint8_t key = partial_key;
  children_[key].store(child, std::memory_order_release);
  ++n_children_;
}

InnerNode *Node256::grow(Allocator *_allocator) {
  throw std::runtime_error("Node256 cannot grow");
}

bool Node256::is_full() const { return false; }

uint8_t Node256::next_partial_key(uint8_t key) const {
  while (key < 255) {
    if (children_[key].load(std::memory_order_acquire) != nullptr) {
      break;
    }
    ++key;
  }
  return key;
}

uint8_t Node256::prev_partial_key(uint8_t key) const {
  while (key > 0) {
    if (children_[key].load(std::memory_order_acquire) != nullptr) {
      break;
    }
    --key;
  }
  return key;
}

} // namespace rocksdb
