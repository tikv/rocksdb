//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "memtable/art_node_256.h"
#include <algorithm>
#include <array>
#include <stdexcept>
#include <utility>

namespace rocksdb {

class Node48 : public InnerNode {
 public:
  Node48();
  virtual ~Node48() {}

  std::atomic<Node *> *find_child(uint8_t partial_key) override;
  void set_child(uint8_t partial_key, Node *child) override;
  const char *node_type() const override { return "Node48"; }
  InnerNode *grow(Allocator* allocator) override;
  bool is_full() const override;

  uint8_t next_partial_key(uint8_t partial_key) const override;
  uint8_t prev_partial_key(uint8_t partial_key) const override;
  uint8_t get_index(uint8_t key) const;
  void set_index(uint8_t key, uint8_t index);

private:
  static const uint8_t EMPTY;

  std::atomic<uint8_t> n_children_;
  std::atomic<uint64_t> indexes_[32];
  std::atomic<Node*> children_[48];
};

Node48::Node48() {
  for (int i = 0; i < 32; i++) {
    indexes_[i].store(0, std::memory_order_relaxed);
  }
  for (int i = 0; i < 48; i++) {
    children_[i].store(nullptr, std::memory_order_relaxed);
  }
  n_children_.store(0, std::memory_order_relaxed);
}

std::atomic<Node *> *Node48::find_child(uint8_t partial_key) {
  // TODO(rafaelkallis): direct lookup instead of temp save?
  uint8_t index = get_index(partial_key);
  return Node48::EMPTY != index ? &children_[index] : nullptr;
}

uint8_t Node48::get_index(uint8_t key) const {
  uint64_t index_value = indexes_[key >> 3].load(std::memory_order_acquire);
  uint8_t index = (index_value >> ((key & 7) << 3) & 255);
  return index - 1;
}

void Node48::set_index(uint8_t key, uint8_t index) {
  uint64_t old_index = indexes_[key >> 3].load(std::memory_order_acquire);
  indexes_[key >> 3].store(old_index | ((uint64_t)index + 1)
                                           << ((key & 7) << 3),
                           std::memory_order_release);
}

void Node48::set_child(uint8_t partial_key, Node *child) {
  uint8_t n_children = n_children_.load(std::memory_order_relaxed);
  set_index(partial_key, n_children);
  children_[n_children].store(child, std::memory_order_release);
  n_children_.store(n_children + 1, std::memory_order_release);
}

InnerNode *Node48::grow(Allocator *allocator) {
  auto new_node = new (allocator->AllocateAligned(sizeof(Node256)))Node256();
  uint8_t index;
  for (int partial_key = 0; partial_key <= 255; ++partial_key) {
    index = get_index(partial_key);
    if (index != Node48::EMPTY) {
      new_node->set_child(partial_key, children_[index]);
    }
  }
  return new_node;
}

bool Node48::is_full() const { return n_children_ == 48; }

const uint8_t Node48::EMPTY = 255;

uint8_t Node48::next_partial_key(uint8_t partial_key) const {
  uint8_t key = partial_key;
  while (key < 255) {
    uint8_t index = get_index(key);
    if (index != Node48::EMPTY) {
      break;
    }
    ++key;
  }
  return key;
}

uint8_t Node48::prev_partial_key(uint8_t key) const {
  while (key > 0) {
    uint8_t index = get_index(key);
    if (index != Node48::EMPTY) {
      break;
    }
    --key;
  }
  return key;
}

} // namespace rocksdb

