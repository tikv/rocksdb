//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include "memtable/art_inner_node.h"
#include "memtable/art_node_48.h"
#include <array>
#include <cstdlib>
#include <stdexcept>
#include <utility>

namespace rocksdb {

class Node12 : public InnerNode {
  const static uint8_t MAX_CHILDREN_NUM = 12;
  struct ChildrenNode {
    ChildrenNode() {}
    char c;
    uint8_t idx;
    std::atomic<uint8_t> next;
    std::atomic<Node*> child;
  };

 public:
  Node12() : n_children_(0), first_(nullptr) {}
  ~Node12() {}
  std::atomic<Node*>* find_child(char partial_key) override;
  void set_child(char partial_key, Node *child) override;
  const char* node_type() const override { return "Node16"; }
  InnerNode *grow(Allocator* allocator) override;
  bool is_full() const override;

  char next_partial_key(char partial_key) const override;

  char prev_partial_key(char partial_key) const override;

private:
 std::atomic<uint8_t> n_children_;
 std::atomic<ChildrenNode*> first_;
 ChildrenNode children_[MAX_CHILDREN_NUM];
};

std::atomic<Node*>* Node12::find_child(char partial_key) {
  ChildrenNode* next = first_.load(std::memory_order_acquire);
  while (next != nullptr) {
    if (next->c == partial_key) {
      return &next->child;
    }
    uint8_t idx = next->next.load(std::memory_order_acquire);
    if (idx > 0) {
      next = &children_[idx - 1];
    } else {
      break;
    }
  }
  return nullptr;
}

void Node12::set_child(char partial_key, Node* child) {
  /* determine index for child */
  uint8_t child_i = n_children_.fetch_add(1, std::memory_order_relaxed);
  ChildrenNode* new_child = &children_[child_i];
  new_child->idx = child_i + 1;
  new_child->c = partial_key;
  new_child->next.store(0, std::memory_order_relaxed);
  new_child->child.store(child, std::memory_order_release);
  ChildrenNode* prev = nullptr;
  ChildrenNode* cur = first_.load(std::memory_order_relaxed);
  while (cur != nullptr) {
    if (cur->c > partial_key) {
      new_child->next.store(cur->idx, std::memory_order_relaxed);
      if (prev == nullptr) {
        first_.store(new_child, std::memory_order_release);
      } else {
        prev->next.store(new_child->idx, std::memory_order_release);
      }
      return;
    }
    prev = cur;
    uint8_t idx = cur->next.load(std::memory_order_relaxed);
    if (idx > 0) {
      cur = &children_[idx - 1];
    } else {
      break;
    }
  }
  if (prev == nullptr) {
    first_.store(new_child, std::memory_order_release);
  } else {
    prev->next.store(new_child->idx, std::memory_order_release);
  }
}

InnerNode* Node12::grow(Allocator* allocator) {
  auto new_node = new (allocator->AllocateAligned(sizeof(Node48)))Node48();
  ChildrenNode* cur = first_.load(std::memory_order_acquire);
  while (cur != nullptr) {
    new_node->set_child(cur->c, cur->child.load(std::memory_order_relaxed));
    uint8_t idx = cur->next.load(std::memory_order_acquire);
    if (idx > 0) {
      cur = &children_[idx - 1];
    } else {
      break;
    }
  }
  return new_node;
}

bool Node12::is_full() const { return n_children_ == MAX_CHILDREN_NUM; }

char Node12::next_partial_key(char partial_key) const {
  const ChildrenNode* cur = first_.load(std::memory_order_acquire);
  while (cur != nullptr) {
    if (cur->c >= partial_key) {
      return cur->c;
    }
    uint8_t idx = cur->next.load(std::memory_order_acquire);
    if (idx == 0) {
      break;
    }
    cur = &children_[idx - 1];
  }
  return 127;
}

char Node12::prev_partial_key(char partial_key) const {
  uint8_t ret = 0;
  const ChildrenNode* cur = first_.load(std::memory_order_acquire);
  while (cur != nullptr) {
    if (cur->c <= partial_key) {
      ret = cur->c;
    }
    uint8_t idx = cur->next.load(std::memory_order_acquire);
    if (idx > 0) {
      cur = &children_[idx - 1];
    } else {
      break;
    }
  }
  return ret;
}

} // namespace rocksdb

