/**
 * @file Node4 header
 * @author Rafael Kallis <rk@rafaelkallis.com>
 */

#pragma once

#include "memtable/art_inner_node.h"
#include "memtable/art_node_16.h"
#include <algorithm>
#include <array>
#include <iostream>
#include <stdexcept>
#include <utility>

namespace rocksdb {

class Node4 : public InnerNode {
public:
  ~Node4() {}
  std::atomic<Node*> *find_child(char partial_key) override;
  void set_child(char partial_key, Node *child) override;
  const char *node_type() const override { return "Node4"; }
  InnerNode *grow(Allocator* allocator) override;
  bool is_full() const override;

  char next_partial_key(char partial_key) const override;

  char prev_partial_key(char partial_key) const override;

  int n_children() const override;

private:
  std::atomic<uint8_t> n_children_;
  std::atomic<uint64_t> keys_;
  std::atomic<Node*> children_[4];
};

std::atomic<Node *> *Node4::find_child(char partial_key) {
  uint8_t key = partial_key + 128;
  uint64_t keys = keys_.load(std::memory_order_acquire);
  while (keys > 0) {
    uint8_t c = keys & 255;
    uint8_t idx = ((keys >> 8) & 255) - 1;
    if (c == key) {
      return &children_[idx];
    }
    keys >>= 16;
  }
  return nullptr;
}

void Node4::set_child(char partial_key, Node *child) {
  /* determine index for child */
  uint8_t n_children = n_children_.load(std::memory_order_relaxed);
  children_[n_children].store(child, std::memory_order_release);
  uint64_t keys = keys_.load(std::memory_order_relaxed);
  uint8_t c_i = partial_key + 128;
  uint64_t c_value = ((uint64_t)n_children + 1) << 8 | c_i;
  uint64_t new_keys = 0;
  bool found = false;
  uint8_t base_value = 0;
  while (keys > 0) {
    uint8_t c = keys & 255;
    if (c > c_i && !found) {
      new_keys |= c_value << base_value;
      base_value += 16;
      found = true;
    }
    new_keys |= (keys & 65535) << base_value;
    keys >>= 16;
    base_value += 16;
  }
  if (!found) {
    new_keys |= c_value << base_value;
  }
  keys_.store(new_keys, std::memory_order_release);
  n_children_.store(n_children + 1, std::memory_order_release);
}

InnerNode *Node4::grow(Allocator *allocator) {
  Node12 *new_node = new (allocator->AllocateAligned(sizeof(Node12))) Node12();
  uint64_t keys = keys_.load(std::memory_order_relaxed);
  while (keys > 0) {
    uint8_t c = keys & 255;
    uint8_t idx = ((keys >> 8) & 255) - 1;
    new_node->set_child(c - 128,
                        children_[idx].load(std::memory_order_relaxed));
    keys >>= 16;
  }
  return new_node;
}

bool Node4::is_full() const { return n_children_ == 4; }

char Node4::next_partial_key(char partial_key) const {
  uint8_t key = partial_key + 128;
  uint64_t keys = keys_.load(std::memory_order_acquire);
  while (keys > 0) {
    uint8_t c = keys & 255;
    if (c >= key) {
      return c - 128;
    }
    keys >>= 16;
  }
  return 127;
}

 char Node4::prev_partial_key(char partial_key) const {
   char ret = -128;
   uint8_t key = partial_key + 128;
   uint64_t keys = keys_.load(std::memory_order_acquire);
   while (keys > 0) {
     uint8_t c = keys & 255;
     if (c <= key) {
       ret = c - 128;
     }
     keys >>= 16;
   }
   return ret;
}

 int Node4::n_children() const {
  return this->n_children_;
}

} // namespace rocksdb
