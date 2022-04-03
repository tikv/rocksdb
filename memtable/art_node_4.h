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
  InnerNode *grow(Allocator* allocator) override;
  bool is_full() const override;

  char next_partial_key(char partial_key) const override;

  char prev_partial_key(char partial_key) const override;

  int n_children() const override;

private:
  std::atomic<uint8_t> n_children_;
  std::atomic<uint32_t> keys_;
  std::atomic<Node*> children_[4];
};

 std::atomic<Node*> *Node4::find_child(char partial_key) {
   uint8_t key = partial_key + 128;
   uint8_t n_children = n_children_.load(std::memory_order_acquire);
   uint32_t keys = keys_.load(std::memory_order_acquire);
  for (uint8_t i = 0; i < n_children; ++i) {
    if (((keys >> (i * 8)) & 255) == key) {
      return &children_[i];
    }
  }
  return nullptr;
}

 void Node4::set_child(char partial_key, Node *child) {
  /* determine index for child */
  uint8_t n_children = n_children_.load(std::memory_order_relaxed);
  uint8_t c_i = partial_key + 128;
  uint32_t idx_value = (uint32_t)c_i << (n_children * 8);
  uint32_t key = keys_.load(std::memory_order_relaxed);
  keys_.store(key | idx_value, std::memory_order_release);
  children_[n_children].store(child, std::memory_order_release);
  n_children_.store(n_children + 1, std::memory_order_release);
}

 InnerNode *Node4::grow(Allocator* allocator) {
   Node16* new_node = new (allocator->AllocateAligned(sizeof(Node16)))Node16();
  uint8_t n_children = n_children_.load(std::memory_order_acquire);
  new_node->n_children_.store(n_children, std::memory_order_relaxed);
  uint32_t keys = keys_.load(std::memory_order_acquire);

  for (uint8_t i = 0; i < n_children; ++i) {
    uint8_t c = (keys >> (i * 8)) & 255;
    new_node->set_child(c - 128, children_[i].load(std::memory_order_relaxed));
  }
  return new_node;
}

 bool Node4::is_full() const { return n_children_ == 4; }

 char Node4::next_partial_key(char partial_key) const {
   uint8_t key = partial_key + 128;
   uint32_t keys = keys_.load(std::memory_order_acquire);
   uint8_t n_children = n_children_.load(std::memory_order_acquire);
   for (uint8_t i = 0; i < n_children; ++i) {
      uint8_t c = keys & 255;
      if (c >= key) {
        return c - 128;
      }
       keys >>= 8;
   }
   return 127;
}

 char Node4::prev_partial_key(char partial_key) const {
   uint8_t key = partial_key + 128;
   uint32_t keys = keys_.load(std::memory_order_acquire);
   uint8_t n_children = n_children_.load(std::memory_order_acquire);
   for (uint8_t i = n_children - 1; i >= 0; --i) {
     uint8_t c = (keys >> (i * 8)) & 255;
     if (c <= key) {
       return c - 128;
     }
   }
  /* return 255; */
  throw std::out_of_range("provided partial key does not have a predecessor");
}

 int Node4::n_children() const {
  return this->n_children_;
}

} // namespace rocksdb
