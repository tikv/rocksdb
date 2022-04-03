/**
 * @file Node48 header
 * @author Rafael Kallis <rk@rafaelkallis.com>
 */

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

  std::atomic<Node*> *find_child(char partial_key) override;
  void set_child(char partial_key, Node *child) override;
  InnerNode *grow(Allocator* allocator) override;
  bool is_full() const override;

  char next_partial_key(char partial_key) const override;
  char prev_partial_key(char partial_key) const override;
  uint8_t get_index(uint8_t key) const;
  void set_index(uint8_t key, uint8_t index);

  int n_children() const override;

private:
  static const uint8_t EMPTY;

  std::atomic<uint8_t> n_children_;
  std::atomic<uint64_t> indexes_[32];
  std::atomic<Node*> children_[48];
};

 Node48::Node48() {
   for (int i = 0; i < 32; i ++) {
     indexes_[i].store(0, std::memory_order_relaxed);
   }
   for (int i = 0; i < 48; i ++) {
     children_[i].store(nullptr, std::memory_order_relaxed);
   }
   n_children_.store(0, std::memory_order_relaxed);
}

 std::atomic<Node*> *Node48::find_child(char partial_key) {
  // TODO(rafaelkallis): direct lookup instead of temp save?
  uint8_t index = get_index(partial_key + 128);
  return Node48::EMPTY != index ? &children_[index] : nullptr;
}

uint8_t Node48::get_index(uint8_t key) const {
  uint64_t index = indexes_[key >> 3].load(std::memory_order_acquire);
  return (index >> ((key & 7) << 3) & 255) - 1;
}

void Node48::set_index(uint8_t key, uint8_t index) {
  uint64_t old_index = indexes_[key >> 3].load(std::memory_order_acquire);
  indexes_[key >> 3].store(old_index | (index + 1) << (key & 7), std::memory_order_release);
}

void Node48::set_child(char partial_key, Node *child) {
  uint8_t n_children = n_children_.load(std::memory_order_relaxed);
  set_index(partial_key + 128, n_children);
  children_[n_children].store(child, std::memory_order_release);
  n_children_.store(n_children + 1, std::memory_order_release);
}

 InnerNode *Node48::grow(Allocator* allocator) {
  auto new_node = new (allocator->AllocateAligned(sizeof(Node256)))Node256();
  uint8_t index;
  for (int partial_key = -128; partial_key < 127; ++partial_key) {
    index = get_index(partial_key + 128);
    if (index != Node48::EMPTY) {
      new_node->set_child(partial_key, children_[index]);
    }
  }
  return new_node;
}


 bool Node48::is_full() const {
  return n_children_ == 48;
}

 const uint8_t Node48::EMPTY = 255;

 char Node48::next_partial_key(char partial_key) const {
   while (partial_key < 127) {
     uint8_t index = get_index(partial_key + 128);
     if (index != Node48::EMPTY) {
       break;
     }
     ++partial_key;
   }
   return partial_key;
}

 char Node48::prev_partial_key(char partial_key) const {
   while (partial_key > -128) {
     uint8_t index = get_index(partial_key + 128);
     if (index != Node48::EMPTY) {
       return partial_key;
     }
     --partial_key;
   }
   return partial_key;
}

 int Node48::n_children() const { return n_children_; }

} // namespace rocksdb

