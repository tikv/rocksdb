/**
 * @file Node256 header
 * @author Rafael Kallis <rk@rafaelkallis.com>
 */

#pragma once

#include "memtable/art_inner_node.h"
#include <array>
#include <stdexcept>

namespace rocksdb {


 class Node256 : public InnerNode {
public:
   Node256() { n_children_.store(0); }
   virtual ~Node256() {}

  std::atomic<Node*>*find_child(char partial_key) override;
  void set_child(char partial_key, Node *child) override;
  const char *node_type() const override { return "Node256"; }
  InnerNode *grow(Allocator* allocator) override;
  bool is_full() const override;

  char next_partial_key(char partial_key) const override;

  char prev_partial_key(char partial_key) const override;

  int n_children() const override;

private:
  std::atomic<uint16_t> n_children_;
  std::atomic<Node *>  children_[256];
};

 std::atomic<Node*>*Node256::find_child(char partial_key) {
  return &children_[128 + partial_key];
}

void Node256::set_child(char partial_key, Node *child) {
  children_[128 + partial_key].store(child, std::memory_order_release);
  ++n_children_;
}

 InnerNode *Node256::grow(Allocator* _allocator) {
  throw std::runtime_error("Node256 cannot grow");
}

bool Node256::is_full() const { return false; }

char Node256::next_partial_key(char partial_key) const {
  uint8_t key = 128 + partial_key;
  while (key < 255) {
    if (children_[key] != nullptr) {
      return partial_key;
    }
    ++partial_key;
    ++key;
  }
  return partial_key;
}

 char Node256::prev_partial_key(char partial_key) const {
   uint8_t key = 128 + partial_key;
   while (key > 0) {
     if (children_[key] != nullptr) {
       return partial_key;
     }
     --partial_key;
     --key;
   }
   return partial_key;
}

 int Node256::n_children() const { return n_children_; }

} // namespace rocksdb
