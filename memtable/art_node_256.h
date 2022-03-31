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

 bool Node256::is_full() const {
  return n_children_ == 256;
}


 char Node256::next_partial_key(char partial_key) const {
  while (true) {
    if (children_[128 + partial_key] != nullptr) {
      return partial_key;
    }
    if (partial_key == 127) {
      throw std::out_of_range("provided partial key does not have a successor");
    }
    ++partial_key;
  }
}

 char Node256::prev_partial_key(char partial_key) const {
  while (true) {
    if (children_[128 + partial_key] != nullptr) {
      return partial_key;
    }
    if (partial_key == -128) {
      throw std::out_of_range(
          "provided partial key does not have a predecessor");
    }
    --partial_key;
  }
}

 int Node256::n_children() const { return n_children_; }

} // namespace rocksdb
