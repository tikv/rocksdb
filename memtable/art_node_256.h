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
  Node256();

  Node **find_child(char partial_key) override;
  void set_child(char partial_key, Node *child) override;
  Node *del_child(char partial_key) override;
  InnerNode *grow() override;
  InnerNode *shrink() override;
  bool is_full() const override;
  bool is_underfull() const override;

  char next_partial_key(char partial_key) const override;

  char prev_partial_key(char partial_key) const override;

  int n_children() const override;

private:
  uint16_t n_children_ = 0;
  std::array<Node *, 256> children_;
};

 Node256::Node256() { children_.fill(nullptr); }

 Node **Node256::find_child(char partial_key) {
  return children_[128 + partial_key] != nullptr ? &children_[128 + partial_key]
                                                 : nullptr;
}


void Node256::set_child(char partial_key, Node *child) {
  children_[128 + partial_key] = child;
  ++n_children_;
}

 Node *Node256::del_child(char partial_key) {
  Node *child_to_delete = children_[128 + partial_key];
  if (child_to_delete != nullptr) {
    children_[128 + partial_key] = nullptr;
    --n_children_;
  }
  return child_to_delete;
}

 InnerNode *Node256::grow() {
  throw std::runtime_error("Node256 cannot grow");
}


 bool Node256::is_full() const {
  return n_children_ == 256;
}

 bool Node256::is_underfull() const {
  return n_children_ == 48;
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
