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

class Node16;

class Node4 : public InnerNode {
  friend class Node16;

public:
  Node **find_child(char partial_key) override;
  void set_child(char partial_key, Node *child) override;
  Node *del_child(char partial_key) override;
  InnerNode *grow() override;
  bool is_full() const override;
  bool is_underfull() const override;

  char next_partial_key(char partial_key) const override;

  char prev_partial_key(char partial_key) const override;

  int n_children() const override;

private:
  uint8_t n_children_ = 0;
  char keys_[4];
  Node *children_[4];
};

 Node **Node4::find_child(char partial_key) {
  for (int i = 0; i < n_children_; ++i) {
    if (keys_[i] == partial_key) {
      return &children_[i];
    }
  }
  return nullptr;
}

 void Node4::set_child(char partial_key, Node *child) {
  /* determine index for child */
  int c_i;
  for (c_i = 0; c_i < n_children_ && partial_key >= keys_[c_i]; ++c_i) {
  }
  std::memmove(keys_ + c_i + 1, keys_ + c_i, n_children_ - c_i);
  std::memmove(children_ + c_i + 1, children_ + c_i,
               (n_children_ - c_i) * sizeof(void *));

  keys_[c_i] = partial_key;
  children_[c_i] = child;
  ++n_children_;
}

 Node *Node4::del_child(char partial_key) {
  Node *child_to_delete = nullptr;
  for (int i = 0; i < n_children_; ++i) {
    if (child_to_delete == nullptr && partial_key == keys_[i]) {
      child_to_delete = children_[i];
    }
    if (child_to_delete != nullptr) {
      /* move existing sibling to the left */
      keys_[i] = i < n_children_ - 1 ? keys_[i + 1] : 0;
      children_[i] = i < n_children_ - 1 ? children_[i + 1] : nullptr;
    }
  }
  if (child_to_delete != nullptr) {
    --n_children_;
  }
  return child_to_delete;
}

 InnerNode *Node4::grow() {
  auto new_node = new Node16();
  new_node->prefix_ = this->prefix_;
  new_node->prefix_len_ = this->prefix_len_;
  new_node->n_children_ = this->n_children_;
  std::copy(this->keys_, this->keys_ + this->n_children_, new_node->keys_);
  std::copy(this->children_, this->children_ + this->n_children_, new_node->children_);
  delete this;
  return new_node;
}


 bool Node4::is_full() const { return n_children_ == 4; }

 bool Node4::is_underfull() const {
  return false;
}

 char Node4::next_partial_key(char partial_key) const {
  for (int i = 0; i < n_children_; ++i) {
    if (keys_[i] >= partial_key) {
      return keys_[i];
    }
  }
  /* return 0; */
  throw std::out_of_range("provided partial key does not have a successor");
}

 char Node4::prev_partial_key(char partial_key) const {
  for (int i = n_children_ - 1; i >= 0; --i) {
    if (keys_[i] <= partial_key) {
      return keys_[i];
    }
  }
  /* return 255; */
  throw std::out_of_range("provided partial key does not have a predecessor");
}

 int Node4::n_children() const {
  return this->n_children_;
}

} // namespace rocksdb
