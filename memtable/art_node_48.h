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
  friend class Node16;
  friend class Node256;

public:
  Node48();

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
  static const char EMPTY;

  uint8_t n_children_ = 0;
  char indexes_[256];
  Node *children_[48];
};

 Node48::Node48() {
  std::fill(this->indexes_, this->indexes_ + 256, Node48::EMPTY);
  std::fill(this->children_, this->children_ + 48, nullptr);
}

 Node **Node48::find_child(char partial_key) {
  // TODO(rafaelkallis): direct lookup instead of temp save?
  uint8_t index = indexes_[128 + partial_key];
  return Node48::EMPTY != index ? &children_[index] : nullptr;
}


void Node48::set_child(char partial_key, Node *child) {

  // TODO(rafaelkallis): pick random starting entry in order to increase
  // performance? i.e. for (int i = random([0,48)); i != (i-1) % 48; i = (i+1) %
  // 48){}

  /* find empty child entry */
  for (int i = 0; i < 48; ++i) {
    if (children_[i] == nullptr) {
      indexes_[128 + partial_key] = (uint8_t) i;
      children_[i] = child;
      break;
    }
  }
  ++n_children_;
}

 Node *Node48::del_child(char partial_key) {
  Node *child_to_delete = nullptr;
  unsigned char index = indexes_[128 + partial_key];
  if (index != Node48::EMPTY) {
    child_to_delete = children_[index];
    indexes_[128 + partial_key] = Node48::EMPTY;
    children_[index] = nullptr;
    --n_children_;
  }
  return child_to_delete;
}

 InnerNode *Node48::grow() {
  auto new_node = new Node256();
  new_node->prefix_ = this->prefix_;
  new_node->prefix_len_ = this->prefix_len_;
  uint8_t index;
  for (int partial_key = -128; partial_key < 127; ++partial_key) {
    index = indexes_[128 + partial_key];
    if (index != Node48::EMPTY) {
      new_node->set_child(partial_key, children_[index]);
    }
  }
  delete this;
  return new_node;
}


 bool Node48::is_full() const {
  return n_children_ == 48;
}

 bool Node48::is_underfull() const {
  return n_children_ == 16;
}

 const char Node48::EMPTY = 48;

 char Node48::next_partial_key(char partial_key) const {
  while (true) {
    if (indexes_[128 + partial_key] != Node48::EMPTY) {
      return partial_key;
    }
    if (partial_key == 127) {
      throw std::out_of_range("provided partial key does not have a successor");
    }
    ++partial_key;
  }
}

 char Node48::prev_partial_key(char partial_key) const {
  while (true) {
    if (indexes_[128 + partial_key] != Node48::EMPTY) {
      return partial_key;
    }
    if (partial_key == -128) {
      throw std::out_of_range(
          "provided partial key does not have a predecessor");
    }
    --partial_key;
  }
}

 int Node48::n_children() const { return n_children_; }

} // namespace rocksdb

