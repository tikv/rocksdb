/**
 * @file Node16 header
 * @author Rafael Kallis <rk@rafaelkallis.com>
 */

#pragma once

#include "memtable/art_inner_node.h"
#include "memtable/art_node_48.h"
#include <array>
#include <cstdlib>
#include <stdexcept>
#include <utility>

#if defined(__i386__) || defined(__amd64__)
#include <emmintrin.h>
#endif

namespace rocksdb {


 class Node16 : public InnerNode {
friend class Node48;
public:
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

   uint8_t n_children_ = 0;
private:
  char keys_[16];
  Node *children_[16];
};

 Node **Node16::find_child(char partial_key) {
#if defined(__i386__) || defined(__amd64__)
  int bitfield =
      _mm_movemask_epi8(_mm_cmpeq_epi8(_mm_set1_epi8(partial_key),
                                       _mm_loadu_si128((__m128i *)keys_))) &
      ((1 << n_children_) - 1);
  return (bool)bitfield ? &children_[__builtin_ctz(bitfield)] : nullptr;
#else
  int lo, mid, hi;
  lo = 0;
  hi = n_children_;
  while (lo < hi) {
    mid = (lo + hi) / 2;
    if (partial_key < keys_[mid]) {
      hi = mid;
    } else if (partial_key > keys_[mid]) {
      lo = mid + 1;
    } else {
      return &children_[mid];
    }
  }
  return nullptr;
#endif
}


void Node16::set_child(char partial_key, Node *child) {
  /* determine index for child */
  int child_i;
  for (int i = this->n_children_ - 1;; --i) {
    if (i >= 0 && partial_key < this->keys_[i]) {
      /* move existing sibling to the right */
      this->keys_[i + 1] = this->keys_[i];
      this->children_[i + 1] = this->children_[i];
    } else {
      child_i = i + 1;
      break;
    }
  }

  this->keys_[child_i] = partial_key;
  this->children_[child_i] = child;
  ++n_children_;
}

 Node *Node16::del_child(char partial_key) {
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

 InnerNode *Node16::grow() {
  auto new_node = new Node48();
  new_node->prefix_ = this->prefix_;
  new_node->prefix_len_ = this->prefix_len_;
  std::copy(this->children_, this->children_ + this->n_children_, new_node->children_);
  for (int i = 0; i < n_children_; ++i) {
    new_node->indexes_[(uint8_t) this->keys_[i]] = i;
  }
  delete this;
  return new_node;
}


 bool Node16::is_full() const {
  return n_children_ == 16;
}

 bool Node16::is_underfull() const {
  return n_children_ == 4;
}

 char Node16::next_partial_key(char partial_key) const {
  for (int i = 0; i < n_children_; ++i) {
    if (keys_[i] >= partial_key) {
      return keys_[i];
    }
  }
  throw std::out_of_range("provided partial key does not have a successor");
}

 char Node16::prev_partial_key(char partial_key) const {
  for (int i = n_children_ - 1; i >= 0; --i) {
    if (keys_[i] <= partial_key) {
      return keys_[i];
    }
  }
  throw std::out_of_range("provided partial key does not have a predecessor");
}

 int Node16::n_children() const { return n_children_; }

} // namespace rocksdb

