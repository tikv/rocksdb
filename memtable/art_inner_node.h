/**
 * @file InnerNode header
 * @author Rafael Kallis <rk@rafaelkallis.com>
 */

#pragma once

#include "memtable/art_leaf_node.h"
#include "memtable/art_node.h"
#include <algorithm>
#include <array>
#include <cassert>
#include <cstring>
#include <iostream>
#include <iterator>
#include <stdexcept>

namespace rocksdb {

class ChildIter;

class InnerNode : public Node {
public:
  virtual ~InnerNode() = default;

  InnerNode() = default;
  InnerNode(const InnerNode &other) = default;
  InnerNode(InnerNode &&other) noexcept = default;
  InnerNode &operator=(const InnerNode &other) = default;
  InnerNode &operator=(InnerNode &&other) noexcept = default;

  bool is_leaf() const override;

  /**
   * Finds and returns the child Node identified by the given partial key.
   *
   * @param partial_key - The partial key associated with the child.
   * @return Child Node identified by the given partial key or
   * a null pointer of no child Node is associated with the partial key.
   */
  virtual Node **find_child(char partial_key) = 0;

  /**
   * Adds the given Node to the Node's children.
   * No bounds checking is done.
   * If a child already exists under the given partial key, the child
   * is overwritten without deleting it.
   *
   * @pre Node should not be full.
   * @param partial_key - The partial key associated with the child.
   * @param child - The child Node.
   */
  virtual void set_child(char partial_key, Node *child) = 0;

  /**
   * Deletes the child associated with the given partial key.
   *
   * @param partial_key - The partial key associated with the child.
   */
  virtual Node *del_child(char partial_key) = 0;

  /**
   * Creates and returns a new Node with bigger children capacity.
   * The current Node gets deleted.
   *
   * @return Node with bigger capacity
   */
  virtual InnerNode *grow() = 0;

  /**
   * Determines if the Node is full, i.e. can carry no more child Nodes.
   */
  virtual bool is_full() const = 0;

  /**
   * Determines if the Node is underfull, i.e. carries less child Nodes than
   * intended.
   */
  virtual bool is_underfull() const = 0;

  virtual int n_children() const = 0;

  virtual char next_partial_key(char partial_key) const = 0;

  virtual char prev_partial_key(char partial_key) const = 0;

  /**
   * Iterator on the first child Node.
   *
   * @return Iterator on the first child Node.
   */
  ChildIter begin();
  std::reverse_iterator<ChildIter> rbegin();

  /**
   * Iterator on after the last child Node.
   *
   * @return Iterator on after the last child Node.
   */
  ChildIter end();
  std::reverse_iterator<ChildIter> rend();
};

bool InnerNode::is_leaf() const { return false; }

class ChildIter {
 public:
  ChildIter() = default;
  ChildIter(const ChildIter &other) = default;
  ChildIter(ChildIter &&other) noexcept = default;
  ChildIter &operator=(const ChildIter &other) = default;
  ChildIter &operator=(ChildIter &&other) noexcept = default;

  explicit ChildIter(InnerNode *n);
  ChildIter(InnerNode *n, int relative_index);

  using iterator_category = std::bidirectional_iterator_tag;
  using value_type = const char;
  using difference_type = int;
  using pointer = value_type *;
  using reference = value_type &;

  reference operator*() const;
  pointer operator->() const;
  ChildIter &operator++();
  ChildIter operator++(int);
  ChildIter &operator--();
  ChildIter operator--(int);
  bool operator==(const ChildIter &rhs) const;
  bool operator!=(const ChildIter &rhs) const;
  bool operator<(const ChildIter &rhs) const;
  bool operator>(const ChildIter &rhs) const;
  bool operator<=(const ChildIter &rhs) const;
  bool operator>=(const ChildIter &rhs) const;

  char get_partial_key() const;
  Node *get_child_node() const;

 private:
  InnerNode *node_ = nullptr;
  char cur_partial_key_ = -128;
  int relative_index_ = 0;
};

ChildIter::ChildIter(InnerNode *n) : ChildIter(n, 0) {}


ChildIter::ChildIter(InnerNode *n, int relative_index)
    : node_(n), cur_partial_key_(0), relative_index_(relative_index) {
  if (relative_index_ < 0) {
    /* relative_index is out of bounds, no seek */
    return;
  }

  if (relative_index_ >= node_->n_children()) {
    /* relative_index is out of bounds, no seek */
    return;
  }

  if (relative_index_ == node_->n_children() - 1) {
    cur_partial_key_ = node_->prev_partial_key(127);
    return;
  }

  cur_partial_key_ = node_->next_partial_key(-128);
  for (int i = 0; i < relative_index_; ++i) {
    cur_partial_key_ = node_->next_partial_key(cur_partial_key_ + 1);
  }
}


typename ChildIter::reference ChildIter::operator*() const {
  if (relative_index_ < 0 || relative_index_ >= node_->n_children()) {
    throw std::out_of_range("child iterator is out of range");
  }

  return cur_partial_key_;
}


typename ChildIter::pointer ChildIter::operator->() const {
  if (relative_index_ < 0 || relative_index_ >= node_->n_children()) {
    throw std::out_of_range("child iterator is out of range");
  }

  return &cur_partial_key_;
}

ChildIter &ChildIter::operator++() {
  ++relative_index_;
  if (relative_index_ < 0) {
    return *this;
  } else if (relative_index_ == 0) {
    cur_partial_key_ = node_->next_partial_key(-128);
  } else if (relative_index_ < node_->n_children()) {
    cur_partial_key_ = node_->next_partial_key(cur_partial_key_ + 1);
  }
  return *this;
}

ChildIter ChildIter::operator++(int) {
  auto old = *this;
  operator++();
  return old;
}

ChildIter &ChildIter::operator--() {
  --relative_index_;
  if (relative_index_ > node_->n_children() - 1) {
    return *this;
  } else if (relative_index_ == node_->n_children() - 1) {
    cur_partial_key_ = node_->prev_partial_key(127);
  } else if (relative_index_ >= 0) {
    cur_partial_key_ = node_->prev_partial_key(cur_partial_key_ - 1);
  }
  return *this;
}

ChildIter ChildIter::operator--(int) {
  auto old = *this;
  operator--();
  return old;
}

bool ChildIter::operator==(const ChildIter &rhs) const {
  return node_ == rhs.node_ && relative_index_ == rhs.relative_index_;
}

bool ChildIter::operator<(const ChildIter &rhs) const {
  return node_ == rhs.node_ && relative_index_ < rhs.relative_index_;
}

bool ChildIter::operator!=(const ChildIter &rhs) const {
  return !((*this) == rhs);
}

bool ChildIter::operator>=(const ChildIter &rhs) const {
  return !((*this) < rhs);
}

bool ChildIter::operator<=(const ChildIter &rhs) const {
  return (rhs >= (*this));
}

bool ChildIter::operator>(const ChildIter &rhs) const {
  return (rhs < (*this));
}


char ChildIter::get_partial_key() const {
  return cur_partial_key_;
}

Node *ChildIter::get_child_node() const {
  assert(0 <= relative_index_ && relative_index_ < node_->n_children());
  return *node_->find_child(cur_partial_key_);
}

ChildIter InnerNode::begin() {
  return ChildIter(this);
}

ChildIter InnerNode::end() {
  return ChildIter(this, n_children());
}

std::reverse_iterator<ChildIter> InnerNode::rbegin() {
  return std::reverse_iterator<ChildIter>(end());
}

std::reverse_iterator<ChildIter> InnerNode::rend() {
  return std::reverse_iterator<ChildIter>(begin());
}



} // namespace art
