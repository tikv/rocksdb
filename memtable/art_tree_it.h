/**
 * @file tree iterator
 * @author Rafael Kallis <rk@rafaelkallis.com>
 */

#pragma once

#include "memtable/art_inner_node.h"
#include "memtable/art_leaf_node.h"
#include <cassert>
#include <cstring>
#include <iostream>
#include <iterator>
#include <vector>

namespace rocksdb {

class TreeIter {
public:
  struct Step {
    Node *node_;
    int depth_;
    ChildIter child_it_;
    ChildIter child_it_end_;

    Step(int depth, ChildIter c_it, ChildIter c_it_end);
    Step(Node *node, int depth, ChildIter c_it, ChildIter c_it_end);

    Step &operator++();
    Step operator++(int);
  };

  TreeIter() = default;
  explicit TreeIter(std::vector<Step> traversal_stack);

  static TreeIter min(Node *root);
  static TreeIter greater_equal(Node *root, const char *key);

  using iterator_category = std::forward_iterator_tag;
  using difference_type = int;
  /* using reference = const value_type &; */

  /* reference operator*(); */
  char* operator*();
  char** operator->();
  TreeIter &operator++();
  TreeIter operator++(int);
  bool operator==(const TreeIter &rhs) const;
  bool operator!=(const TreeIter &rhs) const;

  Node *get_node() const;
  int get_depth() const;

private:
  Step &get_Step();
  const Step &get_Step() const;
  void seek_leaf();

  std::vector<Step> traversal_stack_;
};

TreeIter::Step::Step(Node *node, int depth, ChildIter c_it, ChildIter c_it_end)
  : node_(node), depth_(depth), child_it_(c_it), child_it_end_(c_it_end) {}

TreeIter::Step::Step(int depth, ChildIter c_it, ChildIter c_it_end)
  : Step(c_it != c_it_end ? c_it.get_child_node() : nullptr, depth, c_it, c_it_end) {}

TreeIter::Step &TreeIter::Step::operator++() {
  assert(child_it_ != child_it_end_);
  ++child_it_;
  node_ = child_it_ != child_it_end_ 
    ? child_it_.get_child_node()
    : nullptr;
  return *this;
}

TreeIter::Step TreeIter::Step::operator++(int) {
  auto old = *this;
  operator++();
  return old;
}

TreeIter::TreeIter(std::vector<Step> traversal_stack) : traversal_stack_(traversal_stack) {
  seek_leaf();
}

TreeIter TreeIter::min(Node *root) {
  return TreeIter::greater_equal(root, "");
}

TreeIter TreeIter::greater_equal(Node *root, const char *key) {
  assert(root != nullptr);

  int key_len = std::strlen(key);
  InnerNode *cur_InnerNode;
  ChildIter child_it, child_it_end;
  std::vector<TreeIter::Step> traversal_stack;

  // sentinel child iterator for root
  traversal_stack.push_back({root, 0, {nullptr, -2}, {nullptr, -1}});

  while (true) {
    TreeIter::Step &cur_Step = traversal_stack.back();
    Node *cur_node = cur_Step.node_;
    int cur_depth = cur_Step.depth_;

    int prefix_match_len = std::min<int>(cur_node->check_prefix(key + cur_depth, key_len - cur_depth), key_len - cur_depth);
    // if search key "equals" the prefix
    if (key_len == cur_depth + prefix_match_len) {
        return TreeIter(traversal_stack);
    }
    // if search key is "greater than" the prefix
    if (prefix_match_len < cur_node->prefix_len_ &&  key[cur_depth + prefix_match_len] > cur_node->prefix_[prefix_match_len]) {
      ++cur_Step;
      return TreeIter(traversal_stack);
    }
    if (cur_node->is_leaf()) {
      continue;
    }
    // seek subtree where search key is "lesser than or equal" the subtree partial key
    cur_InnerNode = static_cast<InnerNode *>(cur_node);
    child_it = cur_InnerNode->begin();
    child_it_end = cur_InnerNode->end();
    // TODO more efficient with specialized node search method?
    for (; child_it != child_it_end; ++child_it) {
      if (key[cur_depth + cur_node->prefix_len_] <= child_it.get_partial_key()) {
        break;
      }
    }
    traversal_stack.push_back({cur_depth + cur_node->prefix_len_ + 1, child_it, child_it_end});
  }
}

char* TreeIter::operator*() {
  assert(get_node()->is_leaf());
  return static_cast<LeafNode*>(get_node())->value_;
}

char** TreeIter::operator->() {
  assert(get_node()->is_leaf());
  return &static_cast<LeafNode *>(get_node())->value_;
}

TreeIter &TreeIter::operator++() {
  assert(get_node()->is_leaf());
  ++get_Step();
  seek_leaf();
  return *this;
}

TreeIter TreeIter::operator++(int) {
  auto old = *this;
  operator++();
  return old;
}

bool TreeIter::operator==(const TreeIter &rhs) const {
  return (traversal_stack_.empty() && rhs.traversal_stack_.empty()) ||
         (!traversal_stack_.empty() && !rhs.traversal_stack_.empty() &&
          get_node() == rhs.get_node());
}

 bool TreeIter::operator!=(const TreeIter &rhs) const {
  return !(*this == rhs);
}

Node * TreeIter::get_node() const {
  return get_Step().node_;
}

int TreeIter::get_depth() const {
  return get_Step().depth_;
}

TreeIter::Step &TreeIter::get_Step() {
  assert(!traversal_stack_.empty());
  return traversal_stack_.back();
}

const TreeIter::Step &TreeIter::get_Step() const {
  assert(!traversal_stack_.empty());
  return traversal_stack_.back();
}

void TreeIter::seek_leaf() {

  /* traverse up until a node on the right is found or stack gets empty */
  for (; get_Step().child_it_ == get_Step().child_it_end_; ++get_Step()) {
    traversal_stack_.pop_back();
    if (traversal_stack_.empty()) {
      return;
    }
  }
  
  /* find leftmost leaf node */
  while (!get_node()->is_leaf()) {
    InnerNode *cur_InnerNode = static_cast<InnerNode *>(get_node());
    int depth = get_Step().depth_ + get_node()->prefix_len_ + 1;
    ChildIter c_it = cur_InnerNode->begin();
    ChildIter c_it_end = cur_InnerNode->end();
    traversal_stack_.push_back({depth, c_it, c_it_end});
  }
}

} // namespace art

