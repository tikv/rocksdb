/**
 * @file adaptive radix tree
 * @author Rafael Kallis <rk@rafaelkallis.com>
 */

#pragma once

#include "memtable/art_leaf_node.h"
#include "memtable/art_inner_node.h"
#include "memtable/art_node.h"
#include "memtable/art_node_4.h"
#include "memtable/art_tree_it.h"
#include <algorithm>
#include <iostream>
#include <stack>

namespace rocksdb {

class AdaptiveRadixTree {
public:
  ~AdaptiveRadixTree();

  /**
   * Finds the value associated with the given key.
   *
   * @param key - The key to find.
   * @return the value associated with the key or a nullptr.
   */
  char* get(const char *key) const;

  /**
   * Associates the given key with the given value.
   * If another value is already associated with the given key,
   * since the method consumer is the resource owner.
   *
   * @param key - The key to associate with the value.
   * @param value - The value to be associated with the key.
   * @return a nullptr if no other value is associated with they or the
   * previously associated value.
   */
  char *set(const char *key, char *value);

  /**
   * Deletes the given key and returns it's associated value.
   * The associated value is returned,
   * since the method consumer is the resource owner.
   * If no value is associated with the given key, nullptr is returned.
   *
   * @param key - The key to delete.
   * @return the values assciated with they key or a nullptr otherwise.
   */
  char* del(const char *key);

  /**
   * Forward iterator that traverses the tree in lexicographic order.
   */
  TreeIter begin();

  /**
   * Forward iterator that traverses the tree in lexicographic order starting
   * from the provided key.
   */
  TreeIter begin(const char *key);

  /**
   * Iterator to the end of the lexicographic order.
   */
  TreeIter end();

private:
  Node *root_ = nullptr;
};

AdaptiveRadixTree::~AdaptiveRadixTree() {
  if (root_ == nullptr) {
    return;
  }
  std::stack<Node *> node_stack;
  node_stack.push(root_);
  Node *cur;
  InnerNode *cur_inner;
  ChildIter it, it_end;
  while (!node_stack.empty()) {
    cur = node_stack.top();
    node_stack.pop();
    if (!cur->is_leaf()) {
      cur_inner = static_cast<InnerNode*>(cur);
      for (it = cur_inner->begin(), it_end = cur_inner->end(); it != it_end; ++it) {
        node_stack.push(*cur_inner->find_child(*it));
      }
    }
    if (cur->prefix_ != nullptr) {
      delete[] cur->prefix_;
    }
    delete cur;
  }
}

char* AdaptiveRadixTree::get(const char *key) const {
  Node *cur = root_, **child;
  int depth = 0, key_len = std::strlen(key) + 1;
  while (cur != nullptr) {
    if (cur->prefix_len_ != cur->check_prefix(key + depth, key_len - depth)) {
      /* prefix mismatch */
      return nullptr;
    }
    if (cur->prefix_len_ == key_len - depth) {
      /* exact match */
      return cur->is_leaf() ? static_cast<LeafNode*>(cur)->value_ : nullptr;
    }
    child = static_cast<InnerNode*>(cur)->find_child(key[depth + cur->prefix_len_]);
    depth += (cur->prefix_len_ + 1);
    cur = child != nullptr ? *child : nullptr;
  }
  return nullptr;
}

char* AdaptiveRadixTree::set(const char *key, char* value) {
  int key_len = std::strlen(key) + 1, depth = 0, prefix_match_len;
  if (root_ == nullptr) {
    root_ = new LeafNode(value);
    root_->prefix_ = new char[key_len];
    std::copy(key, key + key_len + 1, root_->prefix_);
    root_->prefix_len_ = key_len;
    return nullptr;
  }

  Node **cur = &root_, **child;
  InnerNode **cur_inner;
  char child_partial_key;
  bool is_prefix_match;

  while (true) {
    /* number of bytes of the current node's prefix that match the key */
    prefix_match_len = (**cur).check_prefix(key + depth, key_len - depth);

    /* true if the current node's prefix matches with a part of the key */
    is_prefix_match = (std::min<int>((**cur).prefix_len_, key_len - depth)) ==
                      prefix_match_len;

    if (is_prefix_match && (**cur).prefix_len_ == key_len - depth) {
      /* exact match:
       * => "replace"
       * => replace value of current node.
       * => return old value to caller to handle.
       *        _                             _
       *        |                             |
       *       (aa)                          (aa)
       *    a /    \ b     +[aaaaa,v3]    a /    \ b
       *     /      \      ==========>     /      \
       * *(aa)->v1  ()->v2             *(aa)->v3  ()->v2
       *
       */

      /* cur must be a leaf */
      auto cur_leaf = static_cast<LeafNode*>(*cur);
      char *old_value = cur_leaf->value_;
      cur_leaf->value_ = value;
      return old_value;
    }

    if (!is_prefix_match) {
      /* prefix mismatch:
       * => new parent node with common prefix and no associated value.
       * => new node with value to insert.
       * => current and new node become children of new parent node.
       *
       *        |                        |
       *      *(aa)                    +(a)->Ø
       *    a /    \ b     +[ab,v3]  a /   \ b
       *     /      \      =======>   /     \
       *  (aa)->v1  ()->v2          *()->Ø +()->v3
       *                          a /   \ b
       *                           /     \
       *                        (aa)->v1 ()->v2
       *                        /|\      /|\
       */

      auto new_parent = new Node4();
      new_parent->prefix_ = new char[prefix_match_len];
      std::copy((**cur).prefix_, (**cur).prefix_ + prefix_match_len,
                new_parent->prefix_);
      new_parent->prefix_len_ = prefix_match_len;
      new_parent->set_child((**cur).prefix_[prefix_match_len], *cur);

      // TODO(rafaelkallis): shrink?
      /* memmove((**cur).prefix_, (**cur).prefix_ + prefix_match_len + 1, */
      /*         (**cur).prefix_len_ - prefix_match_len - 1); */
      /* (**cur).prefix_len_ -= prefix_match_len + 1; */

      auto old_prefix = (**cur).prefix_;
      auto old_prefix_len = (**cur).prefix_len_;
      (**cur).prefix_ = new char[old_prefix_len - prefix_match_len - 1];
      (**cur).prefix_len_ = old_prefix_len - prefix_match_len - 1;
      std::copy(old_prefix + prefix_match_len + 1, old_prefix + old_prefix_len,
                (**cur).prefix_);
      delete old_prefix;

      auto new_node = new LeafNode(value);
      new_node->prefix_ = new char[key_len - depth - prefix_match_len - 1];
      std::copy(key + depth + prefix_match_len + 1, key + key_len,
                new_node->prefix_);
      new_node->prefix_len_ = key_len - depth - prefix_match_len - 1;
      new_parent->set_child(key[depth + prefix_match_len], new_node);

      *cur = new_parent;
      return nullptr;
    }

    /* must be inner node */
    cur_inner = reinterpret_cast<InnerNode**>(cur);
    child_partial_key = key[depth + (**cur).prefix_len_];
    child = (**cur_inner).find_child(child_partial_key);

    if (child == nullptr) {
      /*
       * no child associated with the next partial key.
       * => create new node with value to insert.
       * => new node becomes current node's child.
       *
       *      *(aa)->Ø              *(aa)->Ø
       *    a /        +[aab,v2]  a /    \ b
       *     /         ========>   /      \
       *   (a)->v1               (a)->v1 +()->v2
       */

      if ((**cur_inner).is_full()) {
        *cur_inner = (**cur_inner).grow();
      }

      auto new_node = new LeafNode(value);
      new_node->prefix_ = new char[key_len - depth - (**cur).prefix_len_ - 1];
      std::copy(key + depth + (**cur).prefix_len_ + 1, key + key_len,
                new_node->prefix_);
      new_node->prefix_len_ = key_len - depth - (**cur).prefix_len_ - 1;
      (**cur_inner).set_child(child_partial_key, new_node);
      return nullptr;
    }

    /* propagate down and repeat:
     *
     *     *(aa)->Ø                   (aa)->Ø
     *   a /    \ b    +[aaba,v3]  a /    \ b     repeat
     *    /      \     =========>   /      \     ========>  ...
     *  (a)->v1  ()->v2           (a)->v1 *()->v2
     */

    depth += (**cur).prefix_len_ + 1;
    cur = child;
  }
}


TreeIter AdaptiveRadixTree::begin() {
  return TreeIter::min(this->root_);
}

TreeIter AdaptiveRadixTree::begin(const char *key) {
  return TreeIter::greater_equal(this->root_, key);
}

TreeIter AdaptiveRadixTree::end() { return TreeIter(); }

} // namespace rocksdb
