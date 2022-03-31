/**
 * @file adaptive radix tree
 * @author Rafael Kallis <rk@rafaelkallis.com>
 */

#pragma once

#include "memtable/art_inner_node.h"
#include "memtable/art_node.h"
#include "memtable/art_node_4.h"
#include "memory/allocator.h"
#include <algorithm>
#include <iostream>
#include <stack>

namespace rocksdb {

class AdaptiveRadixTree {
public:
  AdaptiveRadixTree(Allocator* allocator);
  ~AdaptiveRadixTree();

  /**
   * Finds the value associated with the given key.
   *
   * @param key - The key to find.
   * @return the value associated with the key or a nullptr.
   */
  char* Get(const char *key) const;

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
  char *Insert(const char *key, int key_len, char* v);


  char* AllocateLeafNode(const char* v, size_t value_size);
  Node* AllocateNode(InnerNode* inner, size_t prefix_size);

private:
  std::atomic<Node*> root_;
  Allocator* allocator_;
};

AdaptiveRadixTree::~AdaptiveRadixTree() {
}

AdaptiveRadixTree::AdaptiveRadixTree(Allocator* allocator)
  : allocator_(allocator){
  root_.store(nullptr, std::memory_order_relaxed);
}

char* AdaptiveRadixTree::Get(const char *key) const {
  Node *cur = root_.load(std::memory_order_acquire);
  std::atomic<Node*>* child = nullptr;
  int depth = 0, key_len = std::strlen(key) + 1;
  while (cur != nullptr) {
    if (cur->prefix_len != cur->check_prefix(key, depth, key_len)) {
      /* prefix mismatch */
      return nullptr;
    }
    if (cur->prefix_len == key_len - depth) {
      /* exact match */
      return cur->value;
    }

    if (cur->inner == nullptr) {
      return nullptr;
    }
    child = cur->inner->find_child(key[depth + cur->prefix_len]);
    depth += cur->prefix_len + 1;
    cur = child != nullptr ? child->load(std::memory_order_acquire) : nullptr;
  }
  return nullptr;
}

Node* AdaptiveRadixTree::AllocateNode(InnerNode* inner, size_t prefix_size) {
  size_t extra_prefix = prefix_size;
  if (extra_prefix > 0) {
    extra_prefix -= 1;
  }
  char* addr = allocator_->AllocateAligned(sizeof(Node) + extra_prefix);
  Node* node = reinterpret_cast<Node*>(addr);
  node->inner = inner;
  node->value = nullptr;
  node->prefix_len = prefix_size;
  return node;
}


char* AdaptiveRadixTree::Insert(const char *key, int l, char* leaf) {
  int key_len = std::strlen(key) + 1, depth = 0, prefix_match_len;

  std::atomic<Node*>* cur_address = &root_;
  Node *cur = root_.load(std::memory_order_relaxed);
  if (cur == nullptr) {
    Node* root = AllocateNode(nullptr, l);
    root->value = leaf;
    memcpy(root->prefix, key, l);
    root_.store(root, std::memory_order_release);
    return nullptr;
  }

  char child_partial_key;

  while (true) {
    /* number of bytes of the current node's prefix that match the key */
    prefix_match_len = cur->check_prefix(key, depth, key_len);

    /* true if the current node's prefix matches with a part of the key */
    bool is_prefix_match = cur->prefix_len == prefix_match_len;

    if (is_prefix_match && cur->prefix_len == key_len - depth) {
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
      cur->value = leaf;
      return cur->value;
    } else if (!is_prefix_match) {
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

      InnerNode* inner = new (allocator_->AllocateAligned(sizeof(Node4)))Node4();
      Node* new_parent = AllocateNode(inner, prefix_match_len);
      memcpy(new_parent->prefix, cur->prefix, prefix_match_len);

      int old_prefix_len = cur->prefix_len;
      int new_prefix_len = old_prefix_len - prefix_match_len - 1;

      Node* new_cur = AllocateNode(cur->inner, new_prefix_len);
      new_cur->value = cur->value;
      if (new_prefix_len > 0) {
        memcpy(new_cur->prefix, cur->prefix + prefix_match_len + 1, new_prefix_len);
      }
      inner->set_child(cur->prefix[prefix_match_len], cur);
      if (depth + prefix_match_len < key_len) {
        size_t leaf_prefix_len = key_len - depth - prefix_match_len - 1;
        Node* new_node = AllocateNode(nullptr, leaf_prefix_len);
        new_node->value = leaf;
        if (leaf_prefix_len > 0) {
          memcpy(new_node->prefix, key + depth + prefix_match_len + 1, leaf_prefix_len);
        }
        inner->set_child(key[depth + prefix_match_len], new_node);
      } else {
        new_parent->value = leaf;
      }
      cur_address->store(new_parent, std::memory_order_release);
      return nullptr;
    }

    assert(depth + cur->prefix_len < key_len);
    /* must be inner node */
    child_partial_key = key[depth + cur->prefix_len];
    if (cur->inner == nullptr) {
      Node4* new_inner = new (allocator_->AllocateAligned(sizeof(Node4))) Node4();
      cur->inner = new_inner;
    }
    std::atomic<Node*>* child = cur->inner->find_child(child_partial_key);

    if (child == nullptr || child->load(std::memory_order_relaxed) == nullptr) {
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

      if (cur->inner->is_full()) {
        cur->inner = cur->inner->grow(allocator_);
      }
      size_t leaf_prefix_len = key_len - depth - cur->prefix_len - 1;
      Node* new_node = AllocateNode(nullptr, leaf_prefix_len);
      new_node->value = leaf;
      if (leaf_prefix_len > 0) {
        memcpy(new_node->prefix, key + depth + cur->prefix_len + 1, leaf_prefix_len);
      }
      cur->inner->set_child(child_partial_key, new_node);
      return nullptr;
    }

    /* propagate down and repeat:
     *
     *     *(aa)->Ø                   (aa)->Ø
     *   a /    \ b    +[aaba,v3]  a /    \ b     repeat
     *    /      \     =========>   /      \     ========>  ...
     *  (a)->v1  ()->v2           (a)->v1 *()->v2
     */

    depth += cur->prefix_len + 1;
    cur = child->load(std::memory_order_relaxed);
  }
}
} // namespace rocksdb
