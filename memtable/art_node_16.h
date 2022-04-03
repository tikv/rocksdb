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
public:
   Node16() {
   }
   ~Node16() {}
   std::atomic<Node*>* find_child(char partial_key) override;
  void set_child(char partial_key, Node *child) override;
  InnerNode *grow(Allocator* allocator) override;
  bool is_full() const override;

  char next_partial_key(char partial_key) const override;

  char prev_partial_key(char partial_key) const override;

  int n_children() const override;

   std::atomic<uint8_t> n_children_;
private:
  std::atomic<uint64_t> keys_[2];
  std::atomic<Node*> children_[16];
};

 std::atomic<Node*> *Node16::find_child(char partial_key) {
   uint8_t key = partial_key + 128;
   uint8_t n_children = n_children_.load(std::memory_order_acquire);
   uint32_t keys = keys_[0].load(std::memory_order_acquire);
   uint8_t l = std::min(n_children, (uint8_t)8);
   for (uint8_t i = 0; i < l; ++i) {
     if ((keys & 255) == key) {
       return &children_[i];
     }
     keys >>= 8;
   }
   if (n_children > 8) {
     n_children -= 8;
     keys = keys_[1].load(std::memory_order_acquire);
     for (uint8_t i = 0; i < n_children; ++i) {
       if ((keys & 255) == key) {
         return &children_[i + 8];
       }
       keys >>= 8;
     }
   }
  return nullptr;
}


void Node16::set_child(char partial_key, Node *child) {
  /* determine index for child */
  uint8_t child_i = n_children_.load(std::memory_order_relaxed);
  uint8_t key = partial_key + 128;
  if (child_i < 8) {
    uint64_t k = keys_[0].load(std::memory_order_relaxed);
    keys_[0].store(k | key << child_i, std::memory_order_release);
  } else {
    uint64_t k = keys_[1].load(std::memory_order_relaxed);
    keys_[1].store(k | key << (child_i - 8), std::memory_order_release);
  }
  children_[child_i].store(child, std::memory_order_release);
  n_children_.store(child_i + 1, std::memory_order_release);
}

 InnerNode *Node16::grow(Allocator* allocator) {
  auto new_node = new (allocator->AllocateAligned(sizeof(Node48)))Node48();
  uint8_t n_children = n_children_.load(std::memory_order_acquire);
  uint32_t keys = keys_[0].load(std::memory_order_acquire);
  uint8_t l = std::min(n_children, (uint8_t)8);
  for (uint8_t i = 0; i < l; ++i) {
    new_node->set_child(keys & 255, children_[i].load(std::memory_order_relaxed));
    keys >>= 8;
  }
  if (n_children > 8) {
    n_children -= 8;
    keys = keys_[1].load(std::memory_order_acquire);
    for (uint8_t i = 0; i < n_children; ++i) {
      new_node->set_child(keys & 255, children_[i + 8].load(std::memory_order_relaxed));
      keys >>= 8;
    }
  }
  return new_node;
}


 bool Node16::is_full() const {
  return n_children_ == 16;
}

 char Node16::next_partial_key(char partial_key) const {
   uint8_t n_children = n_children_.load(std::memory_order_acquire);
   uint8_t key = partial_key + 128;
   uint32_t keys = keys_[0].load(std::memory_order_acquire);
   uint8_t l = std::min(n_children, (uint8_t)8);
   for (uint8_t i = 0; i < l; ++i) {
     if ((keys & 255) >= key) {
       return (keys & 255) - 128;
     }
     keys >>= 8;
   }
   if (n_children > 8) {
     n_children -= 8;
     keys = keys_[1].load(std::memory_order_acquire);
     for (uint8_t i = 0; i < n_children; ++i) {
       if ((keys & 255) >= key) {
         return (keys & 255) - 128;
       }
       keys >>= 8;
     }
   }
   return 127;
}

 char Node16::prev_partial_key(char partial_key) const {
//  for (int i = n_children_ - 1; i >= 0; --i) {
//    if (keys_[i] <= partial_key) {
//      return keys_[i];
//    }
//  }
  throw std::out_of_range("provided partial key does not have a predecessor");
}

 int Node16::n_children() const { return n_children_; }

} // namespace rocksdb

