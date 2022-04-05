//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.  Use of
// this source code is governed by a BSD-style license that can be found
// in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "memtable/art_node.h"
#include "memory/allocator.h"
#include <algorithm>
#include <iostream>
#include <stack>

namespace rocksdb {

class AdaptiveRadixTree {
  struct NodeIterator {
    Node* node_;
    Node* child = nullptr;
    int depth_;
    uint8_t cur_partial_key_ = 0;

    NodeIterator(Node* node, int depth) : node_(node), depth_(depth) {}

    void Next();
    void Prev();
    bool Valid();
    void SeekToFirst();
    void SeekToLast();
  };

 public:
  struct Iterator {
   public:
    std::atomic<Node*>* root_;
    std::vector<NodeIterator> traversal_stack_;
    explicit Iterator(AdaptiveRadixTree* tree) : root_(&tree->root_) {}
    void Seek(const char* key, int l);
    void SeekToFirst();
    void SeekToLast();
    void SeekForPrev(const char* key, int l);
    void Next();
    void Prev();
    bool Valid() const;
    const char* Value() const { return traversal_stack_.back().node_->value; }

   private:
    void SeekForPrevImpl(const char* key, int l);
    void SeekImpl(const char* key, int key_len);
    void SeekLeftLeaf();
    void SeekRightLeaf();
    void SeekBack();
    void SeekForward();
  };

public:
 AdaptiveRadixTree(Allocator* allocator)
     : root_(nullptr), allocator_(allocator) {}
 ~AdaptiveRadixTree() {}

 const char* Get(const char* key, int key_len) const;

 const char* Insert(const char* key, int key_len, const char* v);

 Node* AllocateNode(InnerNode* inner, int prefix_size);
 char* AllocateKey(size_t l) { return allocator_->AllocateAligned(l); }

private:
 std::atomic<Node*> root_;
 Allocator* allocator_;
};

} // namespace rocksdb
