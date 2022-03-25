/**
 * @file LeafNode header
 * @author Rafael Kallis <rk@rafaelkallis.com>
 */


#pragma once

#include "memtable/art_node.h"

namespace rocksdb {

class LeafNode : public Node {
public:
  explicit LeafNode(char *value);
  bool is_leaf() const override;

  char* value_;
};

LeafNode::LeafNode(char *value): value_(value) {}

bool LeafNode::is_leaf() const { return true; }

} // namespace rocksdb

