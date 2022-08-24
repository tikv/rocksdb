/**
 * @file InnerNode header
 * @author Rafael Kallis <rk@rafaelkallis.com>
 */

#pragma once

#include <algorithm>
#include <array>
#include <cassert>
#include <cstring>
#include <iostream>
#include <iterator>
#include <stdexcept>

#include "memory/allocator.h"

namespace rocksdb {

struct Node;

class InnerNode {
public:
  virtual ~InnerNode() {}

  /**
   * Finds and returns the child Node identified by the given partial key.
   *
   * @param partial_key - The partial key associated with the child.
   * @return Child Node identified by the given partial key or
   * a null pointer of no child Node is associated with the partial key.
   */
  virtual std::atomic<Node*>* find_child(uint8_t partial_key) = 0;

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
  virtual void set_child(uint8_t partial_key, Node* child) = 0;
  virtual const char* node_type() const = 0;

  /**
   * Creates and returns a new Node with bigger children capacity.
   * The current Node gets deleted.
   *
   * @return Node with bigger capacity
   */
  virtual InnerNode *grow(Allocator* allocator) = 0;

  /**
   * Determines if the Node is full, i.e. can carry no more child Nodes.
   */
  virtual bool is_full() const = 0;

  virtual uint8_t next_partial_key(uint8_t partial_key) const = 0;

  virtual uint8_t prev_partial_key(uint8_t partial_key) const = 0;
};


}
