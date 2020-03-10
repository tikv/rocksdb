// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>
#include <mutex>
#include <queue>
#include <vector>

namespace rocksdb {
class SafeFuncQueue {
 public:
  SafeFuncQueue() : que_(10000), que_len_(0), start_(0), end_(0){}

  ~SafeFuncQueue() {}

  bool RunFunc() {
    if (0 == que_len_.load(std::memory_order_relaxed)) {
      return false;
    }
    mu_.lock();
    uint32_t index = 0;
    if (start_ == end_) {
      mu_.unlock();
      return false;
    }
    que_len_.fetch_sub(1, std::memory_order_relaxed);
    index = start_ ++;
    if (start_ >= que_.size()) {
      start_ -= que_.size();
    }
    mu_.unlock();
    que_[index]();
    return true;
  }

  void Push(std::function<void()> &&v) {
    mu_.lock();
    if (que_len_.load(std::memory_order_relaxed) == que_.size()) {
      mu_.unlock();
      v();
    } else {
      que_[end_ ++] = std::move(v);
      if (end_ >= que_.size()) {
        end_ -= que_.size();
      }
      que_len_.fetch_add(1, std::memory_order_relaxed);
      mu_.unlock();
    }
  }

 private:
  std::vector<std::function<void()>> que_;
  std::mutex mu_;
  std::atomic<uint64_t> que_len_;
  uint32_t start_;
  uint32_t end_;
};
}  // namespace rocksdb
