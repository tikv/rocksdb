// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <deque>
#include <memory>
#include <mutex>
#include <queue>

namespace rocksdb {
class SafeFuncQueue {
 private:
  struct Item {
    std::function<void()> func;
  };

 public:
  SafeFuncQueue() : len_(0) {}

  ~SafeFuncQueue() {}

  bool RunFunc() {
    // The memory order does not affect the correctness because we update it
    // with mutex hold so relaxed is enough.
    if (len_.load(std::memory_order_relaxed) == 0) {
      return false;
    }
    mu_.lock();
    if (que_.empty()) {
      mu_.unlock();
      return false;
    }
    auto func = std::move(que_.front().func);
    que_.pop_front();
    len_.fetch_sub(1, std::memory_order_relaxed);
    mu_.unlock();
    func();
    return true;
  }

  void Push(std::function<void()> &&v) {
    std::lock_guard<std::mutex> _guard(mu_);
    que_.emplace_back();
    que_.back().func = std::move(v);
    len_.fetch_add(1, std::memory_order_relaxed);
  }

 private:
  std::deque<Item> que_;
  std::mutex mu_;
  std::atomic<size_t> len_;
};

}  // namespace rocksdb
