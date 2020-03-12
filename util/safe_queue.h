// Copyright (c) 2011-present, Facebook, Inc. All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>
#include <mutex>
#include <queue>
#include <deque>

namespace rocksdb {
class SafeFuncQueue {
private:
    struct Item {
        std::function<void()> func;
    };
 public:
  SafeFuncQueue() : que_len_(0){}

  ~SafeFuncQueue() {}

  bool RunFunc() {
    if (0 == que_len_.load(std::memory_order_acquire)) {
      return false;
    }
    mu_.lock();
    if (que_.empty()) {
      mu_.unlock();
      return false;
    }
    auto func = std::move(que_.front().func);
    que_.pop_front();
    que_len_.fetch_sub(1, std::memory_order_relaxed);
    mu_.unlock();
    func();
    return true;
  }

  void Push(std::function<void()> &&v) {
    mu_.lock();
    que_.emplace_back();
    que_.back().func = std::move(v);
    que_len_.fetch_add(1, std::memory_order_relaxed);
    mu_.unlock();
  }

 private:
  std::deque<Item> que_;
  std::mutex mu_;
  std::atomic<uint32_t> que_len_;
};

}  // namespace rocksdb
