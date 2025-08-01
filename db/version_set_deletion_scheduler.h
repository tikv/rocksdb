//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <memory>
#include <queue>

#include "port/port.h"
#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class VersionSet;
class VersionStorageInfo;
class Logger;

// VersionSetDeletionScheduler provides a dedicated background thread for
// handling VersionStorageInfo deletion operations that were previously executed
// via env_->Schedule() in the LOW priority thread pool.
//
// This dedicated thread helps to:
// 1. Isolate version storage deletion operations from other background work
// 2. Avoid potential interference with other low-priority operations
// 3. Provide better control over deletion timing and parallelism
//
class VersionSetDeletionScheduler {
 public:
  explicit VersionSetDeletionScheduler(Logger* info_log);
  ~VersionSetDeletionScheduler();

  // Schedule a VersionStorageInfo deletion to be executed in the background
  // thread
  void ScheduleDeletion(VersionStorageInfo* storage_info);

  // Wait for all pending operations to complete and shutdown the background
  // thread
  void Shutdown();

 private:
  // Entry point for the background thread
  void BackgroundDeletionThread();

  // Mutex to protect internal state
  mutable port::Mutex version_delete_mutex_;

  // Condition variable for thread coordination
  port::CondVar cv_;

  // Queue of deletion operations
  std::queue<VersionStorageInfo*> deletion_queue_;

  // Background thread for executing deletion operations
  std::unique_ptr<port::Thread> bg_thread_;

  // Flag to indicate shutdown
  bool shutting_down_;

  // Number of pending deletion operations
  int pending_deletion_count_;

  // Logger for debug/info messages
  Logger* info_log_;
};

}  // namespace ROCKSDB_NAMESPACE
