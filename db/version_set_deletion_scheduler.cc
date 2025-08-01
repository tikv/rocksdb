//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "db/version_set_deletion_scheduler.h"

#include <memory>

#include "db/version_set.h"
#include "logging/logging.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "util/mutexlock.h"

namespace ROCKSDB_NAMESPACE {

VersionSetDeletionScheduler::VersionSetDeletionScheduler(Logger* info_log)
    : version_delete_mutex_(),
      cv_(&version_delete_mutex_),
      deletion_queue_(),
      bg_thread_(nullptr),
      shutting_down_(false),
      pending_deletion_count_(0),
      info_log_(info_log) {
  // Start the background thread
  bg_thread_.reset(new port::Thread(
      &VersionSetDeletionScheduler::BackgroundDeletionThread, this));

  ROCKS_LOG_INFO(info_log_,
                 "VersionSetDeletionScheduler: Created dedicated background "
                 "thread for storage deletion");
}

VersionSetDeletionScheduler::~VersionSetDeletionScheduler() { Shutdown(); }

void VersionSetDeletionScheduler::ScheduleDeletion(
    VersionStorageInfo* storage_info) {
  MutexLock lock(&version_delete_mutex_);

  if (shutting_down_) {
    return;
  }

  deletion_queue_.push(storage_info);
  pending_deletion_count_++;

  // Notify the background thread that work is available
  cv_.Signal();
}

void VersionSetDeletionScheduler::Shutdown() {
  {
    MutexLock lock(&version_delete_mutex_);
    if (shutting_down_) {
      return;  // Already shutting down
    }
    shutting_down_ = true;
    cv_.Signal();  // Wake up the background thread
  }

  // Wait for the background thread to finish
  if (bg_thread_ != nullptr) {
    bg_thread_->join();
    bg_thread_.reset();
  }
}

void VersionSetDeletionScheduler::BackgroundDeletionThread() {
  version_delete_mutex_.Lock();

  while (!shutting_down_) {
    // Wait for work or shutdown signal
    while (deletion_queue_.empty() && !shutting_down_) {
      cv_.Wait();
    }

    // Process all pending deletion operations
    while (!deletion_queue_.empty() && !shutting_down_) {
      VersionStorageInfo* storage_info = deletion_queue_.front();
      deletion_queue_.pop();
      pending_deletion_count_--;

      // Unlock mutex while executing the deletion operation
      version_delete_mutex_.Unlock();
      delete storage_info;

      // Re-acquire the mutex for the next iteration
      version_delete_mutex_.Lock();
    }
  }

  version_delete_mutex_.Unlock();
}

}  // namespace ROCKSDB_NAMESPACE
