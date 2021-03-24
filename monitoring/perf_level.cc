//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "rocksdb/perf_level.h"

#include <assert.h>

#include "monitoring/perf_flags_imp.h"

namespace rocksdb {

void SetPerfLevel(PerfLevel level) {
  // TODO: discuss keep assertions or not
  assert(level > kUninitialized);
  assert(level < kOutOfBounds);
  switch (level) {
    case kEnableCount:
      perf_flags = PerfFlagsEnableCount;
      break;
    case kEnableTimeExceptForMutex:
      perf_flags = PerfFlagsEnableTimeExceptForMutex;
      break;
    case kEnableTimeAndCPUTimeExceptForMutex:
      perf_flags = PerfFlagsEnableTimeAndCPUTimeExceptForMutex;
      break;
    case kEnableTime:
      perf_flags = PerfFlagsEnableTime;
      break;
    default:
      perf_flags = {};
      break;
  }
  //  perf_flags.perf_level = level;
}

// PerfLevel GetPerfLevel() { return (PerfLevel)perf_flags.perf_level; }

}  // namespace rocksdb
