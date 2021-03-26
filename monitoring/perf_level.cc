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
    case kEnableTime:
      perf_flags.level5_by_mask = -1;
    case kEnableTimeAndCPUTimeExceptForMutex:
      perf_flags.level4_by_mask = -1;
    case kEnableTimeExceptForMutex:
      perf_flags.level3_by_mask = -1;
    case kEnableCount:
      perf_flags.level2_by_mask = -1;
      return;
    default:
      perf_flags = {};
      break;
  }
  //  perf_flags.perf_level = level;
}
// get the estimated perf level
PerfLevel GetPerfLevel() {
  if (perf_flags.level5_by_mask != 0) return (PerfLevel)5;
  if (perf_flags.level4_by_mask != 0) return (PerfLevel)4;
  if (perf_flags.level3_by_mask != 0) return (PerfLevel)3;
  if (perf_flags.level2_by_mask != 0) return (PerfLevel)2;
  return PerfLevel::kDisable;
}

}  // namespace rocksdb
