//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "rocksdb/perf_level.h"

#include <cassert>
#include <cstring>

#include "monitoring/perf_flags_imp.h"

namespace rocksdb {

void SetPerfLevel(PerfLevel level) {
  // TODO: discuss keep assertions or not
  assert(level > kUninitialized);
  assert(level < kOutOfBounds);
  switch (level) {
    case kEnableTime:
      perf_flags = PERF_LEVEL5;
      break;
    case kEnableTimeAndCPUTimeExceptForMutex:
      perf_flags = PERF_LEVEL4;
      break;
    case kEnableTimeExceptForMutex:
      perf_flags = PERF_LEVEL3;
      break;
    case kEnableCount:
      perf_flags = PERF_LEVEL2;
      return;
    default:
      perf_flags = {};
      break;
  }
  //  perf_flags.perf_level = level;
}
PerfLevel GetPerfLevel() {
  void* levels[5] = {(void*)&PERF_LEVEL1, (void*)&PERF_LEVEL2,
                     (void*)&PERF_LEVEL3, (void*)&PERF_LEVEL4,
                     (void*)&PERF_LEVEL5};
  for (int i = 0; i < 5; ++i) {
    int cmp = memcmp(&perf_flags, levels[i], sizeof(PerfFlags));
    if (cmp == 0) return (PerfLevel)(i + 1);
    if (cmp < 0 && i == 0) return kOutOfBounds;
    if (cmp < 0) return kCustomFlags;
    if (cmp > 0 && i == 4) return kOutOfBounds;
  }
}

}  // namespace rocksdb
