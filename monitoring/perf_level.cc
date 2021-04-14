//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "rocksdb/perf_level.h"

#include <assert.h>

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
// get the estimated perf level
PerfLevel GetPerfLevel() {
  int cmp5 = memcmp(&perf_flags, &PERF_LEVEL5, sizeof(PerfFlags));
  if (cmp5 == 0) {
    return kEnableTime;
  } else if (cmp5 < 0) {
    int cmp4 = memcmp(&perf_flags, &PERF_LEVEL4, sizeof(PerfFlags));
    if (cmp4 == 0) {
      return kEnableTimeAndCPUTimeExceptForMutex;
    } else if (cmp4 < 0) {
      int cmp3 = memcmp(&perf_flags, &PERF_LEVEL3, sizeof(PerfFlags));
      if (cmp3 == 0) {
        return kEnableTimeExceptForMutex;
      } else if (cmp3 < 0) {
        int cmp2 = memcmp(&perf_flags, &PERF_LEVEL2, sizeof(PerfFlags));
        if (cmp2 == 0) {
          return kEnableCount;
        } else if (cmp2 < 0) {
          PerfFlags empt = {};
          int cmp1 = memcmp(&perf_flags, &empt, sizeof(PerfFlags));
          if (cmp1 == 0) {
            return kDisable;
          } else if (cmp1 < 0) {
            abort();
          } else {
            return kCustomFlags;
          }
        } else {
          return kCustomFlags;
        }
      } else {
        return kCustomFlags;
      }
    } else {
      return kCustomFlags;
    }
  } else {
    abort();
  }
}

}  // namespace rocksdb
