//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//

#include "rocksdb/perf_level.h"

#include <assert.h>

#include "monitoring/perf_level_by_bitfield_imp.h"

namespace rocksdb {

void SetPerfLevel(PerfLevel level) {
  // TODO: discuss keep assertions or not
  assert(level > kUninitialized);
  assert(level < kOutOfBounds);
  switch (level) {
    case kEnableCount:
      perf_bit_field = BitFieldEnableCount;
      break;
    case kEnableTimeExceptForMutex:
      perf_bit_field = BitFieldEnableTimeExceptForMutex;
      break;
    case kEnableTimeAndCPUTimeExceptForMutex:
      perf_bit_field = BitFieldEnableTimeAndCPUTimeExceptForMutex;
      break;
    case kEnableTime:
      perf_bit_field = BitFieldEnableTime;
      break;
    default:
      perf_bit_field = {};
      break;
  }
  perf_bit_field.perf_level = level;
}

PerfLevel GetPerfLevel() { return (PerfLevel)perf_bit_field.perf_level; }

}  // namespace rocksdb
