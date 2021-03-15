#pragma once
#include "rocksdb/perf_level_by_bitfield.h"

namespace rocksdb {

#ifdef ROCKSDB_SUPPORT_THREAD_LOCAL
extern __thread PerfLevelByBitField perf_bit_field;
#else
extern PerfLevelByBitField perf_bit_field;
#endif
extern const PerfLevelByBitField BitFieldEnableCount;
extern const PerfLevelByBitField BitFieldEnableTimeExceptForMutex;
extern const PerfLevelByBitField BitFieldEnableTimeAndCPUTimeExceptForMutex;
extern const PerfLevelByBitField BitFieldEnableTime;

}  // namespace rocksdb

#define BITFIELD_METRIC(metric) enable_##metric##_bit
