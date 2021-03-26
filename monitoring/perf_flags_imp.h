#pragma once
#include "rocksdb/perf_flags.h"

namespace rocksdb {

#ifdef ROCKSDB_SUPPORT_THREAD_LOCAL
extern __thread PerfFlags perf_flags;
#else
extern PerfLevelByPerfFlags perf_flags;
#endif

}  // namespace rocksdb

#define BITFIELD_METRIC(metric) enable_##metric##_bit
