#include "monitoring/perf_flags_imp.h"
namespace rocksdb {
const PerfFlags PerfFlagsEnableCount = {
    //    .perf_level = 2,
    PERF_FLAGS_ENABLE_COUNT};

const PerfFlags PerfFlagsEnableTimeExceptForMutex = {
    //    .perf_level = 3,
    PERF_FLAGS_ENABLE_COUNT, PERF_FLAGS_ENABLE_TIME_EXCEPT_FOR_MUTEX};
const PerfFlags PerfFlagsEnableTimeAndCPUTimeExceptForMutex = {
    //    .perf_level = 4,
    PERF_FLAGS_ENABLE_COUNT, PERF_FLAGS_ENABLE_TIME_EXCEPT_FOR_MUTEX,
    PERF_FLAGS_ENABLE_TIME_AND_CPU_TIME_EXCEPT_FOR_MUTEX};

const PerfFlags PerfFlagsEnableTime = {
    //    .perf_level = 5,
    PERF_FLAGS_ENABLE_COUNT,
    PERF_FLAGS_ENABLE_TIME_EXCEPT_FOR_MUTEX,
    PERF_FLAGS_ENABLE_TIME_AND_CPU_TIME_EXCEPT_FOR_MUTEX,
    .enable_db_mutex_lock_nanos_bit = 1,
    .enable_db_condition_wait_nanos_bit = 1,
};

// set default value of perf_flags
#ifdef ROCKSDB_SUPPORT_THREAD_LOCAL
__thread PerfFlags perf_flags = {.level2_by_mask = (uint64_t)-1};
#else
PerfFlags perf_flags = PerfFlagsEnableCount;
#endif

void SetPerfFlags(PerfFlags pbf) { perf_flags = pbf; }

PerfFlags GetPerfFlags() { return perf_flags; }

}  // namespace rocksdb
