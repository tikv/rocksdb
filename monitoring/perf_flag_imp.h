#include <cstdint>
#include "rocksdb/perf_flag.h"

namespace rocksdb {
#ifdef ROCKSDB_SUPPORT_THREAD_LOCAL
extern __thread PerfFlags perf_flags;
#else
extern PerfFlags perf_flags;
#endif
}  // namespace rocksdb
