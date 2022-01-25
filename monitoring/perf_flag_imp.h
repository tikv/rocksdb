#include <cstdint>
#include "rocksdb/perf_flag.h"

namespace rocksdb {
#ifdef ROCKSDB_SUPPORT_THREAD_LOCAL
extern __thread uint8_t perf_flags[FLAGS_LEN];
#else
extern uint8_t perf_flags[FLAGS_LEN];
#endif
}
