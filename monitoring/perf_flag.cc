#include "rocksdb/perf_flag.h"

namespace rocksdb {
#ifdef ROCKSDB_SUPPORT_THREAD_LOCAL
__thread uint8_t perf_flags[FLAGS_LEN] = {0};
#else
uint8_t perf_flags[FLAGS_LEN] = {0};
#endif

void EnablePerfFlag(uint64_t flag) {
  if (CheckPerfFlag(flag)) {
  } else {
    // & 0b111 means find the flag location is a alternative way to do mod operation
    GET_FLAG(flag) ^= (uint64_t)0b1 << ((uint64_t)flag & (uint64_t)0b111);
  }
}

void DisablePerfFlag(uint64_t flag) {
  if (CheckPerfFlag(flag)) {
    GET_FLAG(flag) ^= (uint64_t)0b1 << ((uint64_t)flag & (uint64_t)0b111);
  } else {
  }
}

bool CheckPerfFlag(uint64_t flag) {
  auto _1 = (uint64_t)0b1 << (flag & (uint64_t)0b111);
  auto _2 = GET_FLAG(flag);
  auto _3 = _2 & _1;
  return ((uint64_t)GET_FLAG(flag) & (uint64_t)0b1
                                         << (flag & (uint64_t)0b111)) != 0;
}

}  // namespace rocksdb