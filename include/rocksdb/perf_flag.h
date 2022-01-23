// complements to perf_level
#pragma once

#include <cstdint>

#include "perf_flag_defs.h"

#define GET_FLAG(flag) perf_flags[(uint64_t)(flag) >> 3]

// FLAGS_LEN = ceiling(FLAG_END / bits(uint8_t))
#define FLAGS_LEN                              \
  (((uint64_t)FLAG_END & (uint64_t)0b111) == 0 \
       ? ((uint64_t)FLAG_END >> 3)             \
       : ((uint64_t)FLAG_END >> 3) + 1)

namespace rocksdb {
void EnablePerfFlag(uint64_t flag);
void DisablePerfFlag(uint64_t flag);
bool CheckPerfFlag(uint64_t flag);
}  // namespace rocksdb
