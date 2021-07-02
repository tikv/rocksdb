// complements to perf_level
#include <cstdint>
#pragma once
#define FLAGS_LEN                               \
  (((uint64_t)FLAG_END & (uint64_t)0b111) == 0  \
       ? ((uint64_t)FLAG_END >> (uint64_t)0b11) \
       : ((uint64_t)FLAG_END >> (uint64_t)0b11) + (uint64_t)1)
namespace rocksdb{
  void EnablePerfFlag(uint64_t flag);
  void DisablePerfFlag(uint64_t flag);
  bool CheckPerfFlag(uint64_t flag);
}

#define GET_FLAG(flag) perf_flags[(uint64_t)(flag) >> (uint64_t)0b11]
#include "perf_flag_defs.h"
