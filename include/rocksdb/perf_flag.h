// complements to perf_level
#pragma once

#include <bitset>
#include <initializer_list>

#include "perf_flag_defs.h"

namespace rocksdb {
using PerfFlags = std::bitset<PerfFlag::COUNT>;

PerfFlags NewPerfFlags(std::initializer_list<PerfFlag> l);
bool CheckPerfFlag(PerfFlag flag);
PerfFlags GetPerfFlags();
void SetPerfFlags(PerfFlags flags);
}  // namespace rocksdb
