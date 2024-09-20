#include "rocksdb/perf_flag.h"

namespace ROCKSDB_NAMESPACE {

thread_local PerfFlags perf_flags = {};

PerfFlags NewPerfFlags(std::initializer_list<PerfFlag> l) {
  PerfFlags flags;
  for (const PerfFlag& f : l) {
    flags.set(f);
  }
  return flags;
}

bool CheckPerfFlag(PerfFlag flag) { return perf_flags.test(flag); }

PerfFlags GetPerfFlags() { return perf_flags; }

void SetPerfFlags(PerfFlags flags) { perf_flags = flags; }

}  // namespace ROCKSDB_NAMESPACE
