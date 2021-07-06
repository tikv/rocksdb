#include "rocksdb/perf_flag.h"

#include <gtest/gtest.h>
#include <test_util/testharness.h>

#include <functional>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/slice.h"

namespace rocksdb {
class PerfFlagTest : testing::Test {};
TEST(PerfFlagTest, TestEnableFlag) {
  for (int i = 0; i < 10; ++i) {
    EnablePerfFlag(i);
    ASSERT_EQ(CheckPerfFlag(i), true);
  }
}
TEST(PerfFlagTest, TestDisableFlag) {
  for (int i = 0; i < 10; ++i) {
    EnablePerfFlag(i);
  }
  for (int i = 0; i < 10; ++i) {
    DisablePerfFlag(i);
    ASSERT_EQ(CheckPerfFlag(i), false);
  }
}

#define DB_TEST_HELPER(ENV, VALIDATION)                                  \
  {                                                                      \
    std::unique_ptr<Env> env{NewMemEnv(Env::Default())};                 \
    Options options;                                                     \
    options.create_if_missing = true;                                    \
    options.env = env.get();                                             \
    DB* db;                                                              \
    ENV const Slice keys[] = {Slice("aaa"), Slice("bbb"), Slice("ccc")}; \
    const Slice vals[] = {Slice("foo"), Slice("bar"), Slice("baz")};     \
    ASSERT_OK(DB::Open(options, "/dir/db", &db));                        \
    for (size_t i = 0; i < 3; ++i) {                                     \
      ASSERT_OK(db->Put(WriteOptions(), keys[i], vals[i]));              \
    }                                                                    \
    VALIDATION                                                           \
    std::cout << "current_perf_context:\n\t"                             \
              << get_perf_context()->ToString(true) << std::endl;        \
  }

TEST(PerfFlagTest, TestEnableFlagStandAlone) {
  DB_TEST_HELPER(
      {
        SetPerfLevel(PerfLevel::kDisable);
        EnablePerfFlag(flag_user_key_comparison_count);
      },
      { ASSERT_GT(get_perf_context()->user_key_comparison_count, 0); });
}

TEST(PerfFlagTest, TestPerfLevelNonoverlappingPerfFlag) {
  DB_TEST_HELPER(
      {
        SetPerfLevel(PerfLevel::kEnableCount);
        EnablePerfFlag(flag_write_wal_time);
      },
      { ASSERT_GT(get_perf_context()->write_wal_time, 0); });
}

TEST(PerfFlagTest, TestPerfLevelOverLappingPerfFlag) {
  DB_TEST_HELPER(
      {
        SetPerfLevel(PerfLevel::kEnableTime);
        EnablePerfFlag(flag_write_wal_time);
      },
      { ASSERT_GT(get_perf_context()->write_wal_time, 0); });
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}