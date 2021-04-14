#include "monitoring/perf_flags_imp.h"

#define PERF_FLAGS_INIT_LEVEL2 \
.enable_perf_context_by_level_count_bit = 1,        \
.enable_user_key_comparison_count_bit = 1,          \
.enable_block_cache_hit_count_bit = 1,              \
.enable_block_read_count_bit = 1,                   \
.enable_block_read_byte_bit = 1,                    \
.enable_block_cache_index_hit_count_bit = 1,        \
.enable_index_block_read_count_bit = 1,             \
.enable_block_cache_filter_hit_count_bit = 1,       \
.enable_filter_block_read_count_bit = 1,            \
.enable_compression_dict_block_read_count_bit = 1,  \
.enable_get_read_bytes_bit = 1,                     \
.enable_multiget_read_bytes_bit = 1,                \
.enable_iter_read_bytes_bit = 1,                    \
.enable_internal_key_skipped_count_bit = 1,         \
.enable_internal_delete_skipped_count_bit = 1,      \
.enable_internal_recent_skipped_count_bit = 1,      \
.enable_internal_merge_count_bit = 1,               \
.enable_get_from_memtable_count_bit = 1,            \
.enable_seek_on_memtable_count_bit = 1,             \
.enable_next_on_memtable_count_bit = 1,             \
.enable_prev_on_memtable_count_bit = 1,             \
.enable_seek_child_seek_count_bit = 1,              \
.enable_bloom_memtable_hit_count_bit = 1,           \
.enable_bloom_memtable_miss_count_bit = 1,          \
.enable_bloom_sst_hit_count_bit = 1,                \
.enable_bloom_sst_miss_count_bit = 1,               \
.enable_key_lock_wait_count_bit = 1                

#define PERF_FLAGS_INIT_LEVEL3 \
.enable_measure_cpu_time_bit = 1,                           \
.enable_block_read_time_bit = 1,                            \
.enable_block_checksum_time_bit = 1,                        \
.enable_block_decompress_time_bit = 1,                      \
.enable_get_snapshot_time_bit = 1,                          \
.enable_get_from_memtable_time_bit = 1,                     \
.enable_get_post_process_time_bit = 1,                      \
.enable_get_from_output_files_time_bit = 1,                 \
.enable_seek_on_memtable_time_bit = 1,                      \
.enable_seek_child_seek_time_bit = 1,                       \
.enable_seek_min_heap_time_bit = 1,                         \
.enable_seek_max_heap_time_bit = 1,                         \
.enable_seek_internal_seek_time_bit = 1,                    \
.enable_find_next_user_entry_time_bit = 1,                  \
.enable_write_wal_time_bit = 1,                             \
.enable_write_memtable_time_bit = 1,                        \
.enable_write_delay_time_bit = 1,                           \
.enable_write_scheduling_flushes_compactions_time_bit = 1,  \
.enable_write_pre_and_post_process_time_bit = 1,            \
.enable_write_thread_wait_nanos_bit = 1,                    \
.enable_merge_operator_time_nanos_bit = 1,                  \
.enable_read_index_block_nanos_bit = 1,                     \
.enable_read_filter_block_nanos_bit = 1,                    \
.enable_new_table_block_iter_nanos_bit = 1,                 \
.enable_new_table_iterator_nanos_bit = 1,                   \
.enable_block_seek_nanos_bit = 1,                           \
.enable_find_table_nanos_bit = 1,                           \
.enable_key_lock_wait_time_bit = 1,                         \
.enable_env_new_sequential_file_nanos_bit = 1,              \
.enable_env_new_random_access_file_nanos_bit = 1,           \
.enable_env_new_writable_file_nanos_bit = 1,                \
.enable_env_reuse_writable_file_nanos_bit = 1,              \
.enable_env_new_random_rw_file_nanos_bit = 1,               \
.enable_env_new_directory_nanos_bit = 1,                    \
.enable_env_file_exists_nanos_bit = 1,                      \
.enable_env_get_children_nanos_bit = 1,                     \
.enable_env_get_children_file_attributes_nanos_bit = 1,     \
.enable_env_delete_file_nanos_bit = 1,                      \
.enable_env_create_dir_nanos_bit = 1,                       \
.enable_env_create_dir_if_missing_nanos_bit = 1,            \
.enable_env_delete_dir_nanos_bit = 1,                       \
.enable_env_get_file_size_nanos_bit = 1,                    \
.enable_env_get_file_modification_time_nanos_bit = 1,       \
.enable_env_rename_file_nanos_bit = 1,                      \
.enable_env_link_file_nanos_bit = 1,                        \
.enable_env_lock_file_nanos_bit = 1,                        \
.enable_env_unlock_file_nanos_bit = 1,                      \
.enable_env_new_logger_nanos_bit = 1,                       \
.enable_encrypt_data_nanos_bit = 1,                         \
.enable_decrypt_data_nanos_bit = 1

#define PERF_FLAGS_INIT_LEVEL4 \
.enable_iostats_cpu_timer_bit = 1,    \
.enable_get_cpu_nanos_bit = 1,        \
.enable_iter_next_cpu_nanos_bit = 1,  \
.enable_iter_prev_cpu_nanos_bit = 1,  \
.enable_iter_seek_cpu_nanos_bit = 1

#define PERF_FLAGS_INIT_LEVEL5 \
.enable_db_mutex_lock_nanos_bit = 1,      \
.enable_db_condition_wait_nanos_bit = 1 \

namespace rocksdb {

const PerfFlags PERF_LEVEL2 = {
    PERF_FLAGS_INIT_LEVEL2
};
const PerfFlags PERF_LEVEL3 = {
    PERF_FLAGS_INIT_LEVEL2,
    PERF_FLAGS_INIT_LEVEL3
};
const PerfFlags PERF_LEVEL4 = {
    PERF_FLAGS_INIT_LEVEL2,
    PERF_FLAGS_INIT_LEVEL3,
    PERF_FLAGS_INIT_LEVEL4
};
const PerfFlags PERF_LEVEL5 = {
    PERF_FLAGS_INIT_LEVEL2,
    PERF_FLAGS_INIT_LEVEL3,
    PERF_FLAGS_INIT_LEVEL4,
    PERF_FLAGS_INIT_LEVEL5
};

// set default value of perf_flags
#ifdef ROCKSDB_SUPPORT_THREAD_LOCAL
__thread PerfFlags perf_flags = {
    PERF_FLAGS_INIT_LEVEL2
};
#else
PerfFlags perf_flags = {
    PERF_FLAGS_INIT_LEVEL2
};
#endif

void SetPerfFlags(PerfFlags pbf) { perf_flags = pbf; }

PerfFlags GetPerfFlags() { return perf_flags; }

}  // namespace rocksdb
