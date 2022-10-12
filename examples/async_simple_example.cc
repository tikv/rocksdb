// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <cstdio>
#include <string>
#include <coroutine>
#include <liburing.h>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Async_future;
using ROCKSDB_NAMESPACE::ReadTier;
using ROCKSDB_NAMESPACE::FilePage;
using ROCKSDB_NAMESPACE::IOUringOptions;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::PinnableSlice;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;

std::string kDBPath = "/tmp/rocksdb_async_simple_example";

class Async {
 public:
  explicit Async(DB *db)
      : m_db(db),
        m_io_uring{new io_uring},
        m_shutdown{} {
    std::cout << "Async" << std::endl;

    auto ret = io_uring_queue_init(m_io_uring_size, m_io_uring.get(), 0);

    if (ret < 0) {
        throw "io_uring_queue_init failed";
    }

    m_io_uring_options = std::make_unique<IOUringOptions>(
      [this](FilePage* data, int fd, uint64_t off, IOUringOptions::Ops op) -> Async_future {
        (void)op;

        Async_future a_result(true, data);

        auto sqe = io_uring_get_sqe(m_io_uring.get());

        if (sqe == nullptr) {
            /* Submission queue is full */
            co_return rocksdb::IOStatus::IOError(rocksdb::Status::SubCode::kIOUringSqeFull);
        }

        io_uring_prep_readv(sqe, fd, data->iov, data->pages_, off);

        io_uring_sqe_set_data(sqe, data);

        const auto ret = io_uring_submit(m_io_uring.get());

        if (ret < 0) {
          co_return rocksdb::IOStatus::IOError(rocksdb::Status::SubCode::kIOUringSubmitError, strerror(-ret));
        }

        std::cout << __LINE__ << " io_uring co_await" << std::endl;
        std::cout.flush();

        co_await a_result;

        std::cout << __LINE__ << " io_uring after co_await" << std::endl;
        std::cout.flush();

        co_return rocksdb::IOStatus::OK();
    });

    m_options.verify_checksums = true;
    m_options.read_tier = ReadTier::kPersistedTier;
    m_options.io_uring_option = m_io_uring_options.get();
  }

  ~Async() {
    std::cout << "~Async" << std::endl;

    m_shutdown.store(true, std::memory_order_relaxed);
    io_uring_queue_exit(m_io_uring.get());
  }

  io_uring* get_io_uring() {
    return this->m_io_uring.get();
  }

  void set_shutdown() {
    m_shutdown.fetch_sub(1, std::memory_order_seq_cst);
  }

  void io_uring_completion() {
    do {
      std::cout << __LINE__ << " shutdown: " << m_shutdown.load() << std::endl;

      io_uring_cqe* cqe;
      const auto ret = io_uring_wait_cqe(m_io_uring.get(), &cqe);

      if (ret == 0 && cqe->res >= 0) {
        auto data = reinterpret_cast<FilePage*>(io_uring_cqe_get_data(cqe));

        on_resume(data->promise);

        io_uring_cqe_seen(m_io_uring.get(), cqe);
      }
   } while (m_shutdown.load(std::memory_order_relaxed) > 0);
  }

  Async_future get(const std::string &k, std::string &value) {
    m_shutdown.fetch_add(1, std::memory_order_seq_cst);

    auto v = new (std::nothrow) PinnableSlice();
    assert(v != nullptr);

    std::cout << __LINE__ << " - AsyncGet " << k << std::endl;

    auto result = m_db->AsyncGet(m_options, m_db->DefaultColumnFamily(), k, v, nullptr);

    std::cout << __LINE__ << " - co_await " << k << std::endl;

    co_await result;

    std::cout << __LINE__ << " - after co_await " << k << std::endl;

    set_shutdown();

    value = v->ToString();

    delete v;

    std::cout << __LINE__ << " co_return - " << k << " -> " << value << std::endl;

    co_return true;
  }

 private:
  using Promise = Async_future::promise_type;

  static void on_resume(Promise* promise) {
    auto h{std::coroutine_handle<Promise>::from_promise(*promise)};

    h.resume();
  }

 private:
  const int m_io_uring_size = 4;

  DB *m_db{};
  ReadOptions m_options;
  std::atomic<int> m_shutdown{};
  std::unique_ptr<io_uring> m_io_uring;
  std::unique_ptr<IOUringOptions> m_io_uring_options{};
};


int main() {
  DB* db;

  Options options;

  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();

  options.create_if_missing = true;

  auto s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  const std::string k1{"k1"};
  const std::string k2{"k2"};
  const std::string k3{"k3"};

  s = db->Put(WriteOptions(), k1, "v1");
  assert(s.ok());

  s = db->Put(WriteOptions(), k2, "v2");
  assert(s.ok());

  delete db;

  std::cout << "Open for real ... " << std::endl;

  s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  {
    Async async{db};
    std::string v1{};
    std::string v2{};
    std::string v3{};

    auto r1 = async.get(k1, v1);
    auto r2 = async.get(k2, v2);
    auto r3 = async.get(k1, v3);

    async.io_uring_completion();

    std::cout << "found: ["
      << v1 << "], [" << v2 << "]" << ", [" << v3
      << "]" << std::endl;
  }

  delete db;

  return EXIT_SUCCESS;;
}
