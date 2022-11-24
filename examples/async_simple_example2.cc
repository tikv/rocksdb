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
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::PinnableSlice;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteOptions;
using Submit_queue = Async_future::Submit_queue;

std::string kDBPath = "/tmp/rocksdb/storage";

using Submit_queue = rocksdb::Async_future::Submit_queue;

struct Async_read {
  Async_read(DB *db, size_t io_uring_size);

  ~Async_read() noexcept {
    io_uring_queue_exit(m_io_uring.get());
  }

  void io_uring_completion() noexcept;

  Async_future get(ReadOptions &ropts, const std::string &k, std::string &value);

  void set_read_options(ReadOptions &ropts) noexcept {
    ropts.verify_checksums = true;
    ropts.submit_queue = m_submit_queue;
    ropts.read_tier = ReadTier::kPersistedTier;
  }

  private:
  using Promise = Async_future::promise_type;

  static void schedule_task(Promise* promise) {
    auto h{std::coroutine_handle<Promise>::from_promise(*promise)};
    h.resume();
  }

private:
  using ReadTier = ROCKSDB_NAMESPACE::ReadTier;
  using PinnableSlice = ROCKSDB_NAMESPACE::PinnableSlice;

  DB *m_db{};
  std::atomic<int> m_n_pending_sqe{};
  std::unique_ptr<io_uring> m_io_uring{};
  std::shared_ptr<Submit_queue> m_submit_queue{};
};

Async_read::Async_read(DB* db, size_t io_uring_size) 
        : m_db(db),
          m_io_uring(new io_uring) {
  auto ret = io_uring_queue_init(io_uring_size, m_io_uring.get(), 0);

  if (ret < 0) {
    throw "io_uring_queue_init failed";
  }

  m_submit_queue = std::make_shared<Submit_queue>(
    [this](Async_future::IO_ctx *ctx, int fd, off_t off, Submit_queue::Ops op) -> Async_future {
       using Status = ROCKSDB_NAMESPACE::Status;
       using SubCode = Status::SubCode;
       using IOStatus = ROCKSDB_NAMESPACE::IOStatus;

      assert(op == Submit_queue::Ops::Read);

      for (auto &iov : ctx->m_iov) {
        std::cout << "SUBMIT: " << iov.iov_len << "\n";
      }

      auto io_uring{m_io_uring.get()};
      auto sqe = io_uring_get_sqe(io_uring);

      if (sqe == nullptr) {
        co_return IOStatus::IOError(SubCode::kIOUringSqeFull);
      } else {
        auto &iov{ctx->m_iov};

        io_uring_prep_readv(sqe, fd, iov.data(), iov.size(), off);
        io_uring_sqe_set_data(sqe, ctx);

        const auto ret = io_uring_submit(io_uring);

        if (ret < 0) {
          // FIXME: Error handling.
          auto msg{strerror(-ret)};
          co_return IOStatus::IOError(SubCode::kIOUringSubmitError, msg);
        } else {
   	  m_n_pending_sqe.fetch_add(1, std::memory_order_seq_cst);

          std::cout << "SQE n: " << m_n_pending_sqe << ", "
                    << " fd: " << fd << ", off: " << off << ", promise: " << ctx->m_promise
                    << ", ctx: " << ctx << std::endl;

          Async_future r(true, ctx);
          co_await r;
          co_return IOStatus::OK();
        }
      }
    });
}

void Async_read::io_uring_completion() noexcept {
  auto io_uring{m_io_uring.get()};

  do {
    io_uring_cqe* cqe{};
    const auto ret = io_uring_wait_cqe(io_uring, &cqe);

    std::cout << "CQE: " << cqe << "\n";

    // FIXME: Error handling, short reads etc.
    if (ret == 0 && cqe->res >= 0) {
      auto ctx = reinterpret_cast<Async_future::IO_ctx*>(io_uring_cqe_get_data(cqe));

      io_uring_cqe_seen(io_uring, cqe);

      std::cout << "CQE: " << cqe << ", ret: " << ret << ", res: " << cqe->res
	        << ", " << m_n_pending_sqe << ", ctx: " << ctx << "\n";

      auto promise = ctx->m_promise;

      delete ctx;

      if (promise != nullptr) {
        schedule_task(promise);
      }
    } else {
      assert(false);
    }

  } while (m_n_pending_sqe.fetch_sub(1, std::memory_order_seq_cst) > 1);
 
  std::cout << "io_uring completion exit\n";
}

Async_future Async_read::get(ReadOptions &ropts, const std::string &k, std::string &v) {
  auto pinnable = new (std::nothrow) PinnableSlice();
  assert(pinnable != nullptr);

  auto result = m_db->AsyncGet(ropts, m_db->DefaultColumnFamily(), k, pinnable, nullptr);

  std::cout << __FILE__ << ":" << __LINE__ << "\n";

  co_await result;

  v = pinnable->ToString();

  delete pinnable;

  std::cout << __FILE__ << ":" << __LINE__ << result.status().code() << "\n";

  co_return result.status();;
}

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

  s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  std::string v1{};
  std::string v2{};
  std::string v3{};
  ReadOptions ropts;
  Async_read async_read{db, 2};

  async_read.set_read_options(ropts);

  auto r1 = async_read.get(ropts, k1, v1);
  auto r2 = async_read.get(ropts, k2, v2);
  auto r3 = async_read.get(ropts, k3, v3);

  async_read.io_uring_completion();

  std::cout << "found: ["
     << v1 << "], [" << v2 << "]" << ", [" << v3
     << "] r3.code: " << (int) r3.status().code() << std::endl;

  delete db;

  return EXIT_SUCCESS;;
}
