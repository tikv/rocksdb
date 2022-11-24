// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <liburing.h>
#include <sys/uio.h>

#include <coroutine>
#include <functional>
#include <iostream>
#include <memory>
#include <variant>
#include <vector>

#include "io_status.h"
#include "rocksdb/status.h"

struct RustFutureStatus;
struct RustFutureIOStatus;

namespace ROCKSDB_NAMESPACE {

struct [[nodiscard]] Async_future {
  struct promise_type;

  using Promise_type = promise_type;
  using Void_handle_type = std::coroutine_handle<>;
  using Typed_handle_type = std::coroutine_handle<Promise_type>;

  struct Continuation {
    promise_type* m_prev{};
  };

  struct promise_type {
    struct Return_type {
      /** Whether the result can be co_returned. */
      // FIXME: Use std::optional
      bool m_is_set{};

      /** Status or statues returned by async functions. */
      std::variant<bool, Status, IOStatus, std::vector<Status>> m_value{};
    };

    Async_future get_return_object() {
      auto h{Typed_handle_type::from_promise(*this)};

      // FIXME: This is very expensive
      assert(m_result == nullptr);
      m_result = new(std::nothrow) Return_type{};
      assert(m_result != nullptr);

      return Async_future(h, m_result);
    }

    auto initial_suspend() { return std::suspend_never{}; }

    auto final_suspend() noexcept {
      if (m_continuation.m_prev != nullptr) {
        auto h{Typed_handle_type::from_promise(*m_continuation.m_prev)};
        h.resume();
      }
      return std::suspend_never{};
    }

    void unhandled_exception() {
      std::abort();
    }

    template <typename T>
    void return_value(T v) {
      m_result->m_is_set = true;
      m_result->m_value = std::move(v);
    }

    Return_type* m_result{};
    Continuation m_continuation{};
  };

  struct IO_ctx {
    explicit IO_ctx(int n_pages) : m_iov(n_pages) {}

    ~IO_ctx() = default;

    promise_type* m_promise{};
    std::vector<iovec> m_iov{};
  };

  /** IOUring callback for Async API. */
  struct Submit_queue {
    using FH = int;
    using IO_ctx = Async_future::IO_ctx;

    enum class Ops { Read, Write };

    using FnMut = std::function<Async_future(IO_ctx*, FH, off_t, Ops)>;

    explicit Submit_queue(io_uring* iouring) : m_iouring{iouring} {
      assert(m_iouring != nullptr);
    }

    explicit Submit_queue(FnMut&& delegate)
          : m_delegate{std::forward<FnMut>(delegate)} {}

    FnMut m_delegate{};
    io_uring* m_iouring{};
  };

  Async_future() = default;
  Async_future(const Async_future&) = default;
  Async_future& operator=(Async_future&&) = delete;
  Async_future& operator=(const Async_future&) = delete;

  Async_future(Async_future&& rhs)
      : m_h(rhs.m_h),
        m_async(rhs.m_async),
        m_ctx(rhs.m_ctx),
        m_result(rhs.m_result) {
    rhs.m_ctx = nullptr;
    rhs.m_result = nullptr;
  }

  Async_future(bool async, IO_ctx* ctx)
      : m_async(async),
        m_ctx(ctx) { }

  Async_future(Typed_handle_type h, Promise_type::Return_type* result)
      : m_h(h),
        m_result(result) { }

  ~Async_future() {
    delete m_result;
    m_result = nullptr;
  }

  bool await_ready() const noexcept {
    if (m_async || m_result == nullptr) {
      return false;
    } else {
      return m_result->m_is_set;
    }
  }

  void await_suspend(Typed_handle_type h) {
    auto promise = &h.promise();

    if (!m_async) { 
      m_h.promise().m_continuation.m_prev = promise;
    } else {
      m_ctx->m_promise = promise;
    }
  }

  void await_suspend(Void_handle_type) {
    assert(false);
  }

  void await_resume() const noexcept {}

  template <typename T>
  auto value() const {
    return std::get<T>(m_result->m_value);
  }

  Status status() const {
    return value<Status>();
  }

  IOStatus io_result() const {
    return value<IOStatus>();
  }

  std::vector<Status> statuses() const {
     return value<std::vector<Status>>();
  }

  bool write_result() const {
    return value<bool>();
  }

private:
  Typed_handle_type m_h{};

  /* true if a custom io_uring handler is installed. */
  bool m_async{};

  /** IO context for read/write. */
  IO_ctx* m_ctx{};

  /** Result for the caller. */
  Promise_type::Return_type* m_result{};
};

}// namespace ROCKSDB_NAMESPACE

