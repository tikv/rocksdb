#include "rocksdb/async_future.h"

namespace ROCKSDB_NAMESPACE {

void Async_future::await_suspend(
   std::coroutine_handle<Async_future::promise_type> h) {
  if (!async_) 
    h_.promise().prev_ = &h.promise();
  else
    context_->promise = &h.promise();
}

}  // namespace ROCKSDB_NAMESPACE
