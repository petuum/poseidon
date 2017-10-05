#pragma once

// author: jinliang

#include <petuum_ps/include/abstract_row.hpp>
#include <petuum_ps/client/client_row.hpp>

#include <cstdint>
#include <atomic>
#include <memory>
#include <boost/utility.hpp>
#include <mutex>
#include <glog/logging.h>

namespace petuum {

// ClientRow is a wrapper on user-defined ROW data structure (e.g., vector,
// map) with additional features:
//
// 1. Reference Counting: number of references used by application. Note the
// copy in storage itself does not contribute to the count
// 2. Row Metadata
//
// ClientRow does not provide thread-safety in itself. The locks are
// maintained in the storage and in (user-defined) ROW.
class SSPClientRow : public ClientRow {
public:
  // ClientRow takes ownership of row_data.
  SSPClientRow(int32_t clock, int32_t global_version, AbstractRow *row_data,
               bool use_ref_count)
      : ClientRow(clock, global_version, row_data, use_ref_count),
        clock_(clock), global_version_(global_version) {}

  void SetClock(int32_t clock) {
    std::unique_lock<std::mutex> ulock(clock_mtx_);
    clock_ = clock;
  }

  int32_t GetClock() const {
    std::unique_lock<std::mutex> ulock(clock_mtx_);
    return clock_;
  }

  void SetGlobalVersion(int32_t version) {
    std::unique_lock<std::mutex> ulock(version_mtx_);
    CHECK_GE(version, global_version_)
        << "New version is less than existing version of the row.";
    global_version_ = version;
  }

  int32_t GetGlobalVersion() const {
    std::unique_lock<std::mutex> ulock(version_mtx_);
    return global_version_;
  }

private: // private members
  mutable std::mutex clock_mtx_;
  mutable std::mutex version_mtx_;
  int32_t clock_;
  int32_t global_version_;
};

} // namespace petuum
