#pragma once

#include <boost/noncopyable.hpp>
#include <queue>

#include <petuum_ps/include/configs.hpp>
#include <petuum_ps/util/stats.hpp>
#include <petuum_ps/oplog/create_row_oplog.hpp>
#include <petuum_ps/thread/context.hpp>

namespace petuum {

class RowOpLogRecycle : boost::noncopyable {
public:
  RowOpLogRecycle(int32_t row_oplog_type, const AbstractRow *sample_row,
                  size_t update_size, size_t dense_row_oplog_capacity)
      : sample_row_(sample_row), update_size_(update_size),
        dense_row_oplog_capacity_(dense_row_oplog_capacity) {
    if (row_oplog_type == RowOpLogType::kDenseRowOpLog)
      CreateRowOpLog_ = CreateRowOpLog::CreateDenseRowOpLog;
    else if (row_oplog_type == RowOpLogType::kSparseRowOpLog)
      CreateRowOpLog_ = CreateRowOpLog::CreateSparseRowOpLog;
    else
      CreateRowOpLog_ = CreateRowOpLog::CreateSparseVectorRowOpLog;
  }

  ~RowOpLogRecycle() {
    while (!row_oplog_pool_.empty()) {
      AbstractRowOpLog *row_oplog = row_oplog_pool_.front();
      delete row_oplog;
      row_oplog_pool_.pop();
    }
  }

  AbstractRowOpLog *GetRowOpLog() {
    if (row_oplog_pool_.empty()) {
      return CreateRowOpLog_(update_size_, sample_row_,
                             dense_row_oplog_capacity_);
    }

    AbstractRowOpLog *row_oplog = row_oplog_pool_.front();
    row_oplog_pool_.pop();
    return row_oplog;
  }

  void PutBackRowOpLog(AbstractRowOpLog *row_oplog) {
    row_oplog->Reset();
    row_oplog_pool_.push(row_oplog);
  }

private:
  const AbstractRow *sample_row_;
  const size_t update_size_;
  const size_t dense_row_oplog_capacity_;
  CreateRowOpLog::CreateRowOpLogFunc CreateRowOpLog_;
  std::queue<AbstractRowOpLog *> row_oplog_pool_;
};
}
