#pragma once

#include <petuum_ps/include/abstract_row.hpp>
#include <petuum_ps/include/row_access.hpp>
#include <petuum_ps/include/configs.hpp>
#include <petuum_ps/storage/abstract_process_storage.hpp>
#include <petuum_ps/util/vector_clock_mt.hpp>
#include <petuum_ps/consistency/abstract_consistency_controller.hpp>
#include <petuum_ps/client/abstract_client_table.hpp>
#include <petuum_ps/thread/append_only_row_oplog_buffer.hpp>

#include <petuum_ps/oplog/abstract_oplog.hpp>
#include <petuum_ps/oplog/oplog_index.hpp>
#include <petuum_ps/client/thread_table.hpp>

#include <boost/thread/tss.hpp>

namespace petuum {

class ClientTable : public AbstractClientTable {

public:
  // Instantiate AbstractRow, TableOpLog, and ProcessStorage using config.
  ClientTable(int32_t table_id, const ClientTableConfig &config);

  ~ClientTable() override;

  void RegisterThread() override;

  void DeregisterThread();

  void GetAsyncForced(int32_t row_id) override;

  void GetAsync(int32_t row_id) override;

  void WaitPendingAsyncGet() override;

  void FlushThreadCache() override;

  ClientRow *Get(int32_t row_id, RowAccessor *row_accessor,
                 int32_t clock) override;

  void Inc(int32_t row_id, int32_t column_id, const void *update) override;

  void BatchInc(int32_t row_id, const int32_t *column_ids, const void *updates,
                int32_t num_updates, int32_t global_version = -1) override;

  void DenseBatchInc(int32_t row_id, const void *updates, int32_t index_st,
                     int32_t num_updates) override;

  void Clock() override;

  cuckoohash_map<int32_t, bool> *GetAndResetOpLogIndex(int32_t partition_num);

  size_t GetNumRowOpLogs(int32_t partition_num);

  AbstractProcessStorage &get_process_storage() { return *process_storage_; }

  AbstractOpLog &get_oplog() { return *oplog_; }

  const AbstractRow *get_sample_row() const { return sample_row_; }

  int32_t get_row_type() const override {
    return client_table_config_.table_info.row_type;
  }

  int32_t get_staleness() const {
    return client_table_config_.table_info.table_staleness;
  }

  bool oplog_dense_serialized() const {
    return client_table_config_.table_info.oplog_dense_serialized;
  }

  OpLogType get_oplog_type() const { return client_table_config_.oplog_type; }

  int32_t get_bg_apply_append_oplog_freq() const {
    return client_table_config_.bg_apply_append_oplog_freq;
  }

  int32_t get_row_oplog_type() const {
    return client_table_config_.table_info.row_oplog_type;
  }

  size_t get_dense_row_oplog_capacity() const {
    return client_table_config_.table_info.dense_row_oplog_capacity;
  }

  AppendOnlyOpLogType get_append_only_oplog_type() const {
    return client_table_config_.append_only_oplog_type;
  }

  bool get_no_oplog_replay() const {
    return client_table_config_.no_oplog_replay;
  }

private:
  const int32_t table_id_;

  const AbstractRow *const sample_row_;

  AbstractOpLog *oplog_;

  TableOpLogIndex oplog_index_;

  AbstractProcessStorage *process_storage_;

  AbstractConsistencyController *consistency_controller_;

  boost::thread_specific_ptr<ThreadTable> thread_cache_;

  ClientTableConfig client_table_config_;

  ClientRow *CreateClientRow(int32_t clock);

  ClientRow *CreateSSPClientRow(int32_t clock);
};

} // namespace petuum
