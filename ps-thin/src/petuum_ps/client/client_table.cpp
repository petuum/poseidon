#include <glog/logging.h>
#include <petuum_ps/client/client_table.hpp>
#include <petuum_ps/util/class_register.hpp>
#include <petuum_ps/util/stats.hpp>
#include <petuum_ps/client/client_row.hpp>
#include <petuum_ps/storage/bounded_dense_process_storage.hpp>
#include <petuum_ps/storage/bounded_sparse_process_storage.hpp>
#include <petuum_ps/util/class_register.hpp>

#include <petuum_ps/client/ssp_client_row.hpp>
#include <petuum_ps/consistency/ssp_consistency_controller.hpp>
#include <petuum_ps/thread/context.hpp>

#include <petuum_ps/oplog/sparse_oplog.hpp>
#include <petuum_ps/oplog/dense_oplog.hpp>
#include <petuum_ps/oplog/append_only_oplog.hpp>

#include <cmath>

namespace petuum {

ClientTable::ClientTable(int32_t table_id, const ClientTableConfig &config)
    : AbstractClientTable(), table_id_(table_id),
      sample_row_(ClassRegistry<AbstractRow>::GetRegistry().CreateObject(
          config.table_info.row_type)),
      oplog_index_(
          std::ceil(static_cast<float>(config.oplog_capacity) /
                    GlobalContext::get_num_comm_channels_per_client())),
      client_table_config_(config) {

  switch (config.process_storage_type) {
  case BoundedDense: {
    BoundedDenseProcessStorage::CreateClientRowFunc StorageCreateClientRow;
    if (GlobalContext::get_consistency_model() == SSP) {
      StorageCreateClientRow = std::bind(&ClientTable::CreateSSPClientRow, this,
                                         std::placeholders::_1);
    } else {
      LOG(FATAL) << "Unknown consistency model "
                 << GlobalContext::get_consistency_model();
    }

    process_storage_ =
        static_cast<AbstractProcessStorage *>(new BoundedDenseProcessStorage(
            config.process_cache_capacity, StorageCreateClientRow, 0));
    VLOG(1) << "Process storage for table: " << table_id << " is created."
            << "Type = BoundedDense, Client Row Type = SSPClientRow";
  } break;
  case BoundedSparse: {
    process_storage_ =
        static_cast<AbstractProcessStorage *>(new BoundedSparseProcessStorage(
            config.process_cache_capacity,
            GlobalContext::GetLockPoolSize(config.process_cache_capacity)));
    VLOG(1) << "Process storage for table: " << table_id << " is created."
            << "Type = BoundedSparse, Client Row Type = Unspecified.";
  } break;
  default:
    LOG(FATAL) << "Unknown process storage type "
               << config.process_storage_type;
  } // end switch -- process storage type

  switch (config.oplog_type) {

  case Sparse:

    oplog_ = new SparseOpLog(config.oplog_capacity, sample_row_,
                             config.table_info.dense_row_oplog_capacity,
                             config.table_info.row_oplog_type);

    VLOG(1) << "A Sparse OpLog for table: " << table_id << " is created.";
    VLOG(1) << "Row OpLog types is: " << config.table_info.row_oplog_type;

    break;

  case AppendOnly:
    oplog_ = new AppendOnlyOpLog(config.append_only_buff_capacity, sample_row_,
                                 config.append_only_oplog_type,
                                 config.table_info.dense_row_oplog_capacity,
                                 config.per_thread_append_only_buff_pool_size);

    VLOG(1) << "An Append-only OpLog for table: " << table_id << " is created.";
    break;

  case Dense:
    oplog_ = new DenseOpLog(config.oplog_capacity, sample_row_,
                            config.table_info.dense_row_oplog_capacity,
                            config.table_info.row_oplog_type);

    VLOG(1) << "A Dense OpLog for table: " << table_id << " is created.";
    VLOG(1) << "Row OpLog types is: " << config.table_info.row_oplog_type;
    break;

  default:
    LOG(FATAL) << "Unknown oplog type = " << config.oplog_type;
  }

  // TODO(raajay)  what is the difference between dense and sparse OpLog?

  switch (GlobalContext::get_consistency_model()) {
  case SSP: {
    consistency_controller_ = new SSPConsistencyController(
        config.table_info, table_id, *process_storage_, *oplog_, sample_row_,
        thread_cache_, oplog_index_, config.table_info.row_oplog_type);

    VLOG(1) << "A SSP consistency controller for table: " << table_id
            << " is created.";
  } break;
  default:
    LOG(FATAL) << "Not yet support consistency model "
               << GlobalContext::get_consistency_model();
  }
}

/**
 * @brief Destructor
 */
ClientTable::~ClientTable() {
  delete consistency_controller_;
  delete sample_row_;
  delete oplog_;
  delete process_storage_;
}

// each individual thread is responsible for invoking this function to get
// access to the table.
void ClientTable::RegisterThread() {
  CHECK_EQ(thread_cache_.get() == nullptr, true);
  if (false == GlobalContext::use_table_clock()) {
    thread_cache_.reset(new ThreadTable(
        sample_row_, client_table_config_.table_info.row_oplog_type,
        client_table_config_.table_info.row_capacity));
    VLOG(0) << "Create an new thread table cache for thread: "
            << ThreadContext::get_id();
  }
  oplog_->RegisterThread();
}

void ClientTable::DeregisterThread() {
  thread_cache_.reset(nullptr);
  oplog_->DeregisterThread();
}

void ClientTable::GetAsyncForced(int32_t row_id) {
  consistency_controller_->GetAsyncForced(row_id);
}

void ClientTable::GetAsync(int32_t row_id) {
  consistency_controller_->GetAsync(row_id);
}

void ClientTable::WaitPendingAsyncGet() {
  consistency_controller_->WaitPendingAsnycGet();
}

ClientRow *ClientTable::Get(int32_t row_id, RowAccessor *row_accessor,
                            int32_t clock) {
  return consistency_controller_->Get(row_id, row_accessor, clock);
}

void ClientTable::FlushThreadCache() {
  consistency_controller_->FlushThreadCache();
}

void ClientTable::Inc(int32_t row_id, int32_t column_id, const void *update) {

  STATS_APP_SAMPLE_INC_BEGIN(table_id_);
  consistency_controller_->Inc(row_id, column_id, update);
  STATS_APP_SAMPLE_INC_END(table_id_);
}

void ClientTable::BatchInc(int32_t row_id, const int32_t *column_ids,
                           const void *updates, int32_t num_updates,
                           int32_t global_version) {

  STATS_APP_SAMPLE_BATCH_INC_BEGIN(table_id_);
  consistency_controller_->BatchInc(row_id, column_ids, updates, num_updates,
                                    global_version);
  STATS_APP_SAMPLE_BATCH_INC_END(table_id_);
}

void ClientTable::DenseBatchInc(int32_t row_id, const void *updates,
                                int32_t index_st, int32_t num_updates) {

  STATS_APP_SAMPLE_BATCH_INC_BEGIN(table_id_);
  consistency_controller_->DenseBatchInc(row_id, updates, index_st,
                                         num_updates);
  STATS_APP_SAMPLE_BATCH_INC_END(table_id_);
}

void ClientTable::Clock() {
  STATS_APP_SAMPLE_CLOCK_BEGIN(table_id_);
  consistency_controller_->Clock();
  STATS_APP_SAMPLE_CLOCK_END(table_id_);
}

cuckoohash_map<int32_t, bool> *
ClientTable::GetAndResetOpLogIndex(int32_t partition_num) {
  return oplog_index_.ResetPartition(partition_num);
}

size_t ClientTable::GetNumRowOpLogs(int32_t partition_num) {
  return oplog_index_.GetNumRowOpLogs(partition_num);
}

ClientRow *ClientTable::CreateClientRow(int32_t clock) {
  AbstractRow *row_data =
      ClassRegistry<AbstractRow>::GetRegistry().CreateObject(
          client_table_config_.table_info.row_type);
  row_data->Init((int32_t)client_table_config_.table_info.row_capacity);
  return new ClientRow(clock, -1, row_data, false);
}

ClientRow *ClientTable::CreateSSPClientRow(int32_t clock) {
  AbstractRow *row_data =
      ClassRegistry<AbstractRow>::GetRegistry().CreateObject(
          client_table_config_.table_info.row_type);
  row_data->Init((int32_t)client_table_config_.table_info.row_capacity);
  return static_cast<ClientRow *>(new SSPClientRow(clock, -1, row_data, false));
}

} // namespace petuum
