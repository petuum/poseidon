#include <petuum_ps/thread/ssp_bg_worker.hpp>

#include <petuum_ps/thread/ps_msgs.hpp>
#include <petuum_ps/util/class_register.hpp>
#include <petuum_ps/client/oplog_serializer.hpp>
#include <petuum_ps/client/ssp_client_row.hpp>
#include <petuum_ps/util/stats.hpp>
#include <petuum_ps/comm_bus/comm_bus.hpp>
#include <petuum_ps/thread/mem_transfer.hpp>
#include <petuum_ps/thread/context.hpp>
#include <glog/logging.h>
#include <utility>
#include <limits.h>
#include <algorithm>

namespace petuum {


  SSPBgWorker::SSPBgWorker(int32_t id,
                           int32_t comm_channel_idx,
                           std::map<int32_t, ClientTable* > *tables,
                           pthread_barrier_t *init_barrier,
                           pthread_barrier_t *create_table_barrier):
    AbstractBgWorker(id, comm_channel_idx,
                     tables, init_barrier,
                     create_table_barrier) { }



  SSPBgWorker::~SSPBgWorker() {
    delete row_request_oplog_mgr_;
  }



  void SSPBgWorker::CreateRowRequestOpLogMgr() {
    row_request_oplog_mgr_ = new SSPRowRequestOpLogMgr;
    VLOG(5) << "Create an OpLogMgr in BgWorker with (id=" << my_id_
            << ", comm_channel_idx=" << my_comm_channel_idx_ << ")";
  }



  bool SSPBgWorker::GetRowOpLog(AbstractOpLog &table_oplog,
                                int32_t row_id,
                                AbstractRowOpLog **row_oplog_ptr) {
    return table_oplog.GetEraseOpLog(row_id, row_oplog_ptr);
  }



  void SSPBgWorker::PrepareBeforeInfiniteLoop() { }


  void SSPBgWorker::FinalizeTableStats() { }


  long SSPBgWorker::ResetBgIdleMilli() {
    return 0;
  }

  long SSPBgWorker::BgIdleWork() {
    return 0;
  }


  ClientRow *SSPBgWorker::CreateClientRow(int32_t clock, int32_t global_version, AbstractRow *row_data) {
    return reinterpret_cast<ClientRow*>(new SSPClientRow(clock, global_version, row_data, true));
  }


  BgOpLog *SSPBgWorker::PrepareOpLogsToSend() {
    BgOpLog *bg_oplog = new BgOpLog;

    // Prepare oplogs prepares each table separately.
    for (const auto &table_pair : (*tables_)) {
      int32_t table_id = table_pair.first;

      // why are we creating a oplog partition? This is the most common case.
      // Answer: BgOpLog contains all the oplogs that the current worker is
      // responsible for; it includes oplogs across all tables which are
      // partitioned by the table id.

      // stores op logs responsible for the current workers filtered by the current table id
      BgOpLogPartition *bg_table_oplog = 0;

      if (table_pair.second->get_oplog_type() == Sparse || table_pair.second->get_oplog_type() == Dense) {
        bg_table_oplog = PrepareOpLogsNormal(table_id, table_pair.second);
      }
      else if (table_pair.second->get_oplog_type() == AppendOnly) {
        bg_table_oplog = PrepareOpLogsAppendOnly(table_id, table_pair.second);
      }
      else {
        LOG(FATAL) << "Unknown oplog type = " << table_pair.second->get_oplog_type();
      }

      // we add each tables oplog to the overall oplog
      bg_oplog->Add(table_id, bg_table_oplog);

      FinalizeOpLogMsgStats(table_id,
                            &table_num_bytes_by_server_,
                            &server_table_oplog_size_map_);

    } // end for loop over tables

    VLOG(5) << "Total number of rows modified = " << bg_oplog->num_rows();
    return bg_oplog;

  } // end function -- prepare op logs to send


  BgOpLogPartition *SSPBgWorker::PrepareOpLogsNormal(int32_t table_id,
                                                     ClientTable *table) {

    AbstractOpLog &table_oplog = table->get_oplog();

    GetSerializedRowOpLogSizeFunc GetSerializedRowOpLogSize;

    if (table->oplog_dense_serialized()) {
      GetSerializedRowOpLogSize = GetDenseSerializedRowOpLogSize;
    } else {
      GetSerializedRowOpLogSize = GetSparseSerializedRowOpLogSize;
    }

    // Get OpLog index. The index will tell which rows have been modified. So
    // the function below, will query an oplog index -- maintained per table at
    // the process level -- and get all the rows that have been modified. Note
    // that it will find modified rows from among the rows for each table that
    // the current bg_thread is responsible for.

    cuckoohash_map<int32_t, bool> *new_table_oplog_index_ptr = table->GetAndResetOpLogIndex(my_comm_channel_idx_);

    size_t table_update_size = table->get_sample_row()->get_update_size();

    BgOpLogPartition *bg_table_oplog = new BgOpLogPartition(table_id,
                                                            table_update_size,
                                                            my_comm_channel_idx_);

    for (const auto &server_id : server_ids_) {
      // Reset size to 0
      table_num_bytes_by_server_[server_id] = 0;
    }

    // iterate over all rows that are potentially modified
    for (auto oplog_index_iter = new_table_oplog_index_ptr->cbegin(); !oplog_index_iter.is_end(); oplog_index_iter++) {

      int32_t row_id = oplog_index_iter->first;

      AbstractRowOpLog *row_oplog = 0;
      bool found = GetRowOpLog(table_oplog, row_id, &row_oplog);

      // if not found, row_id has not been modified in this table
      if (!found) {
        continue;
      }

      // if row_oplog is null, nothing more to do
      if (found && (row_oplog == 0)) {
        continue;
      }

      // this function
      // 1. updates the bytes per server dict,
      // 2. adds the oplog to bg_table_oplog, indexed by row_id
      CountRowOpLogToSend(row_id,
                          row_oplog,
                          &table_num_bytes_by_server_,
                          bg_table_oplog,
                          GetSerializedRowOpLogSize);

      // we update the  version used to compute the model
      VLOG(20) << "model_version_prepared_:" << model_version_prepared_;
      model_version_prepared_ = std::min(model_version_prepared_, row_oplog->GetGlobalVersion());
      VLOG(20) << "model_version_prepared_ (new):" << model_version_prepared_;

    } // end for -- over all rows that have oplog (i.e., those which are modified; obtained from oplog index)

    delete new_table_oplog_index_ptr; // no one else points to this struct, see earlier GetAndResetOpLogIndex function
    return bg_table_oplog;
  }



  BgOpLogPartition *SSPBgWorker::PrepareOpLogsAppendOnly(int32_t table_id,
                                                         ClientTable *table) {
    VLOG(2) << "In PrepareOpLogsAppendOnly";
    GetSerializedRowOpLogSizeFunc GetSerializedRowOpLogSize;

    if (table->oplog_dense_serialized()) {
      GetSerializedRowOpLogSize = GetDenseSerializedRowOpLogSize;
    } else {
      GetSerializedRowOpLogSize = GetSparseSerializedRowOpLogSize;
    }

    size_t table_update_size
      = table->get_sample_row()->get_update_size();
    BgOpLogPartition *bg_table_oplog = new BgOpLogPartition(table_id,
                                                            table_update_size,
                                                            my_comm_channel_idx_);

    for (const auto &server_id : server_ids_) {
      // Reset size to 0
      table_num_bytes_by_server_[server_id] = 0;
    }

    auto buff_iter = append_only_row_oplog_buffer_map_.find(table_id);
    if (buff_iter != append_only_row_oplog_buffer_map_.end()) {
      AppendOnlyRowOpLogBuffer *append_only_row_oplog_buffer = buff_iter->second;
      append_only_row_oplog_buffer->MergeTmpOpLog();

      int32_t row_id;
      AbstractRowOpLog *row_oplog
        = append_only_row_oplog_buffer->InitReadRmOpLog(&row_id);
      while (row_oplog != 0) {
        CountRowOpLogToSend(row_id, row_oplog, &table_num_bytes_by_server_,
                            bg_table_oplog, GetSerializedRowOpLogSize);

        row_oplog = append_only_row_oplog_buffer->NextReadRmOpLog(&row_id);
      }
    }
    return bg_table_oplog;
  }




  void SSPBgWorker::TrackBgOpLog(BgOpLog *bg_oplog) {
    bool tracked = row_request_oplog_mgr_->AddOpLog(version_, bg_oplog);
    ++version_;
    VLOG(5) << "Increment version of bgworker:" << my_id_ << " to " << version_;

    // the below function does nothing.
    row_request_oplog_mgr_->InformVersionInc();

    if (!tracked) {
      delete bg_oplog;
    }
  }

} // end namespace -- petuum
