// author: Jinliang
#include <petuum_ps/consistency/ssp_consistency_controller.hpp>
#include <petuum_ps/thread/context.hpp>
#include <petuum_ps/thread/bg_workers.hpp>
#include <petuum_ps/util/stats.hpp>
#include <glog/logging.h>
#include <algorithm>

namespace petuum {

  SSPConsistencyController::SSPConsistencyController(const TableInfo& info,
                                                     int32_t table_id,
                                                     AbstractProcessStorage& process_storage,
                                                     AbstractOpLog& oplog,
                                                     const AbstractRow* sample_row,
                                                     boost::thread_specific_ptr<ThreadTable> &thread_cache,
                                                     TableOpLogIndex &oplog_index,
                                                     int32_t row_oplog_type) :

    AbstractConsistencyController(table_id, process_storage, sample_row),
    staleness_(info.table_staleness),
    thread_cache_(thread_cache),
    oplog_index_(oplog_index),
    oplog_(oplog) {

    VLOG(2) << "SSP controller for table=" << table_id << " has staleness=" << staleness_;
    AddUpdates_ = std::bind(&AbstractRow::AddUpdates,
                            sample_row_, std::placeholders::_1,
                            std::placeholders::_2,
                            std::placeholders::_3);

    if (row_oplog_type == RowOpLogType::kDenseRowOpLog) {
      DenseBatchIncOpLog_ = &SSPConsistencyController::DenseBatchIncDenseOpLog;
    } else {
      DenseBatchIncOpLog_ = &SSPConsistencyController::DenseBatchIncNonDenseOpLog;
    }
  }


  void SSPConsistencyController::GetAsync(int32_t row_id) {
    BgWorkers::RequestRowAsync(table_id_, row_id, ThreadContext::get_clock(), false);
  }


  void SSPConsistencyController::WaitPendingAsnycGet() {
    BgWorkers::GetAsyncRowRequestReply();
  }


  ClientRow *SSPConsistencyController::Get(int32_t row_id,
                                           RowAccessor* row_accessor,
                                           int32_t clock) {

    STATS_APP_SAMPLE_SSP_GET_BEGIN(table_id_);

    // Look for row_id in process_storage_.
    int32_t stalest_clock = clock > 0 ? clock : 0;

    ClientRow *client_row = process_storage_.Find(row_id, row_accessor);

    if (client_row != 0) {
      // Found it! Check staleness.
      int32_t clock = client_row->GetClock();
      if (clock >= stalest_clock) {
        STATS_APP_SAMPLE_SSP_GET_END(table_id_, true);
        return client_row;
      }
    }
    VLOG(20) << "Issue Request Row row_id=" << row_id << " for table=" << this->table_id_;

    // Didn't find row_id that's fresh enough in process_storage_.
    // Fetch from server.
    int32_t num_fetches = 0;
    do {
      STATS_APP_ACCUM_SSP_GET_SERVER_FETCH_BEGIN(table_id_);
      BgWorkers::RequestRow(table_id_, row_id, stalest_clock);
      STATS_APP_ACCUM_SSP_GET_SERVER_FETCH_END(table_id_);

      // fetch again
      client_row = process_storage_.Find(row_id, row_accessor);
      // TODO (jinliang):
      // It's possible that the application thread does not find the row that
      // the bg thread has just inserted. In practice, this shouldn't be an issue.
      // We'll fix it if it turns out there are too many misses.
      ++num_fetches;
      CHECK_LE(num_fetches, 3); // to prevent infinite loop
    } while(client_row == 0);
    VLOG(20) << "Received row. row_id=" << row_id << " for table=" << this->table_id_;

    CHECK_GE(client_row->GetClock(), stalest_clock);
    STATS_APP_SAMPLE_SSP_GET_END(table_id_, false);

    return client_row;
  } // end function -- Get



  void SSPConsistencyController::Inc(int32_t row_id,
                                     int32_t column_id,
                                     const void* delta) {

    thread_cache_->IndexUpdate(row_id);

    OpLogAccessor oplog_accessor;
    oplog_.FindInsertOpLog(row_id, &oplog_accessor);

    void *oplog_delta = oplog_accessor.get_row_oplog()->FindCreate(column_id);
    sample_row_->AddUpdates(column_id, oplog_delta, delta);

    RowAccessor row_accessor;
    ClientRow *client_row = process_storage_.Find(row_id, &row_accessor);
    if (client_row != 0) {
      client_row->GetRowDataPtr()->ApplyInc(column_id, delta);
    }
  } // end function -- Inc



  void SSPConsistencyController::BatchInc(int32_t row_id,
                                          const int32_t* column_ids,
                                          const void* updates,
                                          int32_t num_updates,
                                          int32_t global_version) {

    STATS_APP_SAMPLE_BATCH_INC_OPLOG_BEGIN();

    // update the thread index saying that row id is updated
    thread_cache_->IndexUpdate(row_id);

    // (raajay) create and insert an oplog. The oplog now holds the values that
    // are sent in updates. If an oplog for the same row is already present,
    // then the values in the OpLog are updated. By using an oplog_accessor, the
    // current execution gains a lock on the row oplog the lock is release at
    // the end of this function when the oplog_accessor variable is destroyed.

    OpLogAccessor oplog_accessor;
    oplog_.FindInsertOpLog(row_id, &oplog_accessor);

    // set the version from which the oplog is calculated
    oplog_accessor.get_row_oplog()->SetGlobalVersion(global_version);

    // update the data entries in the oplog
    const uint8_t* deltas_uint8 = reinterpret_cast<const uint8_t*>(updates);
    for (int i = 0; i < num_updates; ++i) {
      void *oplog_delta = oplog_accessor.get_row_oplog()->FindCreate(column_ids[i]);
      sample_row_->AddUpdates(column_ids[i],
                              oplog_delta,
                              deltas_uint8 + sample_row_->get_update_size()*i);
    }
    STATS_APP_SAMPLE_BATCH_INC_OPLOG_END();


    // TODO (Raajay) these updates are also synced into the process_storage_.
    // This enables other app threads to read it when needed. Process storage
    // has the local view of the Table so updating it makes sense. However, is
    // the version count incremented? If not, how do we avoid double counting?
    // the double counting is avoided by using version numbers. Every time as
    // worker sends an update a version number is incremented. On getting an
    // update from the server, we also get the information on all the versions
    // that have been applied. Then, using local cache of oplogs, we add all the
    // version numbers that have not been synced in the parameter server to
    // local process storage.

    // -- REMOVE syncing updates to process storage. Process storage is updates
    // -- only through updates from the server.

    /*
    STATS_APP_SAMPLE_BATCH_INC_PROCESS_STORAGE_BEGIN();
    RowAccessor row_accessor;
    ClientRow *client_row = process_storage_.Find(row_id, &row_accessor);
    if (client_row != 0) {
      // Apply batch inc grabs a lock before updating data
      client_row->GetRowDataPtr()->ApplyBatchInc(column_ids,
                                                 updates,
                                                 num_updates);
    }
    STATS_APP_SAMPLE_BATCH_INC_PROCESS_STORAGE_END();
    */

  } // end function -- BatchInc



  void SSPConsistencyController::DenseBatchInc(int32_t row_id,
                                               const void *updates,
                                               int32_t index_st,
                                               int32_t num_updates) {

    STATS_APP_SAMPLE_BATCH_INC_OPLOG_BEGIN();
    thread_cache_->IndexUpdate(row_id);

    OpLogAccessor oplog_accessor;
    bool new_create = oplog_.FindInsertOpLog(row_id, &oplog_accessor);

    if (new_create) {
      oplog_accessor.get_row_oplog()->OverwriteWithDenseUpdate(updates, index_st, num_updates);
    } else {
      const uint8_t* deltas_uint8 = reinterpret_cast<const uint8_t*>(updates);
      (this->*DenseBatchIncOpLog_)(&oplog_accessor, deltas_uint8,
                                   index_st, num_updates);
    }
    STATS_APP_SAMPLE_BATCH_INC_OPLOG_END();

    STATS_APP_SAMPLE_BATCH_INC_PROCESS_STORAGE_BEGIN();
    RowAccessor row_accessor;
    ClientRow *client_row = process_storage_.Find(row_id, &row_accessor);
    if (client_row != 0) {
      client_row->GetRowDataPtr()->ApplyDenseBatchInc(updates, index_st, num_updates);
    }
    STATS_APP_SAMPLE_BATCH_INC_PROCESS_STORAGE_END();
  } // end function -- DenseBatchInc



  void SSPConsistencyController::DenseBatchIncDenseOpLog(OpLogAccessor *oplog_accessor,
                                                         const uint8_t *updates,
                                                         int32_t index_st,
                                                         int32_t num_updates) {

    size_t update_size = sample_row_->get_update_size();
    uint8_t *oplog_delta = reinterpret_cast<uint8_t*>(
                                                      oplog_accessor->get_row_oplog()->FindCreate(index_st));
    for (int i = 0; i < num_updates; ++i) {
      int32_t col_id = i + index_st;
      AddUpdates_(col_id, oplog_delta, updates + update_size*i);
      oplog_delta += update_size;
    }
  } // end function -- DenseBatchIncDenseOpLog



  void SSPConsistencyController::DenseBatchIncNonDenseOpLog(OpLogAccessor *oplog_accessor,
                                                            const uint8_t *updates,
                                                            int32_t index_st,
                                                            int32_t num_updates) {

    size_t update_size = sample_row_->get_update_size();
    for (int i = 0; i < num_updates; ++i) {
      int32_t col_id = i + index_st;
      void *oplog_delta
        = oplog_accessor->get_row_oplog()->FindCreate(col_id);
      sample_row_->AddUpdates(col_id, oplog_delta, updates + update_size*i);
    }
  } // end function -- DenseBatchIncNonDenseOpLog


  void SSPConsistencyController::FlushThreadCache() {
    thread_cache_->FlushCache(process_storage_,
                              oplog_,
                              sample_row_);
  } // end function -- FlushThreadCache


  void SSPConsistencyController::Clock() {
    // order is important
    thread_cache_->FlushCache(process_storage_,
                              oplog_,
                              sample_row_);
    thread_cache_->FlushOpLogIndex(oplog_index_);
  } // end function -- Clock

}   // namespace petuum
