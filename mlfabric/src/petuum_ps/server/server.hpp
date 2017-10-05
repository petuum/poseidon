// author: jinliang

#pragma once

#include <vector>
#include <pthread.h>
#include <petuum_ps/util/pthread_barrier.hpp>
#include <boost/unordered_map.hpp>
#include <petuum_ps/include/table.hpp>
#include <petuum_ps/include/configs.hpp>
#include <petuum_ps/include/abstract_row.hpp>
#include <petuum_ps/include/constants.hpp>
#include <petuum_ps/util/vector_clock.hpp>
#include <petuum_ps/server/server_table.hpp>
#include <petuum_ps/thread/ps_msgs.hpp>
#include <petuum_ps/util/high_resolution_timer.hpp>

namespace petuum {

  struct ServerRowRequest {
  public:
    int32_t bg_id; // requesting bg thread id
    int32_t table_id;
    int32_t row_id;
    int32_t clock;
  };

  // 1. Manage the table storage on server;
  // 2. Manage the pending reads;
  // 3. Manage the vector clock for clients
  // 4. (TODO): manage OpLogs that need acknowledgements

  class Server {
  public:
    Server();
    ~Server();

    void Init(int32_t server_id, const std::vector<int32_t> &bg_ids, bool is_replica=false);

    void CreateTable(int32_t table_id, TableInfo &table_info);

    ServerRow *FindCreateRow(int32_t table_id, int32_t row_id);

    bool ClockUntil(int32_t bg_id, int32_t clock);

    void AddRowRequest(int32_t bg_id, int32_t table_id, int32_t row_id, int32_t clock);

    void GetFulfilledRowRequests(std::vector<ServerRowRequest> *requests);

    void ApplyOpLogUpdateVersion(const void *oplog,
                                 size_t oplog_size,
                                 int32_t bg_thread_id, uint32_t version, int32_t *observed_delay);

    // Accessors
    int32_t GetMinClock();
    int32_t GetBgVersion(int32_t bg_thread_id);
    int32_t GetAsyncModelVersion();
    double GetElapsedTime();

  private:
    VectorClock bg_clock_;
    int32_t async_version_; // (raajay) we add this to maintain async version number
    boost::unordered_map<int32_t, ServerTable> tables_;
    // mapping <clock, table id> to an array of row requests
    std::map<int32_t, boost::unordered_map<int32_t, std::vector<ServerRowRequest> > > clock_bg_row_requests_;
    // latest oplog version that I have received from a bg thread
    std::map<int32_t, uint32_t> bg_version_map_;
    // Assume a single row does not exceed this size!
    static const size_t kPushRowMsgSizeInit = 4*k1_Mi;
    size_t push_row_msg_data_size_;
    int32_t server_id_;
    size_t accum_oplog_count_;
    bool is_replica_;
    HighResolutionTimer from_start_timer_;

  }; // end class -- Server

}  // namespace petuum
