// author: raajay

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

namespace petuum {

  // 1. Manage the table storage on aggregator;
  // 2. Manage the pending reads;
  // 3. Manage the vector clock for clients
  // 4. (TODO): manage OpLogs that need acknowledgements

  class Aggregator {
  public:
    Aggregator();
    ~Aggregator();

    void Init(int32_t server_id, const std::vector<int32_t> &bg_ids);
    void CreateTable(int32_t table_id, TableInfo &table_info);
    ServerRow* FindCreateRow(int32_t table_id, int32_t row_id);

    void ApplyOpLogUpdateVersion(const void *oplog,
                                 size_t oplog_size,
                                 int32_t bg_thread_id, uint32_t version, int32_t *observed_delay);

    int32_t GetAsyncModelVersion();

  private:
    int32_t aggregator_id_;
    int32_t async_version_; // (raajay) we add this to maintain async version number

    boost::unordered_map<int32_t, ServerTable> tables_;
    size_t accum_oplog_count_;
    int32_t curr_destination_server_id_;

    std::vector<int32_t> bg_ids_;
    std::vector<int32_t> server_ids_;


    // Assume a single row does not exceed this size!
    // static const size_t kPushRowMsgSizeInit = 4*k1_Mi;
    // size_t push_row_msg_data_size_;


  }; // end class -- Aggregator

}  // namespace petuum
