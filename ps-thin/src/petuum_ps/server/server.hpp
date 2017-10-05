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
// 3. Manage the vector clock for clients;

class Server {
private:
  VectorClock bg_clock_;

  boost::unordered::unordered_map<int32_t, VectorClock> table_vector_clock_;
  typedef boost::unordered::unordered_map<int32_t, VectorClock>::iterator
      TableClockIter;

  boost::unordered::unordered_map<int32_t, ServerTable> tables_;
  typedef boost::unordered::unordered_map<int32_t, ServerTable>::iterator
      TableIter;

  std::vector<int32_t> bg_ids_;

  // mapping <clock, table id> to an array of row requests
  // Keeping track of all pending row requests. These are indexed first by the
  // clock requested by each row and secondly by the table id.
  std::map<int32_t, boost::unordered::unordered_map<
                        int32_t, std::vector<ServerRowRequest>>>
      clock_bg_row_requests_;
  // Keep track of the latest oplog version received from each bg thread
  std::map<int32_t, int32_t> bg_version_map_;

  int32_t server_id_;

  size_t accum_oplog_count_;

  HighResolutionTimer from_start_timer_;

  bool is_replica_;

  ServerTable *GetServerTable(int32_t table_id);

  void TakeSnapShot(int32_t current_clock);

public:
  Server();

  ~Server();

  void Init(int32_t server_id, const std::vector<int32_t> &bg_ids,
            bool is_replica = false);

  void CreateTable(int32_t table_id, TableInfo &table_info);

  ServerRow *FindCreateRow(int32_t table_id, int32_t row_id);

  void AddRowRequest(int32_t bg_id, int32_t table_id, int32_t row_id,
                     int32_t clock);

  void GetFulfilledRowRequests(std::vector<ServerRowRequest> *requests);

  void ApplyOpLogUpdateVersion(const void *oplog, size_t oplog_size,
                               int32_t bg_thread_id, uint32_t version,
                               int32_t *observed_delay);

  bool ClockUntil(int32_t bg_id, int32_t clock);

  int32_t GetMinClock();

  bool ClockTableUntil(int32_t table_id, int32_t bg_id, int32_t clock);

  int32_t GetTableMinClock(int32_t table_id);

  int32_t GetBgVersion(int32_t bg_thread_id);

  double GetElapsedTime();
};
}
