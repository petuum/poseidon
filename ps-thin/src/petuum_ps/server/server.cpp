// author: jinliang

#include <petuum_ps/server/server.hpp>
#include <petuum_ps/server/serialized_oplog_reader.hpp>
#include <petuum_ps/util/class_register.hpp>
#include <petuum_ps/util/utils.hpp>

#include <utility>
#include <fstream>
#include <map>

namespace petuum {

Server::Server() {}

Server::~Server() {}

/**
 * Each server thread has its own copy of Server object.
 */
void Server::Init(int32_t server_id, const std::vector<int32_t> &bg_ids,
                  bool is_replica) {
  // bg_clock is vector clock. We set it to be zero for all bg threads (one
  // from each worker client)
  for (auto bg : bg_ids) {
    bg_clock_.AddClock(bg, 0);
    bg_version_map_[bg] = -1;
    // TODO(raajay) is there an easier way to copy vectors?
    bg_ids_.push_back(bg);
  }
  server_id_ = server_id;
  accum_oplog_count_ = 0;
  is_replica_ = is_replica;
  from_start_timer_.restart();
}

/**
 */
void Server::CreateTable(int32_t table_id, TableInfo &table_info) {
  // Each Server object is responsible of different parts of all tables. Thus,
  // a copy of all tables is maintained.
  auto ret = tables_.emplace(table_id, ServerTable(table_info));
  CHECK(ret.second);

  // Add vector clock for each table
  auto ret1 = table_vector_clock_.emplace(table_id, VectorClock());
  CHECK(ret1.second);
  if (GlobalContext::get_resume_clock() > 0) {
    TableIter table_iter = tables_.find(table_id);
    table_iter->second.ReadSnapShot(GlobalContext::get_resume_dir(), server_id_,
                                    table_id,
                                    GlobalContext::get_resume_clock());
  }

  // Initialize the vector clocks
  TableClockIter iter = table_vector_clock_.find(table_id);
  for (auto bg : bg_ids_) {
    iter->second.AddClock(bg, std::max(GlobalContext::get_resume_clock(), 0));
  }
}

/**
 * Find a row indexed by table and row id. If no row exists, create a new row
 * and return.
 */
ServerRow *Server::FindCreateRow(int32_t table_id, int32_t row_id) {
  // access ServerTable via reference to avoid copying
  auto iter = tables_.find(table_id);
  CHECK(iter != tables_.end());
  return iter->second.FindCreateRow(row_id);
}

/**
 * Push the vector clock for a particular bg_id to the specified value. On
 * pushing the clock of a single bg_thread, if the min_value of th vector
 * clock changes, then return true. Also, see if we have to take a snapshot.
 */
bool Server::ClockUntil(int32_t bg_id, int32_t clock) {
  int new_clock = bg_clock_.TickUntil(bg_id, clock);
  if (new_clock) {
    if (GlobalContext::get_snapshot_clock() <= 0 ||
        new_clock % GlobalContext::get_snapshot_clock() != 0) {
      return true;
    }
    TakeSnapShot(new_clock);
    return true;
  }
  return false;
}

/**
 * Update an internal data structure to cache all row requests. One row
 * requests are cached, they are replied to only when the clock moves on an
 * update.
 */
void Server::AddRowRequest(int32_t bg_id, int32_t table_id, int32_t row_id,
                           int32_t clock) {

  ServerRowRequest server_row_request;
  server_row_request.bg_id = bg_id;
  server_row_request.table_id = table_id;
  server_row_request.row_id = row_id;
  server_row_request.clock = clock;

  if (clock_bg_row_requests_.count(clock) == 0) {
    clock_bg_row_requests_.insert(
        std::make_pair(clock, boost::unordered::unordered_map<
                                  int32_t, std::vector<ServerRowRequest>>()));
  }
  if (clock_bg_row_requests_[clock].count(bg_id) == 0) {
    clock_bg_row_requests_[clock].insert(
        std::make_pair(bg_id, std::vector<ServerRowRequest>()));
  }
  clock_bg_row_requests_[clock][bg_id].push_back(server_row_request);
}

/**
 * Look at the cache of row request, and return those that are satisfied upon
 * the clock moving.
 */
void Server::GetFulfilledRowRequests(std::vector<ServerRowRequest> *requests) {

  int32_t clock = bg_clock_.get_min_clock();
  requests->clear();
  auto iter = clock_bg_row_requests_.find(clock);

  if (iter == clock_bg_row_requests_.end())
    return;

  boost::unordered::unordered_map<int32_t, std::vector<ServerRowRequest>> &
      bg_row_requests = iter->second;

  for (auto bg_iter = bg_row_requests.begin(); bg_iter != bg_row_requests.end();
       bg_iter++) {
    requests->insert(requests->end(), bg_iter->second.begin(),
                     bg_iter->second.end());
  }

  clock_bg_row_requests_.erase(clock);
}

/**
 * (raajay) This is a key function, one where the internal tables are updated
 * with values sent from the client. It is important to note that the client
 * message will contain updates to all the tables.
 */
void Server::ApplyOpLogUpdateVersion(const void *oplog, size_t oplog_size,
                                     int32_t bg_thread_id, uint32_t version,
                                     int32_t *observed_delay) {
  if (!is_replica_) {
    CHECK_EQ(bg_version_map_[bg_thread_id] + 1, version);
    bg_version_map_[bg_thread_id] = version;
  }

  if (0 == oplog_size) { return; }

  SerializedOpLogReader oplog_reader(oplog, tables_);
  if (false == oplog_reader.Restart()) {
      return;
  }

  *observed_delay = -1; // init the observed delay

  int32_t table_id;
  int32_t row_id;
  int32_t model_version_for_update;
  const int32_t *column_ids; // the variable pointer points to const memory
  int32_t num_updates;
  bool started_new_table;

  // read the first few bytes of the message. It will populate the arguments.
  const void *updates =
      oplog_reader.Next(&table_id, &row_id, &model_version_for_update,
                        &column_ids, &num_updates, &started_new_table);
  CHECK_EQ(started_new_table, false);

  ServerTable *server_table;
  if (updates != 0) {
    server_table = GetServerTable(table_id);
  }

  while (updates != 0) {
    ++accum_oplog_count_;

    // TODO (raajay) use delayed based scaling.
    // 1. We have to decide if the scaling has to be determined per-row or on a
    // table basis.

    server_table->FindCreateRow(row_id);
    bool success = server_table->ApplyRowOpLog(row_id, column_ids, updates,
                                               num_updates, 1.0);
    CHECK_EQ(success, true) << "Row not found. "
                            << GetTableRowStringId(table_id, row_id);

    // get the next row update
    updates = oplog_reader.Next(&table_id, &row_id, &model_version_for_update,
                                &column_ids, &num_updates, &started_new_table);

    if (updates == 0) {
      break;
    }

    if (started_new_table) {
      server_table = GetServerTable(table_id);
    }
  }
  CHECK_EQ(oplog_reader.GetCurrentOffset(), oplog_size);
  VLOG(2) << "server_id=" << server_id_ << " sender_id=" << bg_thread_id
          << ", time=" << GetElapsedTime() << ", size=" << oplog_size;
}

/**
 */
int32_t Server::GetMinClock() { return bg_clock_.get_min_clock(); }

/**
 */
int32_t Server::GetBgVersion(int32_t bg_thread_id) {
  return bg_version_map_[bg_thread_id];
}

/**
 */
double Server::GetElapsedTime() { return from_start_timer_.elapsed(); }

/**
 */
ServerTable *Server::GetServerTable(int32_t table_id) {
  auto table_iter = tables_.find(table_id);
  CHECK(table_iter != tables_.end()) << "Not found table_id = " << table_id;
  return &(table_iter->second);
}

/**
 */
void Server::TakeSnapShot(int32_t current_clock) {
  for (auto table_iter = tables_.begin(); table_iter != tables_.end();
       table_iter++) {
    table_iter->second.TakeSnapShot(GlobalContext::get_snapshot_dir(),
                                    server_id_, table_iter->first,
                                    current_clock);
  }
}

/**
 */
bool Server::ClockTableUntil(int32_t table_id, int32_t bg_id, int32_t clock) {
  TableClockIter iter = table_vector_clock_.find(table_id);
  return (0 != iter->second.TickUntil(bg_id, clock));
}

/**
 */
int32_t Server::GetTableMinClock(int32_t table_id) {
  TableClockIter iter = table_vector_clock_.find(table_id);
  return iter->second.get_min_clock();
}
}
