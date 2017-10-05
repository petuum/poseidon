// author: jinliang

#include <petuum_ps/server/server.hpp>
#include <petuum_ps/server/serialized_oplog_reader.hpp>
#include <petuum_ps/util/class_register.hpp>

#include <utility>
#include <fstream>
#include <map>

namespace petuum {

  Server::Server() {}

  Server::~Server() {}

  // Each server thread has its own copy of Server object.
  void Server::Init(int32_t server_id,
                    const std::vector<int32_t> &bg_ids,
                    bool is_replica) {
    // bg_clock is vector clock. We set it to be zero for all bg threads (one
    // from each worker client)
    for (auto iter = bg_ids.cbegin(); iter != bg_ids.cend(); iter++){
      bg_clock_.AddClock(*iter, 0); // the clock is 0 initially
      bg_version_map_[*iter] = -1; // the version -1 initially
    }

    // we start with async version -1, it will be incremented to 0, when we init the parameters.
    async_version_ = -1;

    push_row_msg_data_size_ = kPushRowMsgSizeInit;
    server_id_ = server_id;
    accum_oplog_count_ = 0;
    is_replica_ = is_replica;
    from_start_timer_.restart();

  } // end function - Init



  void Server::CreateTable(int32_t table_id, TableInfo &table_info) {

    // Each Server object is responsible of different parts of all tables. Thus,
    // a copy of all tables is maintained.

    auto ret = tables_.emplace(table_id, ServerTable(table_info));
    CHECK(ret.second);

    if (GlobalContext::get_resume_clock() > 0) {
      boost::unordered_map<int32_t, ServerTable>::iterator table_iter = tables_.find(table_id);
      table_iter->second.ReadSnapShot(GlobalContext::get_resume_dir(),
                                      server_id_, table_id,
                                      GlobalContext::get_resume_clock());
    }

  } // end function -- Create table



  // Find a row indexed by table and row id. If no row exists, create a new row and return.
  ServerRow *Server::FindCreateRow(int32_t table_id, int32_t row_id) {
    // access ServerTable via reference to avoid copying
    auto iter = tables_.find(table_id);
    CHECK(iter != tables_.end());
    ServerTable &server_table = iter->second;
    ServerRow *server_row = server_table.FindRow(row_id);
    if(server_row != 0) {
      return server_row;
    }
    server_row = server_table.CreateRow(row_id);
    return server_row;
  } // end function -- Find create row



  // Push the vector clock for a particular bg_id to the specified value. On
  // pushing the clock of a single bg_thread, if the min_value of th vector
  // clock changes, then return true. Also, see if we have to take a snapshot.

  bool Server::ClockUntil(int32_t bg_id, int32_t clock) {
    int new_clock = bg_clock_.TickUntil(bg_id, clock);
    if(new_clock) {

      // take snapshot of all tables if configured to. That is all is happening
      // here. Always returns true.
      if (GlobalContext::get_snapshot_clock() <= 0 || new_clock % GlobalContext::get_snapshot_clock() != 0) {
        return true;
      }

      for (auto table_iter = tables_.begin(); table_iter != tables_.end(); table_iter++) {
        table_iter->second.TakeSnapShot(GlobalContext::get_snapshot_dir(),
                                        server_id_,
                                        table_iter->first,
                                        new_clock);
      }
      return true;
    }

    return false;
  } // end function - clock until



  // Update an internal data structure to cache all row requests. One row requests are cached, they
  // are replied to only when the clock moves on an update.
  void Server::AddRowRequest(int32_t bg_id,
                             int32_t table_id,
                             int32_t row_id,
                             int32_t clock) {

    ServerRowRequest server_row_request;
    server_row_request.bg_id = bg_id;
    server_row_request.table_id = table_id;
    server_row_request.row_id = row_id;
    server_row_request.clock = clock;

    if (clock_bg_row_requests_.count(clock) == 0) {
      clock_bg_row_requests_.insert(std::make_pair(clock,
                                                   boost::unordered_map<int32_t, std::vector<ServerRowRequest> >()));
    }
    if (clock_bg_row_requests_[clock].count(bg_id) == 0) {
      clock_bg_row_requests_[clock].insert(std::make_pair(bg_id,
                                                          std::vector<ServerRowRequest>()));
    }
    clock_bg_row_requests_[clock][bg_id].push_back(server_row_request);

  } // end function -- Add row request



  // Look at the cache of row request, and return those that are satisfied upon the clock moving.
  void Server::GetFulfilledRowRequests(std::vector<ServerRowRequest> *requests) {

    int32_t clock = bg_clock_.get_min_clock();
    requests->clear();
    auto iter = clock_bg_row_requests_.find(clock);

    if(iter == clock_bg_row_requests_.end())
      return;

    boost::unordered_map<int32_t, std::vector<ServerRowRequest> > &bg_row_requests = iter->second;

    for (auto bg_iter = bg_row_requests.begin(); bg_iter != bg_row_requests.end(); bg_iter++) {
      requests->insert(requests->end(), bg_iter->second.begin(), bg_iter->second.end());
    }

    clock_bg_row_requests_.erase(clock);

  } // end function -- get full filled row requests



  // (raajay) This is a key function, one where the internal tables are updated
  // with values sent from the client. It is important to note that the client
  // message will contain updates to all the tables.
  void Server::ApplyOpLogUpdateVersion(const void *oplog,
                                       size_t oplog_size,
                                       int32_t bg_thread_id,
                                       uint32_t version,
                                       int32_t *observed_delay) {

    if(!is_replica_) {
      CHECK_EQ(bg_version_map_[bg_thread_id] + 1, version);
      // Update the version from a single bg thread that has been applied to the model.
      bg_version_map_[bg_thread_id] = version;
    }


    *observed_delay = -1;

    if (oplog_size == 0) {
      return;
    }

    SerializedOpLogReader oplog_reader(oplog, tables_);
    bool to_read = oplog_reader.Restart();

    if(!to_read) {
      return;
    }


    int32_t table_id;
    int32_t row_id;
    int32_t update_model_version;
    const int32_t * column_ids; // the variable pointer points to const memory
    int32_t num_updates;
    bool started_new_table;

    // read the first few bytes of the message. It will populate the arguments.
    const void *updates = oplog_reader.Next(&table_id,
                                            &row_id,
                                            &update_model_version,
                                            &column_ids,
                                            &num_updates,
                                            &started_new_table);

    ServerTable *server_table;
    if (updates != 0) {
      // find the pointer to the server table at the server.
      auto table_iter = tables_.find(table_id);
      CHECK(table_iter != tables_.end()) << "Not found table_id = " << table_id;
      server_table = &(table_iter->second);
    }

    while (updates != 0) {

      ++accum_oplog_count_;

      int current_update_delay = async_version_ - update_model_version;

      // fix the delay as the max seen across all tables and rows
      *observed_delay = std::max(current_update_delay, *observed_delay);
      int corrected_delay = std::max(1, current_update_delay);


      // Apply or Create and apply the row op log. This will basically increment
      // the values at the server.
      bool found = server_table->ApplyRowOpLog(row_id, column_ids, updates, num_updates, 1.0 / corrected_delay);

      if (!found) {
        server_table->CreateRow(row_id);
        server_table->ApplyRowOpLog(row_id, column_ids, updates, num_updates, 1.0 / corrected_delay);
      }

      // VLOG(20) << "updated row_id="  << row_id;

      // get the next row id worth of update
      updates = oplog_reader.Next(&table_id,
                                  &row_id,
                                  &update_model_version,
                                  &column_ids,
                                  &num_updates,
                                  &started_new_table);

      if (updates == 0) {
        break;
      }

      // if we have started a new table, change the table that is being updated internally
      if (started_new_table) {
        auto table_iter = tables_.find(table_id);
        CHECK(table_iter != tables_.end()) << "Not found table_id = " << table_id;
        server_table = &(table_iter->second);
      }

    } // end while -- as long as updates are available

    VLOG(2) << "Observed delay, sender_id=" << bg_thread_id
            << ", server_id=" << server_id_
            << ", server_version=" << async_version_
            << ", delay=" << *observed_delay
            << ", time=" << GetElapsedTime()
            << ", size=" << oplog_size;

    async_version_++; // finally, increment the global version of the model

  } // end function -- apply op log update



  int32_t Server::GetMinClock() {
    return bg_clock_.get_min_clock();
  }



  int32_t Server::GetBgVersion(int32_t bg_thread_id) {
    return bg_version_map_[bg_thread_id];
  }

  double Server::GetElapsedTime() {
    return from_start_timer_.elapsed();
  }

  int32_t Server::GetAsyncModelVersion() {
    return async_version_;
  }

}  // namespace petuum
