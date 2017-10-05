#include <petuum_ps/aggregator/aggregator.hpp>
#include <petuum_ps/server/serialized_oplog_reader.hpp>
#include <petuum_ps/util/class_register.hpp>

#include <utility>
#include <fstream>
#include <map>

namespace petuum {

  Aggregator::Aggregator() {}

  Aggregator::~Aggregator() {}

  void Aggregator::Init(int32_t aggregator_id, const std::vector<int32_t> &bg_ids) {
    aggregator_id_ = aggregator_id;
    async_version_ = -1;
    accum_oplog_count_ = 0;
    curr_destination_server_id_ = -1;
    for(auto bg_id : bg_ids) {
      bg_ids_.push_back(bg_id);
    }
  } // end function -- Init


  void Aggregator::CreateTable(int32_t table_id, TableInfo &table_info) {
    auto ret = tables_.emplace(table_id, ServerTable(table_info));
    CHECK(ret.second);
  }

  ServerRow* Aggregator::FindCreateRow(int32_t table_id, int32_t row_id) {
    auto iter = tables_.find(table_id);
    CHECK(iter != tables_.end());
    ServerTable &server_table = iter->second;
    ServerRow *server_row = server_table.FindRow(row_id);
    if(server_row != 0) {
      return server_row;
    }
    server_row = server_table.CreateRow(row_id);
    return server_row;
  }


  // (raajay) This is a key function, one where the internal tables are updated
  // with values sent from the client. It is important to note that the client
  // message will contain updates to all the tables.
  void Aggregator::ApplyOpLogUpdateVersion(const void *oplog,
                                           size_t oplog_size,
                                           int32_t bg_thread_id,
                                           uint32_t version,
                                           int32_t *observed_delay) {

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

      // fix the delay as the max seen across all tables and rows
      *observed_delay = std::max(async_version_ - update_model_version, *observed_delay);

      // Apply or Create and apply the row op log. This will basically increment
      // the values at the server.
      bool found = server_table->ApplyRowOpLog(row_id, column_ids, updates, num_updates);

      if (!found) {
        server_table->CreateRow(row_id);
        server_table->ApplyRowOpLog(row_id, column_ids, updates, num_updates);
      }

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

  } // end function -- apply op log update


  int32_t Aggregator::GetAsyncModelVersion() {
    return async_version_;
  }

} // end namespace -- petuum
