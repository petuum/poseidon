#include <petuum_ps/thread/bg_oplog_partition.hpp>
#include <petuum_ps/thread/context.hpp>

namespace petuum {

  BgOpLogPartition::BgOpLogPartition(int32_t table_id,
                                     size_t update_size,
                                     int32_t my_comm_channel_idx):

    table_id_(table_id),
    update_size_(update_size),
    comm_channel_idx_(my_comm_channel_idx){ }



  BgOpLogPartition::~BgOpLogPartition() {
    for (auto iter = oplog_map_.begin(); iter != oplog_map_.end(); iter++) {
      delete iter->second;
    }
  }



  AbstractRowOpLog *BgOpLogPartition::FindOpLog(int row_id) {
    auto oplog_iter = oplog_map_.find(row_id);
    if (oplog_iter != oplog_map_.end())
      return oplog_iter->second;
    return 0;
  }



  void BgOpLogPartition::InsertOpLog(int row_id, AbstractRowOpLog *row_oplog) {
    oplog_map_[row_id] = row_oplog;
  }



  void BgOpLogPartition::SerializeByServer(std::map<int32_t, void* > *bytes_by_server,
                                           bool dense_serialize) {

    AbstractRowOpLog::SerializeFunc SerializeOpLog;
    if (dense_serialize) {
      SerializeOpLog = &AbstractRowOpLog::SerializeDense;
    } else {
      SerializeOpLog = &AbstractRowOpLog::SerializeSparse;
    }

    std::map<int32_t, size_t> offset_by_server;

    for(auto iter = (*bytes_by_server).begin(); iter != (*bytes_by_server).end(); ++iter) {

      int32_t server_id = iter->first;
      // make offset as 1 to store the number of rows information
      offset_by_server[server_id] = sizeof(int32_t);
      // Init number of rows to 0 - the first element in table update to the server is the number of rows
      *((int32_t *) iter->second) = 0;

    } // end for - over the mapping from server to num bytes

    for (auto iter = oplog_map_.cbegin(); iter != oplog_map_.cend(); iter++) {

      int32_t row_id = iter->first;
      int32_t server_id = GlobalContext::GetPartitionServerID(row_id,
                                                              comm_channel_idx_);

      auto server_iter = (*bytes_by_server).find(server_id);
      CHECK(server_iter != (*bytes_by_server).end());

      AbstractRowOpLog *row_oplog_ptr = iter->second;

      // the location to write the oplog data for a single row.
      uint8_t *mem = ((uint8_t *) server_iter->second) + offset_by_server[server_id];

      // 1. write the row id in to Client Send Op log msg memory
      int32_t &mem_row_id = *((int32_t *) mem);
      mem_row_id = row_id;
      mem += sizeof(int32_t);

      // 2. write the global version of the row into memory
      int32_t &mem_global_version = *((int32_t *)  mem);
      mem_global_version = row_oplog_ptr->GetGlobalVersion();
      mem += sizeof(int32_t);

      // 3. write oplog data
      size_t serialized_size = (row_oplog_ptr->*SerializeOpLog)(mem);

      // increment offset by the total space taken for a single row:
      // 1. row id, 2. global version, 3. serialized size
      offset_by_server[server_id] += sizeof(int32_t) + sizeof(int32_t) + serialized_size;

      // increment number of rows by 1
      *((int32_t *) server_iter->second) += 1;

    } // end for -- over all the oplogs indexed by row id

  } // end function -- SerializeByServer

} // end namespace -- petuum
