#include <petuum_ps/thread/abstract_bg_worker.hpp>

#include <petuum_ps/util/class_register.hpp>
#include <petuum_ps/client/oplog_serializer.hpp>
#include <petuum_ps/client/ssp_client_row.hpp>
#include <petuum_ps/thread/mem_transfer.hpp>
#include <petuum_ps/util/utils.hpp>

namespace petuum {

AbstractBgWorker::AbstractBgWorker(int32_t id, int32_t comm_channel_idx,
                                   std::map<int32_t, ClientTable *> *tables,
                                   pthread_barrier_t *init_barrier,
                                   pthread_barrier_t *create_table_barrier)
    : my_id_(id), my_comm_channel_idx_(comm_channel_idx), tables_(tables),
      per_worker_update_version_(0), worker_clock_(0), clock_has_pushed_(-1),
      comm_bus_(GlobalContext::comm_bus), init_barrier_(init_barrier),
      create_table_barrier_(create_table_barrier) {

  GlobalContext::GetServerThreadIDs(my_comm_channel_idx_, &(server_ids_));

}


AbstractBgWorker::~AbstractBgWorker() {
  for (auto &serializer_pair : row_oplog_serializer_map_) {
    delete serializer_pair.second;
  }
}

void AbstractBgWorker::ShutDown() { Join(); }

void AbstractBgWorker::AppThreadRegister() {
  AppConnectMsg app_connect_msg;
  void *msg = app_connect_msg.get_mem();
  size_t msg_size = app_connect_msg.get_size();

  comm_bus_->ConnectTo(my_id_, msg, msg_size);
}

void AbstractBgWorker::AppThreadDeregister() {
  AppThreadDeregMsg msg;
  size_t sent_size = SendMsg(reinterpret_cast<MsgBase *>(&msg));
  CHECK_EQ(sent_size, msg.get_size());
}

void AbstractBgWorker::SyncThreadRegister() {
  SyncThreadConnectMsg connect_msg;
  void *msg = connect_msg.get_mem();
  size_t msg_size = connect_msg.get_size();
  comm_bus_->ConnectTo(my_id_, msg, msg_size);
}

void AbstractBgWorker::SyncThreadDeregister() {
  SyncThreadDeregMsg msg;
  size_t sent_size = SendMsg(reinterpret_cast<MsgBase *>(&msg));
  CHECK_EQ(sent_size, msg.get_size());
}

bool AbstractBgWorker::CreateTable(int32_t table_id,
                                   const ClientTableConfig &table_config) {
  {
    const TableInfo &table_info = table_config.table_info;
    BgCreateTableMsg bg_create_table_msg;
    bg_create_table_msg.get_table_id() = table_id;
    bg_create_table_msg.get_staleness() = table_info.table_staleness;
    bg_create_table_msg.get_row_type() = table_info.row_type;
    bg_create_table_msg.get_row_capacity() = table_info.row_capacity;
    bg_create_table_msg.get_process_cache_capacity() =
        table_config.process_cache_capacity;
    bg_create_table_msg.get_thread_cache_capacity() =
        table_config.thread_cache_capacity;
    bg_create_table_msg.get_oplog_capacity() = table_config.oplog_capacity;

    bg_create_table_msg.get_oplog_dense_serialized() =
        table_info.oplog_dense_serialized;
    bg_create_table_msg.get_row_oplog_type() = table_info.row_oplog_type;
    bg_create_table_msg.get_dense_row_oplog_capacity() =
        table_info.dense_row_oplog_capacity;

    bg_create_table_msg.get_oplog_type() = table_config.oplog_type;
    bg_create_table_msg.get_append_only_oplog_type() =
        table_config.append_only_oplog_type;
    bg_create_table_msg.get_append_only_buff_capacity() =
        table_config.append_only_buff_capacity;
    bg_create_table_msg.get_per_thread_append_only_buff_pool_size() =
        table_config.per_thread_append_only_buff_pool_size;
    bg_create_table_msg.get_bg_apply_append_oplog_freq() =
        table_config.bg_apply_append_oplog_freq;
    bg_create_table_msg.get_process_storage_type() =
        table_config.process_storage_type;
    bg_create_table_msg.get_no_oplog_replay() = table_config.no_oplog_replay;

    size_t sent_size =
        SendMsg(reinterpret_cast<MsgBase *>(&bg_create_table_msg));
    CHECK_EQ((int32_t)sent_size, bg_create_table_msg.get_size());
    VLOG(5) << "THREAD-" << my_id_
            << ": Send CREATE_TABLE request from bgworker=" << this->my_id_
            << " for table=" << table_id;
  }
  // waiting for response
  {
    zmq::message_t zmq_msg;
    int32_t sender_id;
    comm_bus_->RecvInProc(&sender_id, &zmq_msg);
    MsgType msg_type = MsgBase::get_msg_type(zmq_msg.data());
    CHECK_EQ(msg_type, kCreateTableReply);
    VLOG(5) << "THREAD-" << my_id_
            << ": Received reply for CREATE_TABLE from sender=" << sender_id;
  }
  return true;
}

bool AbstractBgWorker::RequestRow(int32_t table_id, int32_t row_id,
                                  int32_t clock) {
  petuum::HighResolutionTimer rr_send;
  {
    RowRequestMsg request_row_msg;
    request_row_msg.get_table_id() = table_id;
    request_row_msg.get_row_id() = row_id;
    request_row_msg.get_clock() = clock;
    request_row_msg.get_forced_request() = false;
    VLOG(20) << "RR App Thread >>> Bg Thread (" << my_id_ << ") "
             << petuum::GetTableRowStringId(table_id, row_id)
             << " clock=" << clock;
    size_t sent_size = SendMsg(reinterpret_cast<MsgBase *>(&request_row_msg));
    CHECK_EQ(sent_size, request_row_msg.get_size());
  }

  {
    zmq::message_t zmq_msg;
    int32_t sender_id;
    comm_bus_->RecvInProc(&sender_id, &zmq_msg);
    VLOG(20) << "RR Latency@App Thread for "
             << petuum::GetTableRowStringId(table_id, row_id)
             << " clock=" << clock << " equals " << rr_send.elapsed() << " s";
    MsgType msg_type = MsgBase::get_msg_type(zmq_msg.data());
    CHECK_EQ(msg_type, kRowRequestReply);
  }
  return true;
}

void AbstractBgWorker::RequestRowAsync(int32_t table_id, int32_t row_id,
                                       int32_t clock, bool forced) {
  RowRequestMsg request_row_msg;
  request_row_msg.get_table_id() = table_id;
  request_row_msg.get_row_id() = row_id;
  request_row_msg.get_clock() = clock;
  request_row_msg.get_forced_request() = forced;

  VLOG(20) << "RR-Async App Thread >>> Bg Thread "
           << petuum::GetTableRowStringId(table_id, row_id)
           << " clock=" << clock;

  size_t sent_size = SendMsg(reinterpret_cast<MsgBase *>(&request_row_msg));
  CHECK_EQ(sent_size, request_row_msg.get_size());
}

void AbstractBgWorker::GetAsyncRowRequestReply() {
  zmq::message_t zmq_msg;
  int32_t sender_id;
  comm_bus_->RecvInProc(&sender_id, &zmq_msg);
  MsgType msg_type = MsgBase::get_msg_type(zmq_msg.data());
  CHECK_EQ(msg_type, kRowRequestReply);
}

void AbstractBgWorker::ClockAllTables() {
  BgClockMsg bg_clock_msg;
  size_t sent_size = SendMsg(reinterpret_cast<MsgBase *>(&bg_clock_msg));
  CHECK_EQ(sent_size, bg_clock_msg.get_size());
}

void AbstractBgWorker::ClockTable(int32_t table_id) {
  BgClockMsg msg;
  msg.get_table_id() = table_id;
  size_t sent_size = SendMsg(reinterpret_cast<MsgBase *>(&msg));
  CHECK_EQ(sent_size, msg.get_size());
}

void AbstractBgWorker::SendOpLogsAllTables() {
  BgSendOpLogMsg bg_send_oplog_msg;
  size_t sent_size = SendMsg(reinterpret_cast<MsgBase *>(&bg_send_oplog_msg));
  CHECK_EQ(sent_size, bg_send_oplog_msg.get_size());
}

void AbstractBgWorker::InitWhenStart() {
  SetWaitMsg();
  CreateRowRequestOpLogMgr();
}

bool AbstractBgWorker::WaitMsgBusy(int32_t *sender_id, zmq::message_t *zmq_msg,
                                   long timeout_milli __attribute__((unused))) {
  bool received =
      (GlobalContext::comm_bus->*(GlobalContext::comm_bus->RecvAsyncAny_))(
          sender_id, zmq_msg);
  while (!received)
    received =
        (GlobalContext::comm_bus->*(GlobalContext::comm_bus->RecvAsyncAny_))(
            sender_id, zmq_msg);
  return true;
}

bool AbstractBgWorker::WaitMsgSleep(int32_t *sender_id, zmq::message_t *zmq_msg,
                                    long timeout_milli
                                    __attribute__((unused))) {
  (GlobalContext::comm_bus->*(GlobalContext::comm_bus->RecvAny_))(sender_id,
                                                                  zmq_msg);
  return true;
}

bool AbstractBgWorker::WaitMsgTimeOut(int32_t *sender_id,
                                      zmq::message_t *zmq_msg,
                                      long timeout_milli) {
  bool received =
      (GlobalContext::comm_bus->*(GlobalContext::comm_bus->RecvTimeOutAny_))(
          sender_id, zmq_msg, timeout_milli);
  return received;
}

size_t
AbstractBgWorker::GetDenseSerializedRowOpLogSize(AbstractRowOpLog *row_oplog) {
  return row_oplog->GetDenseSerializedSize();
}

size_t
AbstractBgWorker::GetSparseSerializedRowOpLogSize(AbstractRowOpLog *row_oplog) {
  row_oplog->ClearZerosAndGetNoneZeroSize();
  return row_oplog->GetSparseSerializedSize();
}

void AbstractBgWorker::SetWaitMsg() {
  if (GlobalContext::get_aggressive_cpu()) {
    WaitMsg_ = WaitMsgBusy;
  } else {
    WaitMsg_ = WaitMsgSleep;
  }
}

void AbstractBgWorker::InitCommBus() {
  CommBus::Config comm_config;
  comm_config.entity_id_ = my_id_;
  comm_config.ltype_ = CommBus::kInProc;
  comm_bus_->ThreadRegister(comm_config);
}

void AbstractBgWorker::BgServerHandshake() {
  {
    // connect to name node
    int32_t name_node_id = GlobalContext::get_name_node_id();
    ConnectToNameNodeOrServer(name_node_id);

    // wait for ConnectServerMsg from NameNode
    zmq::message_t zmq_msg;
    int32_t sender_id;
    if (comm_bus_->IsLocalEntity(name_node_id)) {
      comm_bus_->RecvInProc(&sender_id, &zmq_msg);
    } else {
      comm_bus_->RecvInterProc(&sender_id, &zmq_msg);
    }
    MsgType msg_type = MsgBase::get_msg_type(zmq_msg.data());
    CHECK_EQ(sender_id, name_node_id);
    CHECK_EQ(msg_type, kConnectServer) << "sender_id = " << sender_id;
  }

  // connect to servers
  {
    for (const auto &server_id : server_ids_) {
      ConnectToNameNodeOrServer(server_id);
    }
  }

  // get messages from servers, namenode for permission to start
  {
    int32_t num_started_servers = 0;
    for (num_started_servers = 0;
         // receive from all servers and name node
         num_started_servers < GlobalContext::get_num_server_clients() + 1;
         ++num_started_servers) {
      zmq::message_t zmq_msg;
      int32_t sender_id;
      (comm_bus_->*(comm_bus_->RecvAny_))(&sender_id, &zmq_msg);
      MsgType msg_type = MsgBase::get_msg_type(zmq_msg.data());

      CHECK_EQ(msg_type, kClientStart);
      VLOG(5) << "THREAD-" << my_id_
              << ": Received client start from server:" << sender_id;
    }
  }

} // end function -- bg server handshake

void AbstractBgWorker::HandleCreateTables() {
  for (int32_t num_created_tables = 0;
       num_created_tables < GlobalContext::get_num_tables();
       ++num_created_tables) {

    int32_t table_id;
    int32_t sender_id;
    ClientTableConfig client_table_config;

    {
      zmq::message_t zmq_msg;
      comm_bus_->RecvInProc(&sender_id, &zmq_msg);
      MsgType msg_type = MsgBase::get_msg_type(zmq_msg.data());
      CHECK_EQ(msg_type, kBgCreateTable);
      BgCreateTableMsg bg_create_table_msg(zmq_msg.data());
      // set up client table config
      client_table_config.table_info.table_staleness =
          bg_create_table_msg.get_staleness();
      client_table_config.table_info.row_type =
          bg_create_table_msg.get_row_type();
      client_table_config.table_info.row_capacity =
          bg_create_table_msg.get_row_capacity();
      client_table_config.process_cache_capacity =
          bg_create_table_msg.get_process_cache_capacity();
      client_table_config.thread_cache_capacity =
          bg_create_table_msg.get_thread_cache_capacity();
      client_table_config.oplog_capacity =
          bg_create_table_msg.get_oplog_capacity();

      client_table_config.table_info.oplog_dense_serialized =
          bg_create_table_msg.get_oplog_dense_serialized();
      client_table_config.table_info.row_oplog_type =
          bg_create_table_msg.get_row_oplog_type();
      client_table_config.table_info.dense_row_oplog_capacity =
          bg_create_table_msg.get_dense_row_oplog_capacity();

      client_table_config.oplog_type = bg_create_table_msg.get_oplog_type();
      client_table_config.append_only_oplog_type =
          bg_create_table_msg.get_append_only_oplog_type();
      client_table_config.append_only_buff_capacity =
          bg_create_table_msg.get_append_only_buff_capacity();
      client_table_config.per_thread_append_only_buff_pool_size =
          bg_create_table_msg.get_per_thread_append_only_buff_pool_size();
      client_table_config.bg_apply_append_oplog_freq =
          bg_create_table_msg.get_bg_apply_append_oplog_freq();
      client_table_config.process_storage_type =
          bg_create_table_msg.get_process_storage_type();
      client_table_config.no_oplog_replay =
          bg_create_table_msg.get_no_oplog_replay();

      CreateTableMsg create_table_msg;
      create_table_msg.get_table_id() = bg_create_table_msg.get_table_id();
      create_table_msg.get_staleness() = bg_create_table_msg.get_staleness();
      create_table_msg.get_row_type() = bg_create_table_msg.get_row_type();
      create_table_msg.get_row_capacity() =
          bg_create_table_msg.get_row_capacity();
      create_table_msg.get_oplog_dense_serialized() =
          bg_create_table_msg.get_oplog_dense_serialized();
      create_table_msg.get_row_oplog_type() =
          bg_create_table_msg.get_row_oplog_type();
      create_table_msg.get_dense_row_oplog_capacity() =
          bg_create_table_msg.get_dense_row_oplog_capacity();

      table_id = create_table_msg.get_table_id();

      // send msg to name node
      int32_t name_node_id = GlobalContext::get_name_node_id();
      size_t sent_size = (comm_bus_->*(comm_bus_->SendAny_))(
          name_node_id, create_table_msg.get_mem(),
          create_table_msg.get_size());
      CHECK_EQ(sent_size, create_table_msg.get_size());
      VLOG(5) << "THREAD-" << my_id_
              << ": Send CREATE_TABLE request from bgworker=" << my_id_
              << " to namenode=" << name_node_id;
    }

    // wait for response from name node
    {
      zmq::message_t zmq_msg;
      int32_t name_node_id;
      (comm_bus_->*(comm_bus_->RecvAny_))(&name_node_id, &zmq_msg);
      MsgType msg_type = MsgBase::get_msg_type(zmq_msg.data());

      CHECK_EQ(msg_type, kCreateTableReply);
      CreateTableReplyMsg create_table_reply_msg(zmq_msg.data());
      CHECK_EQ(create_table_reply_msg.get_table_id(), table_id);

      ClientTable *client_table;
      try {
        VLOG(5) << "THREAD-" << my_id_
                << ": Creating an instance of a ClientTable(id=" << table_id
                << ", address:" << &client_table << ") in bgworker=" << my_id_;
        client_table = new ClientTable(table_id, client_table_config);
      } catch (std::bad_alloc &e) {
        LOG(FATAL) << "Bad alloc exception";
      }
      // not thread-safe
      (*tables_)[table_id] = client_table;

      size_t sent_size =
          comm_bus_->SendInProc(sender_id, zmq_msg.data(), zmq_msg.size());
      CHECK_EQ(sent_size, zmq_msg.size());
    }
  }

  {
    zmq::message_t zmq_msg;
    int32_t sender_id;
    (comm_bus_->*(comm_bus_->RecvAny_))(&sender_id, &zmq_msg);
    MsgType msg_type = MsgBase::get_msg_type(zmq_msg.data());
    CHECK_EQ(msg_type, kCreatedAllTables);
    VLOG(5) << "THREAD-" << my_id_
            << ": Received a kCreatedAllTables message from sender="
            << sender_id;
  }
}

long AbstractBgWorker::HandleClockMsg(int32_t table_id, bool clock_advanced) {

  STATS_BG_ACCUM_CLOCK_END_OPLOG_SERIALIZE_BEGIN();
  petuum::HighResolutionTimer begin_clock;

  // preparation, partitions the oplog based on destination. The current
  // bg_thread only deals with row_ids that it is responsible for. After
  // preparation, we will know exactly how many bytes are being sent to each
  // server. We will also know how the data being sent to the server is split
  // across tables.

  BgOpLog *bg_oplog = PrepareOpLogsToSend(table_id);
  VLOG(20) << "PrepareOplogs successful";
  CreateOpLogMsgs(bg_oplog);
  VLOG(20) << "CreateOplogs successful";
  STATS_BG_ACCUM_CLOCK_END_OPLOG_SERIALIZE_END();

  clock_has_pushed_ = worker_clock_;
  // send the information to the server with info on whether the clock has
  // advanced (or) if the client is just pushing updates aggressively.
  SendOpLogMsgs(clock_advanced);
  VLOG(20) << "SendOpLogs successful";
  // increments the current version of the bgworker, and keeps track of the
  // oplog in ssp_row_request_oplog_manager
  // note that the version number is incremented even if the clock has not
  // advanced.
  TrackBgOpLog(bg_oplog);

  VLOG(20) << "THREAD-" << my_id_
           << ": Handle clock message (prepare, create, send) took "
           << begin_clock.elapsed() << " s at clock=" << worker_clock_;
  return 0;
  // the clock (worker_clock_) is immediately incremented after this function
  // completes
}

void AbstractBgWorker::PrepareBeforeInfiniteLoop() {}

void AbstractBgWorker::FinalizeTableStats() {}

long AbstractBgWorker::ResetBgIdleMilli() { return 0; }

long AbstractBgWorker::BgIdleWork() { return 0; }


/**
 */
void AbstractBgWorker::FinalizeOpLogMsgStats(int32_t table_id) {
  for (auto server_id : ephemeral_server_byte_counter_.GetKeysPosValue()) {
    // add the size used to represent the number of rows in an update to stats
    ephemeral_server_byte_counter_.Increment(server_id, sizeof(int32_t));
    ephemeral_server_table_size_counter_.Increment(server_id, table_id,
            ephemeral_server_byte_counter_.Get(server_id));
  }
}


void AbstractBgWorker::CreateOpLogMsgs(const BgOpLog *bg_oplog) {

  std::map<int32_t, std::map<int32_t, void *>> table_server_mem_map;
  size_t bytes_written = 0;
  size_t bytes_allocated = 0;

  for (auto server_id : server_ids_) {

      if(0 == ephemeral_server_table_size_counter_.Get(server_id)) {
          continue;
      }

    ServerOpLogSerializer oplog_serializer;
    size_t msg_size = oplog_serializer.Init(server_id, ephemeral_server_table_size_counter_);

    if (msg_size == 0) { continue; }

    ephemeral_server_oplog_msg_.Put(server_id, new ClientSendOpLogMsg(msg_size));
    bytes_allocated += msg_size;

    oplog_serializer.AssignMem(ephemeral_server_oplog_msg_.Get(server_id)->get_data(), bytes_written);

    for (const auto &table_pair : (*tables_)) {
      int32_t table_id = table_pair.first;
      uint8_t *table_ptr = reinterpret_cast<uint8_t *> (oplog_serializer.GetTablePtr(table_id));

      if (table_ptr == nullptr) {
        table_server_mem_map[table_id].erase(server_id);
        continue;
      }

      // 1. table id
      // 2. table update size
      // 3. table data

      // table id -- store table_id at the table_ptr location
      *(reinterpret_cast<int32_t *>(table_ptr)) = table_id;
      bytes_written += sizeof(int32_t);

      // table update size -- store table update size at the table_prt + one
      // int32 location
      // some understanding of the serialization is also happening here.
      *(reinterpret_cast<size_t *>(table_ptr + sizeof(int32_t))) =
          table_pair.second->get_sample_row()->get_update_size();
      bytes_written += sizeof(size_t);

      // offset for table rows -- store the offset for each table and each
      // server. This is the offset into oplog msg's memory.
      table_server_mem_map[table_id][server_id] =
          table_ptr + sizeof(int32_t) + sizeof(size_t);
    }
  }

  // here the re-arranging of the different dimensions happen. We use the
  // bg_oplog data structure that partitions oplog into tables and server to
  // write into locations pointed in the table_server_mem_map.

  for (const auto &table_pair : (*tables_)) {
    int32_t table_id = table_pair.first;
    BgOpLogPartition *oplog_partition = bg_oplog->Get(table_id);
    // the second argument to function is an indicator to notify is the
    // serialization is dense or sparse
    bytes_written += oplog_partition->SerializeByServer(
        &(table_server_mem_map[table_id]),
        table_pair.second->oplog_dense_serialized());
  }

  VLOG(20) << "Total bytes allocated=" << bytes_allocated;
  VLOG(20) << "Total bytes_written=" << bytes_written;
  CHECK_EQ(bytes_written, bytes_allocated);
}

size_t AbstractBgWorker::SendOpLogMsgs(bool clock_advanced) {
  size_t accum_size = 0;

  STATS_MLFABRIC_CLIENT_PUSH_BEGIN(0, per_worker_update_version_);

  for (const auto &server_id : server_ids_) {
    // server_oplog_msg_msp will be populated in Create Op Log Msgs
    ClientSendOpLogMsg *msg = ephemeral_server_oplog_msg_.Get(server_id);
    STATS_MLFABRIC_CLIENT_PUSH_BEGIN(server_id, per_worker_update_version_);

    if (msg != nullptr) {
      // if there is data that needs to be sent to the server, we send it along
      // with clock information.
      msg->get_is_clock() = clock_advanced;
      msg->get_client_id() = GlobalContext::get_client_id();
      msg->get_version() = per_worker_update_version_;
      msg->get_bg_clock() = clock_has_pushed_ + 1;

      accum_size += msg->get_size();
      MemTransfer::TransferMem(comm_bus_, server_id, msg);
      // delete message after send
      delete msg;

      VLOG(2) << "THREAD-" << my_id_
              << ": Oplog sent: client_clock=" << worker_clock_
              << " server=" << server_id
              << " clientversion=" << per_worker_update_version_
              << " size=" << accum_size << " time=" << GetElapsedTime();

    } else {

      // If there is no gradient update to be sent to the server, then we just
      // send them a clock message notifying the server that client has moved
      // its clock (we also tell the server the iteration (clock) that
      // generated the data).
      // create a message with zero data size
      ClientSendOpLogMsg clock_oplog_msg(0);
      clock_oplog_msg.get_is_clock() = clock_advanced;
      clock_oplog_msg.get_client_id() = GlobalContext::get_client_id();
      clock_oplog_msg.get_version() = per_worker_update_version_;
      clock_oplog_msg.get_bg_clock() = clock_has_pushed_ + 1;

      accum_size += clock_oplog_msg.get_size();
      MemTransfer::TransferMem(comm_bus_, server_id, &clock_oplog_msg);
    }
  }

  STATS_MLFABRIC_CLIENT_PUSH_END(0, per_worker_update_version_);

  STATS_BG_ADD_PER_CLOCK_OPLOG_SIZE(accum_size);
  return accum_size;
}


/**
 */
size_t AbstractBgWorker::AddOplogAndCountPerServerSize(
    int32_t row_id, AbstractRowOpLog *row_oplog,
    BgOpLogPartition *bg_table_oplog,
    GetSerializedRowOpLogSizeFunc GetSerializedRowOpLogSize) {
  // row oplog message size includes allocation for
  // 1) row id
  // 2) global version id
  // 3) serialized row size
  size_t serialized_size = sizeof(int32_t) + sizeof(int32_t) + GetSerializedRowOpLogSize(row_oplog);

  int32_t server_id = GlobalContext::GetPartitionServerID(row_id, my_comm_channel_idx_);

  ephemeral_server_byte_counter_.Increment(server_id, serialized_size);

  bg_table_oplog->InsertOpLog(row_id, row_oplog);

  return serialized_size;
}


void AbstractBgWorker::RecvAppInitThreadConnection(
    int32_t *num_connected_app_threads) {
  zmq::message_t zmq_msg;
  int32_t sender_id;
  comm_bus_->RecvInProc(&sender_id, &zmq_msg);
  MsgType msg_type = MsgBase::get_msg_type(zmq_msg.data());
  CHECK_EQ(msg_type, kAppConnect) << "send_id = " << sender_id;
  ++(*num_connected_app_threads);
  CHECK(*num_connected_app_threads <= GlobalContext::get_num_app_threads());
}

void AbstractBgWorker::CheckForwardRowRequestToServer(
    int32_t app_thread_id, RowRequestMsg &row_request_msg) {

  int32_t table_id = row_request_msg.get_table_id();
  int32_t row_id = row_request_msg.get_row_id();
  int32_t clock = row_request_msg.get_clock();
  bool forced = row_request_msg.get_forced_request();

  if (!forced) {
    // Check if the row exists in process cache
    auto table_iter = tables_->find(table_id);
    CHECK(table_iter != tables_->end());
    {
      // check if it is in process storage
      ClientTable *table = table_iter->second;
      AbstractProcessStorage &table_storage = table->get_process_storage();
      RowAccessor row_accessor;
      ClientRow *client_row = table_storage.Find(row_id, &row_accessor);
      if (client_row != nullptr) {
        if (GlobalContext::get_consistency_model() == SSP &&
            client_row->GetClock() >= clock) {
          VLOG(20) << "RRR-cache hit BgThread (" << my_id_
                   << ") >>> App Thread (" << app_thread_id << ") "
                   << petuum::GetTableRowStringId(table_id, row_id)
                   << " request clock=" << clock
                   << " row clock=" << client_row->GetClock();
          RowRequestReplyMsg row_request_reply_msg;
          size_t sent_size = comm_bus_->SendInProc(
              app_thread_id, row_request_reply_msg.get_mem(),
              row_request_reply_msg.get_size());
          CHECK_EQ(sent_size, row_request_reply_msg.get_size());
          return;
        }
      }
    }
  }

  RowRequestInfo row_request;
  row_request.app_thread_id = app_thread_id;
  row_request.clock = row_request_msg.get_clock();

  // Version in request denotes the update version that the row on server can
  // see. Which should be 1 less than the current version number.
  // raajay: per_worker_update_version_ is the latest version that is pushed to
  // the server from this client.
  // which means the request can be for one less than that?

  // (raajay) when ever an app thread requests for a row and it is not found
  // in the process storage, then the following lines are encountered.
  // Basically, a row request has to be sent to the server now. We also
  // remember that the app thread made a request for the row when the current
  // version of the model was blah.

  row_request.version = per_worker_update_version_ - 1;

  bool should_be_sent =
      row_request_oplog_mgr_->AddRowRequest(row_request, table_id, row_id);

  if (should_be_sent) {
    int32_t server_id =
        GlobalContext::GetPartitionServerID(row_id, my_comm_channel_idx_);
    VLOG(20) << "THREAD-" << my_id_
             << ": Sending a row request received from app thread="
             << app_thread_id << " to server=" << server_id
             << " for table=" << table_id << " and row_id=" << row_id
             << " with version=" << row_request.version;

    size_t sent_size = (comm_bus_->*(comm_bus_->SendAny_))(
        server_id, row_request_msg.get_mem(), row_request_msg.get_size());

    CHECK_EQ(sent_size, row_request_msg.get_size());
  }
} // end function -- CheckForwardRowRequestToServer

void AbstractBgWorker::UpdateExistingRow(
    int32_t table_id, int32_t row_id, ClientRow *client_row,
    ClientTable *client_table, const void *data, size_t row_size,
    uint32_t version) { // version : from row request reply msg

  AbstractRow *row_data = client_row->GetRowDataPtr();
  row_data->GetWriteLock();
  row_data->ResetRowData(data, row_size);
  row_data->ReleaseWriteLock();

} // end function - Update Existing Row

void AbstractBgWorker::InsertNonexistentRow(int32_t table_id, int32_t row_id,
                                            ClientTable *client_table,
                                            const void *data, size_t row_size,
                                            uint32_t version, int32_t clock,
                                            int32_t global_model_version) {

  int32_t row_type = client_table->get_row_type();
  AbstractRow *row_data =
      ClassRegistry<AbstractRow>::GetRegistry().CreateObject(row_type);
  row_data->Deserialize(data, row_size);
  ClientRow *client_row =
      CreateClientRow(clock, global_model_version, row_data);
  client_table->get_process_storage().Insert(row_id, client_row);

} // end function -- Insert Non existent row

void AbstractBgWorker::HandleServerRowRequestReply(
    int32_t server_id, ServerRowRequestReplyMsg &server_row_request_reply_msg) {

  int32_t table_id = server_row_request_reply_msg.get_table_id();
  int32_t row_id = server_row_request_reply_msg.get_row_id();
  int32_t clock = server_row_request_reply_msg.get_clock();
  uint32_t version = server_row_request_reply_msg.get_version();
  int32_t global_model_version =
      server_row_request_reply_msg.get_global_model_version();

  auto table_iter = tables_->find(table_id);
  CHECK(table_iter != tables_->end()) << "Cannot find table " << table_id;
  ClientTable *client_table = table_iter->second;

  // (raajay) for SSP the below function does nothing.
  row_request_oplog_mgr_->ServerAcknowledgeVersion(server_id, version);

  RowAccessor row_accessor;
  ClientRow *client_row =
      client_table->get_process_storage().Find(row_id, &row_accessor);

  const void *data = server_row_request_reply_msg.get_row_data();
  size_t row_size = server_row_request_reply_msg.get_row_size();

  if (client_row != nullptr) {
    // internal private function defined in this class.
    VLOG(20) << "Update ClientRow "
             << petuum::GetTableRowStringId(table_id, row_id)
             << " clock=" << client_row->GetClock() << " new clock=" << clock;

    CHECK_GT(clock, client_row->GetClock())
        << "Latest clock is not greater than prior clock for "
        << petuum::GetTableRowStringId(table_id, row_id) << ". "
        << "latest clock=" << clock
        << " prior clock=" << client_row->GetClock();

    UpdateExistingRow(table_id, row_id, client_row, client_table, data,
                      row_size, version);
    client_row->SetClock(clock);
    client_row->SetGlobalVersion(global_model_version);
  } else { // not found
    VLOG(20) << "New ClientRow "
             << petuum::GetTableRowStringId(table_id, row_id)
             << " new clock=" << clock;
    InsertNonexistentRow(table_id, row_id, client_table, data, row_size,
                         version, clock, global_model_version);
  }

  // populate app_thread_ids with the list of app threads whose request can be
  // satisfied with this update.
  std::vector<int32_t> app_thread_ids;
  int32_t clock_to_request = row_request_oplog_mgr_->InformReply(
      table_id, row_id, clock, per_worker_update_version_, &app_thread_ids);

  if (clock_to_request >= 0) {
    // send a new request to the server, if there exists a app row request
    // with a higher clock for which a request to server has not been sent

    RowRequestMsg row_request_msg;
    row_request_msg.get_table_id() = table_id;
    row_request_msg.get_row_id() = row_id;
    row_request_msg.get_clock() = clock_to_request;

    int32_t server_id_1 =
        GlobalContext::GetPartitionServerID(row_id, my_comm_channel_idx_);
    VLOG(20) << "RR BgThread >>> ServerThread (" << server_id_1 << ") "
             << petuum::GetTableRowStringId(table_id, row_id);
    size_t sent_size = (comm_bus_->*(comm_bus_->SendAny_))(
        server_id_1, row_request_msg.get_mem(), row_request_msg.get_size());
    CHECK_EQ(sent_size, row_request_msg.get_size());
  }

  // respond to each satisfied application row request
  RowRequestReplyMsg row_request_reply_msg;
  for (int app_thread_id : app_thread_ids) {
    VLOG(20) << "RRR BgThread (" << my_id_ << ") >>> App Thread ("
             << app_thread_id << ") "
             << petuum::GetTableRowStringId(table_id, row_id)
             << " clock=" << clock;
    size_t sent_size =
        comm_bus_->SendInProc(app_thread_id, row_request_reply_msg.get_mem(),
                              row_request_reply_msg.get_size());
    CHECK_EQ(sent_size, row_request_reply_msg.get_size());
  }

} // end function - Handle Row Request reply msg

size_t AbstractBgWorker::SendMsg(MsgBase *msg) {
  size_t sent_size =
      comm_bus_->SendInProc(my_id_, msg->get_mem(), msg->get_size());
  return sent_size;
}

void AbstractBgWorker::RecvMsg(zmq::message_t &zmq_msg) {
  int32_t sender_id;
  comm_bus_->RecvInProc(&sender_id, &zmq_msg);
}

void AbstractBgWorker::ConnectToNameNodeOrServer(int32_t server_id) {

  ClientConnectMsg client_connect_msg;
  client_connect_msg.get_client_id() = GlobalContext::get_client_id();
  void *msg = client_connect_msg.get_mem();
  int32_t msg_size = client_connect_msg.get_size();

  if (comm_bus_->IsLocalEntity(server_id)) {
    comm_bus_->ConnectTo(server_id, msg, msg_size);
  } else {
    HostInfo server_info;
    if (server_id == GlobalContext::get_name_node_id()) {
      server_info = GlobalContext::get_name_node_info();
    } else {
      server_info = GlobalContext::get_server_info(server_id);
    }
    std::string server_addr = server_info.ip + ":" + server_info.port;
    comm_bus_->ConnectTo(server_id, server_addr, msg, msg_size);
  }
}

void *AbstractBgWorker::operator()() {
  STATS_REGISTER_THREAD(kBgThread);

  ThreadContext::RegisterThread(my_id_);

  InitCommBus();

  BgServerHandshake();

  pthread_barrier_wait(init_barrier_);

  int32_t num_connected_app_threads = 0;
  int32_t num_deregistered_app_threads = 0;
  int32_t num_shutdown_acked_servers = 0;
  int32_t num_ephemeral_app_threads = 0;

  VLOG(5) << "THREAD-" << my_id_ << ": Prepare to connect with app threads";
  RecvAppInitThreadConnection(&num_connected_app_threads);
  VLOG(5) << "THREAD-" << my_id_ << ": Bg Worker thread:" << my_id_
          << " connected with " << num_connected_app_threads << " app threads.";
  from_start_timer_.restart();

  if (my_comm_channel_idx_ == 0) {
    HandleCreateTables();
  }
  pthread_barrier_wait(create_table_barrier_);

  FinalizeTableStats();

  zmq::message_t zmq_msg;
  int32_t sender_id;
  MsgType msg_type;
  void *msg_mem;
  bool destroy_mem = false;
  long timeout_milli = GlobalContext::get_bg_idle_milli();
  PrepareBeforeInfiniteLoop();
  // here, the BgWorker runs an infinite loop and processes the different
  // messages
  // initiated by either the AppThread (eg. RequestRow) or server thread (e.g. )
  // Further, it also creates new handle clock messages I guess.
  while (true) {
    bool received = WaitMsg_(&sender_id, &zmq_msg, timeout_milli);

    if (!received) {
      timeout_milli = BgIdleWork();
      continue;
    } else {
      // (TODO): verify the difference in performance
      timeout_milli = ResetBgIdleMilli();
    }

    msg_type = MsgBase::get_msg_type(zmq_msg.data());
    destroy_mem = false;

    if (msg_type == kMemTransfer) {
      MemTransferMsg mem_transfer_msg(zmq_msg.data());
      msg_mem = mem_transfer_msg.get_mem_ptr();
      msg_type = MsgBase::get_msg_type(msg_mem);
      destroy_mem = true;
    } else {
      msg_mem = zmq_msg.data();
    }

    switch (msg_type) {
    case kAppConnect: {
      ++num_connected_app_threads;

      CHECK(num_connected_app_threads <= GlobalContext::get_num_app_threads())
          << "num_connected_app_threads = " << num_connected_app_threads
          << " get_num_app_threads() = "
          << GlobalContext::get_num_app_threads();
    } break;
    case kAppThreadDereg: {
      ++num_deregistered_app_threads;
      // when all the app thread have de-registered, send a shut down message to
      // namenode,
      // scheduler and all the servers.
      if (num_deregistered_app_threads ==
          GlobalContext::get_num_app_threads()) {
        ClientShutDownMsg msg;
        int32_t name_node_id = GlobalContext::get_name_node_id();
        (comm_bus_->*(comm_bus_->SendAny_))(name_node_id, msg.get_mem(),
                                            msg.get_size());

        for (const auto &server_id : server_ids_) {
          (comm_bus_->*(comm_bus_->SendAny_))(server_id, msg.get_mem(),
                                              msg.get_size());
        }
      }
    } break;

    case kSyncThreadConnect:
      ++num_ephemeral_app_threads;
      // (raajay) since ephemeral threads have an exclusive lock
      // on the tables, the number of such threads at any given
      // point should not exceed the number of tables.
      CHECK_LE(num_ephemeral_app_threads, GlobalContext::get_num_tables());
      break;

    case kSyncThreadDereg:
      --num_ephemeral_app_threads;
      CHECK_GE(num_ephemeral_app_threads, 0);
      break;

    case kServerShutDownAck: {
      ++num_shutdown_acked_servers;
      // if all them ack your shutdown, only then de-register and terminate out
      // of the infinite loop
      if (num_shutdown_acked_servers ==
          GlobalContext::get_num_server_clients() + 1) {
        comm_bus_->ThreadDeregister();
        STATS_DEREGISTER_THREAD();
        return nullptr;
      }
    } break;
    case kApplicationThreadRowRequest: {
      // app thread typically sends a row request, your job is to forward it to
      // the server.
      RowRequestMsg row_request_msg(msg_mem);
      CheckForwardRowRequestToServer(sender_id, row_request_msg);
    } break;
    case kServerRowRequestReply: {
      // server responds with the information of rows requested
      ServerRowRequestReplyMsg server_row_request_reply_msg(msg_mem);
      HandleServerRowRequestReply(sender_id, server_row_request_reply_msg);
    } break;
    case kBgClock: {
      // clock message is sent from the app thread using the static function
      // defined in bgworkers.
      BgClockMsg clock_msg(msg_mem);
      timeout_milli = HandleClockMsg(clock_msg.get_table_id(), true);
      ++worker_clock_;
      VLOG(5) << "THREAD-" << my_id_ << ": "
              << "Increment client clock in bgworker:" << my_id_ << " to "
              << worker_clock_;
      STATS_BG_CLOCK();
    } break;
    case kBgSendOpLog: {
      BgSendOpLogMsg oplog_msg(msg_mem);
      timeout_milli = HandleClockMsg(oplog_msg.get_table_id(), false);
    } break;
    case kServerOpLogAck: {
      // this is sent by the server to acknowledge the receipt of an oplog
      ServerOpLogAckMsg server_oplog_ack_msg(msg_mem);
      int32_t acked_version = server_oplog_ack_msg.get_ack_version();
      row_request_oplog_mgr_->ServerAcknowledgeVersion(sender_id,
                                                       acked_version);
      STATS_MLFABRIC_CLIENT_PUSH_END(sender_id, acked_version);
    } break;
    default:
      LOG(FATAL) << "Unrecognized type " << msg_type;
    }

    if (destroy_mem) {
      MemTransfer::DestroyTransferredMem(msg_mem);
    }
  }

  return 0;
}
}
