// name_node_thread.cpp
// author: jinliang

#include <petuum_ps/server/name_node_thread.hpp>
#include <iostream>

namespace petuum {

NameNodeThread::NameNodeThread(pthread_barrier_t *init_barrier)
    : my_id_(0), init_barrier_(init_barrier),
      comm_bus_(GlobalContext::comm_bus),
      bg_worker_ids_(GlobalContext::get_num_total_bg_threads()),
      num_shutdown_bgs_(0) {}

/* Private Functions */
int32_t NameNodeThread::GetConnection(bool *is_client, int32_t *client_id) {
  int32_t sender_id;
  zmq::message_t zmq_msg;
  (comm_bus_->*(comm_bus_->RecvAny_))(&sender_id, &zmq_msg);
  MsgType msg_type = MsgBase::get_msg_type(zmq_msg.data());

  if (msg_type == kClientConnect) {

    ClientConnectMsg msg(zmq_msg.data());
    *is_client = true;
    *client_id = msg.get_client_id();
    VLOG(5) << "Receive connection from worker: " << msg.get_client_id();

  } else if (msg_type == kAggregatorConnect) {

    AggregatorConnectMsg msg(zmq_msg.data());
    *is_client = false;
    CHECK_EQ(msg_type, kAggregatorConnect);
    VLOG(5) << "Receive connection from aggregator: " << msg.get_client_id();

  } else {

    CHECK_EQ(msg_type, kServerConnect);
    *is_client = false;
    VLOG(5) << "Receive connection from server.";
  }
  return sender_id;
}

void NameNodeThread::SendToAllBgThreads(MsgBase *msg) {
  for (const auto &bg_id : bg_worker_ids_) {
    size_t sent_size = (comm_bus_->*(comm_bus_->SendAny_))(
        bg_id, msg->get_mem(), msg->get_size());
    CHECK_EQ(sent_size, msg->get_size());
  }
}

void NameNodeThread::SendToAllServers(MsgBase *msg) {
  std::vector<int32_t> server_ids = GlobalContext::get_all_server_ids();
  for (const auto &server_id : server_ids) {
    size_t sent_size = (comm_bus_->*(comm_bus_->SendAny_))(
        server_id, msg->get_mem(), msg->get_size());
    CHECK_EQ(sent_size, msg->get_size());
  }
}

void NameNodeThread::InitNameNode() {
  int32_t num_bgs = 0;
  int32_t num_servers = 0;
  int32_t num_expected_conns = (GlobalContext::get_num_total_bg_threads() +
                                GlobalContext::get_num_total_server_threads());

  VLOG(5) << "Number of expected connections at name node="
          << num_expected_conns;

  int32_t num_connections;
  for (num_connections = 0; num_connections < num_expected_conns;
       ++num_connections) {
    int32_t client_id;
    bool is_client;
    int32_t sender_id = GetConnection(&is_client, &client_id);
    if (is_client) {
      bg_worker_ids_[num_bgs] = sender_id;
      ++num_bgs;
    } else {
      ++num_servers;
    }
  }
  VLOG(5) << "Total connections received: " << num_connections;
  CHECK_EQ(num_bgs, GlobalContext::get_num_total_bg_threads());

  server_obj_.Init(0, bg_worker_ids_);

  // Note that we send two types of messages to the bg worker threads
  ConnectServerMsg connect_server_msg;
  VLOG(5) << "Name node - send connect server to all bg threads";
  SendToAllBgThreads(reinterpret_cast<MsgBase *>(&connect_server_msg));

  ClientStartMsg client_start_msg;
  VLOG(5) << "Name node - send client start to all bg threads";
  SendToAllBgThreads(reinterpret_cast<MsgBase *>(&client_start_msg));

} // end function -- init name node

bool NameNodeThread::HaveCreatedAllTables() {
  if ((int32_t)create_table_map_.size() < GlobalContext::get_num_tables())
    return false;

  for (const auto &create_table_pair : create_table_map_) {
    if (!create_table_pair.second.RepliedToAllClients()) {
      return false;
    }
  }
  return true;
}

/**
 * Once all the tables are created at the server, we notify
 * all the workers the success notification.
 */
void NameNodeThread::SendCreatedAllTablesMsg() {
  CreatedAllTablesMsg created_all_tables_msg;
  for (auto client_id : GlobalContext::get_worker_client_ids()) {
    int32_t head_bg_id = GlobalContext::get_head_bg_id(client_id);
    size_t sent_size = (comm_bus_->*(comm_bus_->SendAny_))(
        head_bg_id, created_all_tables_msg.get_mem(),
        created_all_tables_msg.get_size());
    CHECK_EQ(sent_size, created_all_tables_msg.get_size());
  }
}

bool NameNodeThread::HandleShutDownMsg() {
  // When num_shutdown_bgs reaches the total number of bg threads, the server
  // reply to each bg with a ShutDownReply message
  ++num_shutdown_bgs_;
  if (num_shutdown_bgs_ == GlobalContext::get_num_total_bg_threads()) {
    ServerShutDownAckMsg shut_down_ack_msg;
    size_t msg_size = shut_down_ack_msg.get_size();

    for (int i = 0; i < GlobalContext::get_num_total_bg_threads(); ++i) {
      int32_t bg_id = bg_worker_ids_[i];
      size_t sent_size = (comm_bus_->*(comm_bus_->SendAny_))(
          bg_id, shut_down_ack_msg.get_mem(), msg_size);
      CHECK_EQ(msg_size, sent_size);
    }

    return true;
  }
  return false;
}

void NameNodeThread::HandleCreateTable(int32_t sender_id,
                                       CreateTableMsg &create_table_msg) {
  int32_t table_id = create_table_msg.get_table_id();

  if (create_table_map_.count(table_id) == 0) {
    // create a new TableInfo if this the first request for the table.
    TableInfo table_info;
    table_info.table_staleness = create_table_msg.get_staleness();
    table_info.row_type = create_table_msg.get_row_type();
    table_info.row_capacity = create_table_msg.get_row_capacity();
    table_info.oplog_dense_serialized =
        create_table_msg.get_oplog_dense_serialized();
    table_info.row_oplog_type = create_table_msg.get_row_oplog_type();
    table_info.dense_row_oplog_capacity =
        create_table_msg.get_dense_row_oplog_capacity();

    VLOG(5) << "Calling CreateTable from instantiation of Server("
            << &server_obj_ << ") for table=" << table_id
            << " in name_node_thread";
    server_obj_.CreateTable(table_id, table_info);

    create_table_map_.insert(std::make_pair(
        table_id, CreateTableInfo())); // access it to call default constructor
    SendToAllServers(reinterpret_cast<MsgBase *>(&create_table_msg));
  }

  if (create_table_map_[table_id].ReceivedFromAllServers()) {

    // if the current table is already created, let the bg worker know that.
    CreateTableReplyMsg create_table_reply_msg;
    create_table_reply_msg.get_table_id() = create_table_msg.get_table_id();
    size_t sent_size = (comm_bus_->*(comm_bus_->SendAny_))(
        sender_id, create_table_reply_msg.get_mem(),
        create_table_reply_msg.get_size());
    CHECK_EQ(sent_size, create_table_reply_msg.get_size());

    ++create_table_map_[table_id].num_clients_replied_;

    if (HaveCreatedAllTables()) {
      SendCreatedAllTablesMsg();
    }

  } else {
    // to be sent later
    create_table_map_[table_id].bgs_to_reply_.push(sender_id);
  }

} // end function -- HandleCreateTable

void NameNodeThread::HandleCreateTableReply(
    CreateTableReplyMsg &create_table_reply_msg) {

  int32_t table_id = create_table_reply_msg.get_table_id();
  ++create_table_map_[table_id].num_servers_replied_;

  if (create_table_map_[table_id].ReceivedFromAllServers()) {

    std::queue<int32_t> &bgs_to_reply =
        create_table_map_[table_id].bgs_to_reply_;
    while (!bgs_to_reply.empty()) {
      int32_t bg_id = bgs_to_reply.front();
      bgs_to_reply.pop();
      size_t sent_size = (comm_bus_->*(comm_bus_->SendAny_))(
          bg_id, create_table_reply_msg.get_mem(),
          create_table_reply_msg.get_size());
      CHECK_EQ(sent_size, create_table_reply_msg.get_size());
      ++create_table_map_[table_id].num_clients_replied_;
    }

    if (HaveCreatedAllTables()) {
      SendCreatedAllTablesMsg();
    }
  }
}

void NameNodeThread::SetUpCommBus() {
  CommBus::Config comm_config;
  comm_config.entity_id_ = my_id_;

  if (GlobalContext::get_num_clients() > 1) {
    comm_config.ltype_ = CommBus::kInProc | CommBus::kInterProc;
    HostInfo host_info = GlobalContext::get_name_node_info();
    comm_config.network_addr_ = "*:" + host_info.port;
  } else {
    comm_config.ltype_ = CommBus::kInProc;
  }

  comm_bus_->ThreadRegister(comm_config);
  std::cout << "NameNode is ready to accept connections!" << std::endl;
}

void *NameNodeThread::operator()() {
  ThreadContext::RegisterThread(my_id_);

  // set up thread-specific server context
  SetUpCommBus();

  pthread_barrier_wait(init_barrier_);

  // wait for connections from client, server
  InitNameNode();

  zmq::message_t zmq_msg;
  int32_t sender_id;

  while (1) {
    (comm_bus_->*(comm_bus_->RecvAny_))(&sender_id, &zmq_msg);
    MsgType msg_type = MsgBase::get_msg_type(zmq_msg.data());

    switch (msg_type) {
    case kClientShutDown: {
      bool shutdown = HandleShutDownMsg();
      if (shutdown) {
        comm_bus_->ThreadDeregister();
        return 0;
      }
      break;
    }
    case kCreateTable: {
      CreateTableMsg create_table_msg(zmq_msg.data());
      HandleCreateTable(sender_id, create_table_msg);
      break;
    }
    case kCreateTableReply: {
      CreateTableReplyMsg create_table_reply_msg(zmq_msg.data());
      HandleCreateTableReply(create_table_reply_msg);
      break;
    }
    default:
      LOG(FATAL) << "Unrecognized message type " << msg_type
                 << " sender = " << sender_id;
    }

  } // end while -- infinite loop
} // end function -- operator ()

} // end namespace -- petuum
