#include <petuum_ps/replica/replica_thread.hpp>
#include <petuum_ps/thread/msg_base.hpp>
#include <petuum_ps/thread/context.hpp>
#include <petuum_ps/thread/ps_msgs.hpp>
#include <petuum_ps/util/stats.hpp>
#include <petuum_ps/thread/mem_transfer.hpp>

namespace petuum {

  bool ReplicaThread::WaitMsgBusy(int32_t *sender_id,
                                 zmq::message_t *zmq_msg,
                                 long timeout_milli __attribute__ ((unused)) ) {
    bool received = (GlobalContext::comm_bus->*(GlobalContext::comm_bus->RecvAsyncAny_))
      (sender_id, zmq_msg);
    while (!received) {
      received = (GlobalContext::comm_bus->*(GlobalContext::comm_bus->RecvAsyncAny_))
        (sender_id, zmq_msg);
    }
    return true;
  }



  bool ReplicaThread::WaitMsgSleep(int32_t *sender_id,
                                  zmq::message_t *zmq_msg,
                                  long timeout_milli __attribute__ ((unused)) ) {
    (GlobalContext::comm_bus->*(
                                GlobalContext::comm_bus->RecvAny_))(sender_id, zmq_msg);

    return true;
  }



  bool ReplicaThread::WaitMsgTimeOut(int32_t *sender_id,
                                    zmq::message_t *zmq_msg,
                                    long timeout_milli) {

    bool received = (GlobalContext::comm_bus->*(GlobalContext::comm_bus->RecvTimeOutAny_))
      (sender_id, zmq_msg, timeout_milli);
    return received;
  }



  void ReplicaThread::InitWhenStart() {
    SetWaitMsg();
  }



  void ReplicaThread::SetWaitMsg() {
    if (GlobalContext::get_aggressive_cpu()) {
      WaitMsg_ = WaitMsgBusy;
    } else {
      WaitMsg_ = WaitMsgSleep;
    }
  }



  void ReplicaThread::SetUpCommBus() {
    CommBus::Config comm_config;
    comm_config.entity_id_ = my_id_;

    if (GlobalContext::get_num_clients() > 1) {
      comm_config.ltype_ = CommBus::kInProc | CommBus::kInterProc;
      HostInfo host_info = GlobalContext::get_replica_info(my_id_);
      comm_config.network_addr_ = "*:" + host_info.port;
    } else {
      comm_config.ltype_ = CommBus::kInProc;
    }

    comm_bus_->ThreadRegister(comm_config);
  }


  void ReplicaThread::ConnectToEntity(int32_t entity_id) {
    ConnectMsg connect_msg;
    connect_msg.get_entity_type() = petuum::REPLICA;
    connect_msg.get_entity_id() = my_id_;

    void *msg = connect_msg.get_mem();
    int32_t msg_size = connect_msg.get_size();

    if (comm_bus_->IsLocalEntity(entity_id)) {
      comm_bus_->ConnectTo(entity_id, msg, msg_size);
    } else {
      HostInfo destination_info = GlobalContext::get_destination_info(entity_id);
      std::string destination_addr = destination_info.ip + ":" + destination_info.port;
      comm_bus_->ConnectTo(entity_id, destination_addr, msg, msg_size);
    }
  }


  int32_t ReplicaThread::GetConnection() {
    int32_t sender_id;
    zmq::message_t zmq_msg;
    (comm_bus_->*(comm_bus_->RecvAny_))(&sender_id, &zmq_msg);
    MsgType msg_type = MsgBase::get_msg_type(zmq_msg.data());

    if (msg_type == kConnect) {
      ConnectMsg msg(zmq_msg.data());
      int32_t entity_id = msg.get_entity_id();
      CHECK_EQ(sender_id, entity_id);
      EntityType entity_type = msg.get_entity_type();
      if(entity_type == petuum::WORKER) {
        bg_worker_ids_[num_registered_workers_++] = sender_id;
      } else if(entity_type == petuum::AGGREGATOR) {
        aggregator_ids_[num_registered_aggregators_++] = sender_id;
      } else {
        LOG(FATAL) << "Connect message from unknown type.";
      }
    } else if(msg_type == kServerConnect) {
      num_replied_servers_++;
    } else {
      LOG(FATAL) << "Server received request from non bgworker/aggregator";
    }
    return sender_id;
  }

  void ReplicaThread::SendToAllBgThreads(MsgBase *msg) {
    for (const auto &bg_worker_id : bg_worker_ids_) {
      size_t sent_size = (comm_bus_->*(comm_bus_->SendAny_))
        (bg_worker_id, msg->get_mem(), msg->get_size());
      CHECK_EQ(sent_size, msg->get_size());
    }
  }

  void ReplicaThread::SendToAllAggregatorThreads(MsgBase *msg) {
    for (const auto &aggregator_id : aggregator_ids_) {
      size_t sent_size = (comm_bus_->*(comm_bus_->SendAny_)) (aggregator_id, msg->get_mem(), msg->get_size());
      CHECK_EQ(sent_size, msg->get_size());
    }
  }

  void ReplicaThread::InitReplica() {
    // neither the name node nor scheduler respond.
    ConnectToEntity(GlobalContext::get_name_node_id());
    ConnectToEntity(GlobalContext::get_scheduler_id());

    // int32_t num_expected_connections = GlobalContext::get_num_worker_clients()
    //   + GlobalContext::get_num_aggregator_clients();
    int32_t num_expected_connections = 1; // from the server

    VLOG(5) << "Number of expected connections at server thread: " << num_expected_connections;

    int32_t num_connections;
    for (num_connections = 0; num_connections < num_expected_connections; ++num_connections) {
      GetConnection();
    } // end waiting for connections from aggregator and bg worker

    CHECK_EQ(num_replied_servers_, 1);

    server_obj_.Init(my_id_, bg_worker_ids_, true);

    // (raajay) we might need this later

    // ClientStartMsg client_start_msg;
    // VLOG(5) << "Server Thread - send client start to all bg threads";
    // SendToAllBgThreads(reinterpret_cast<MsgBase*>(&client_start_msg));

    // VLOG(5) << "Server Thread - send client start to all aggregators";
    // SendToAllAggregatorThreads(reinterpret_cast<MsgBase*>(&client_start_msg));

  } // end function -- init server




  bool ReplicaThread::HandleShutDownMsg() {
    // When num_shutdown_bgs reaches the total number of clients, the server
    // reply to each bg with a ShutDownReply message
    ++num_shutdown_bgs_;
    if (num_shutdown_bgs_ == GlobalContext::get_num_worker_clients()) {
      ServerShutDownAckMsg shut_down_ack_msg;
      size_t msg_size = shut_down_ack_msg.get_size();
      for (int i = 0; i < GlobalContext::get_num_worker_clients(); ++i) {
        int32_t bg_id = bg_worker_ids_[i];
        size_t sent_size = (comm_bus_->*(comm_bus_->SendAny_))
          (bg_id, shut_down_ack_msg.get_mem(), msg_size);
        CHECK_EQ(msg_size, sent_size);
      }
      return true;
    }
    return false;
  }



  void ReplicaThread::HandleCreateTable(int32_t sender_id,
                                       CreateTableMsg &create_table_msg) {
    int32_t table_id = create_table_msg.get_table_id();

    // I'm not name node
    CreateTableReplyMsg create_table_reply_msg;
    create_table_reply_msg.get_table_id() = create_table_msg.get_table_id();
    size_t sent_size = (comm_bus_->*(comm_bus_->SendAny_))
      (sender_id, create_table_reply_msg.get_mem(), create_table_reply_msg.get_size());
    CHECK_EQ(sent_size, create_table_reply_msg.get_size());

    TableInfo table_info;
    table_info.table_staleness = create_table_msg.get_staleness();
    table_info.row_type = create_table_msg.get_row_type();
    table_info.row_capacity = create_table_msg.get_row_capacity();
    table_info.oplog_dense_serialized
      = create_table_msg.get_oplog_dense_serialized();
    table_info.row_oplog_type
      = create_table_msg.get_row_oplog_type();
    table_info.dense_row_oplog_capacity
      = create_table_msg.get_dense_row_oplog_capacity();
    server_obj_.CreateTable(table_id, table_info);
  }


  void ReplicaThread::HandleOpLogMsg(int32_t sender_id,
                                    ClientSendOpLogMsg &client_send_oplog_msg) {

    STATS_SERVER_OPLOG_MSG_RECV_INC_ONE();

    bool is_clock = client_send_oplog_msg.get_is_clock(); // if the oplog also says that client has clocked
    int32_t bg_clock = client_send_oplog_msg.get_bg_clock(); // the value of clock at client
    uint32_t version = client_send_oplog_msg.get_version(); // the bg version of the oplog update

    STATS_SERVER_ADD_PER_CLOCK_OPLOG_SIZE(client_send_oplog_msg.get_size());


    int32_t observed_delay;
    STATS_SERVER_ACCUM_APPLY_OPLOG_BEGIN();
    server_obj_.ApplyOpLogUpdateVersion(client_send_oplog_msg.get_data(),
                                        client_send_oplog_msg.get_avai_size(),
                                        sender_id,
                                        version,
                                        &observed_delay);
    STATS_SERVER_ACCUM_APPLY_OPLOG_END();
    STATS_MLFABRIC_SERVER_RECORD_DELAY(observed_delay);

    // TODO add delay to the statistics

    bool clock_changed = false;
    if (is_clock) {
      clock_changed = server_obj_.ClockUntil(sender_id, bg_clock);
      if (clock_changed) {
        // update the stats clock
        STATS_SERVER_CLOCK();
        // we remove the piece of code that will look at buffered updates
        // respond if their clock request is not satisfied. In asynchronous
        // mode, we DO NOT buffer updates.
      } // end if -- clock changed
    } // end if -- is clock

    // always ack op log receipt, saying the version number for a particular
    // update from a client was applied to the model.
    SendOpLogAckMsg(sender_id, server_obj_.GetBgVersion(sender_id));
  }



  void ReplicaThread::HandleServerOpLogMsg(int32_t sender_id,
                                    ServerSendOpLogMsg &server_send_oplog_msg) {

    STATS_SERVER_OPLOG_MSG_RECV_INC_ONE();

    int32_t orig_sender = server_send_oplog_msg.get_original_sender_id();
    uint32_t orig_version = server_send_oplog_msg.get_original_version();

    VLOG(5) << "Received server oplog msg from " << sender_id
            << " orig_version=" << orig_version << " orig_sender=" << orig_sender;

    //int32_t server_model_version = server_send_oplog_msg.get_global_model_version();

    STATS_SERVER_ADD_PER_CLOCK_OPLOG_SIZE(server_send_oplog_msg.get_size());

    // int32_t observed_delay;
    // STATS_SERVER_ACCUM_APPLY_OPLOG_BEGIN();
    // server_obj_.ApplyOpLogUpdateVersion(server_send_oplog_msg.get_data(),
    //                                     server_send_oplog_msg.get_avai_size(),
    //                                     orig_sender,
    //                                     orig_version,
    //                                     &observed_delay);
    // STATS_SERVER_ACCUM_APPLY_OPLOG_END();
    // STATS_MLFABRIC_SERVER_RECORD_DELAY(observed_delay);

    // always ack op log receipt, saying the version number for a particular
    // update from a client was applied to the model.
    // SendOpLogAckMsg(sender_id, server_obj_.GetBgVersion(sender_id));

    SendServerOpLogAckMsg(sender_id, orig_version, orig_sender);
  }


  long ReplicaThread::ServerIdleWork() {
    return 0;
  }


  long ReplicaThread::ResetServerIdleMilli() {
    return 0;
  }


  /**
   * Is invoked at the end of handle Oplog msg function.
   */
  void ReplicaThread::SendOpLogAckMsg(int32_t bg_id, uint32_t version) {
    ServerOpLogAckMsg server_oplog_ack_msg;
    server_oplog_ack_msg.get_ack_version() = version;
    size_t msg_size = server_oplog_ack_msg.get_size();
    size_t sent_size = (comm_bus_->*(comm_bus_->SendAny_))(bg_id, server_oplog_ack_msg.get_mem(), msg_size);
    CHECK_EQ(msg_size, sent_size);
  }

  void ReplicaThread::SendServerOpLogAckMsg(int32_t server_id, uint32_t version, int32_t orig_sender) {
    ReplicaOpLogAckMsg replica_oplog_ack_msg;
    replica_oplog_ack_msg.get_ack_version() = version;
    replica_oplog_ack_msg.get_original_sender() = orig_sender;

    size_t msg_size = replica_oplog_ack_msg.get_size();
    size_t sent_size = (comm_bus_->*(comm_bus_->SendAny_))(server_id, replica_oplog_ack_msg.get_mem(), msg_size);
    CHECK_EQ(msg_size, sent_size);
  }

  void *ReplicaThread::operator() () {

    ThreadContext::RegisterThread(my_id_);

    STATS_REGISTER_THREAD(kServerThread);

    SetUpCommBus();

    pthread_barrier_wait(init_barrier_);

    // waits for bg worker threads from each worker client to connect. One bg
    // thread from each worker client will connect with a server thread. Each bg
    // worker is also notified that it can start.
    InitReplica();

    zmq::message_t zmq_msg;
    int32_t sender_id;
    MsgType msg_type;
    void *msg_mem;
    bool destroy_mem = false;
    long timeout_milli = GlobalContext::get_server_idle_milli();


    // like the bg thread, the server thread also goes on an infinite loop.
    // It processes one message at a time; TODO (raajay) shouldn't we have separate queues for
    // control and data messages.

    while(1) {
      bool received = WaitMsg_(&sender_id, &zmq_msg, timeout_milli);
      if (!received) {
        timeout_milli = ServerIdleWork();
        continue;
      } else {
        timeout_milli = GlobalContext::get_server_idle_milli();
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
      case kClientShutDown:
        {
          bool shutdown = HandleShutDownMsg();
          if (shutdown) {
            comm_bus_->ThreadDeregister();
            STATS_DEREGISTER_THREAD();
            return 0; // only point for terminating the thread.
          }
          break;
        }
      case kCreateTable:
        {
          CreateTableMsg create_table_msg(msg_mem);
          HandleCreateTable(sender_id, create_table_msg);
          break;
        }
      case kClientSendOpLog:
        {
          ClientSendOpLogMsg client_send_oplog_msg(msg_mem);
          HandleOpLogMsg(sender_id, client_send_oplog_msg);
        }
        break;
      case kServerSendOpLog:
        {
          ServerSendOpLogMsg server_send_oplog_msg(msg_mem);
          HandleServerOpLogMsg(sender_id, server_send_oplog_msg);
        }
        break;
      default:
        LOG(FATAL) << "Unrecognized message type " << msg_type;
      }

      if (destroy_mem)
        MemTransfer::DestroyTransferredMem(msg_mem);
    }
  }

}
