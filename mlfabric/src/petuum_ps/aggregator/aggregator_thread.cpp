#include <petuum_ps/aggregator/aggregator_thread.hpp>
#include <petuum_ps/aggregator/aggregator.hpp>
#include <petuum_ps/thread/mem_transfer.hpp>
#include <petuum_ps/thread/context.hpp>
#include <petuum_ps/util/stats.hpp>
#include <petuum_ps/thread/ps_msgs.hpp>
#include <petuum_ps/thread/msg_base.hpp>

namespace petuum {

  bool AggregatorThread::WaitMsgBusy(int32_t *sender_id,
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

  bool AggregatorThread::WaitMsgSleep(int32_t *sender_id,
                                      zmq::message_t *zmq_msg,
                                      long timeout_milli __attribute__ ((unused)) ) {
    (GlobalContext::comm_bus->*(GlobalContext::comm_bus->RecvAny_))(sender_id, zmq_msg);
    return true;
  }

  bool AggregatorThread::WaitMsgTimeOut(int32_t *sender_id,
                                        zmq::message_t *zmq_msg,
                                        long timeout_milli) {
    bool received = (GlobalContext::comm_bus->*(GlobalContext::comm_bus->RecvTimeOutAny_))
      (sender_id, zmq_msg, timeout_milli);
    return received;
  }

  void AggregatorThread::InitWhenStart() {
    SetWaitMsg();
  }

  void AggregatorThread::SetWaitMsg() {
    if (GlobalContext::get_aggressive_cpu()) {
      WaitMsg_ = WaitMsgBusy;
    } else {
      WaitMsg_ = WaitMsgSleep;
    }
  }

  void AggregatorThread::SetUpCommBus() {
    CommBus::Config comm_config;
    comm_config.entity_id_ = my_id_;

    if (GlobalContext::get_num_clients() > 1) {
      comm_config.ltype_ = CommBus::kInProc | CommBus::kInterProc;
      HostInfo host_info = GlobalContext::get_aggregator_info(my_id_);
      comm_config.network_addr_ = "*:" + host_info.port;
    } else {
      comm_config.ltype_ = CommBus::kInProc;
    }
    comm_bus_->ThreadRegister(comm_config);
  }

  void AggregatorThread::ConnectToEntity(int32_t entity_id) {
    ConnectMsg agg_connect_msg;
    agg_connect_msg.get_entity_id() = my_id_;
    agg_connect_msg.get_entity_type() = petuum::AGGREGATOR;
    void *msg = agg_connect_msg.get_mem();
    size_t msg_size = agg_connect_msg.get_size();
    if(comm_bus_->IsLocalEntity(entity_id)) {
      comm_bus_->ConnectTo(entity_id, msg, msg_size);
    } else {
      HostInfo destination_info = GlobalContext::get_destination_info(entity_id);
      std::string destination_addr = destination_info.ip + ":" + destination_info.port;
      comm_bus_->ConnectTo(entity_id, destination_addr, msg, msg_size);
    }
  }

  int32_t AggregatorThread::GetConnection() {

    int32_t sender_id;
    zmq::message_t zmq_msg;
    (comm_bus_->*(comm_bus_->RecvAny_))(&sender_id, &zmq_msg);
    MsgType msg_type = MsgBase::get_msg_type(zmq_msg.data());

    if (msg_type == kConnect) {
      ConnectMsg msg(zmq_msg.data());
      EntityType entity_type = msg.get_entity_type();
      int32_t entity_id = msg.get_entity_id();
      if(entity_type == petuum::WORKER) {
        bg_worker_ids_[num_registered_workers_++] = sender_id;
      } else {
        LOG(FATAL) << "Received connect from non WORKER entity:" << entity_id;
      }
    } else if(msg_type == kClientStart) {
      ClientStartMsg msg(zmq_msg.data());
      num_replied_servers_++;
    } else {
      LOG(FATAL) << "Aggregator received message from non worker/server";
    }
    return sender_id;
  } // end function -- get connection

  void AggregatorThread::SendToAllBgThreads(MsgBase *msg) {
    for (const auto &bg_worker_id : bg_worker_ids_) {
      size_t sent_size = (comm_bus_->*(comm_bus_->SendAny_))
        (bg_worker_id, msg->get_mem(), msg->get_size());
      CHECK_EQ(sent_size, msg->get_size());
    }
  }

  void AggregatorThread::InitAggregator() {

    ConnectToEntity(GlobalContext::get_name_node_id());
    ConnectToEntity(GlobalContext::get_scheduler_id());

    for (const auto &server_id : server_ids_) {
      ConnectToEntity(server_id);
    }

    // wait for connection from all bg threads and replies from all server
    int32_t num_expected_conns = GlobalContext::get_num_worker_clients()
      + GlobalContext::get_num_server_clients();

    int32_t num_connections;
    for (num_connections = 0; num_connections < num_expected_conns; ++num_connections) {
      GetConnection();
    } // end for -- over expected connections

    aggregator_obj_.Init(my_id_, bg_worker_ids_);

    ClientStartMsg client_start_msg;
    SendToAllBgThreads(reinterpret_cast<MsgBase*>(&client_start_msg));
  }


  // msg handler

  void AggregatorThread::HandleCreateTable(int32_t sender_id,
                                           CreateTableMsg &create_table_msg) {
    VLOG(5)  << "Aggregator received create table request from sender:" << sender_id
             << " table=" << create_table_msg.get_table_id();

    int32_t table_id = create_table_msg.get_table_id();

    CreateTableReplyMsg create_table_reply_msg;
    create_table_reply_msg.get_table_id() = create_table_msg.get_table_id();
    size_t sent_size = (comm_bus_->*(comm_bus_->SendAny_))(sender_id,
                                                           create_table_reply_msg.get_mem(),
                                                           create_table_reply_msg.get_size());
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

    aggregator_obj_.CreateTable(table_id, table_info);
  }


  void AggregatorThread::FlushServerTable() {
  }



  void *AggregatorThread::operator() () {
    ThreadContext::RegisterThread(my_id_);
    STATS_REGISTER_THREAD(kServerThread);
    SetUpCommBus();

    // wait launch all aggregator threads
    pthread_barrier_wait(init_barrier_);

    InitAggregator();

    zmq::message_t zmq_msg;
    int32_t sender_id;
    MsgType msg_type;
    void *msg_mem;
    bool destroy_mem = false;
    long timeout_milli = GlobalContext::get_server_idle_milli();

    VLOG(5) << "Entering the while loop on aggregator thread";
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

      if(msg_type == kMemTransfer) {
        MemTransferMsg mem_transfer_msg(zmq_msg.data());
        msg_mem = mem_transfer_msg.get_mem_ptr();
        msg_type = MsgBase::get_msg_type(msg_mem);
        destroy_mem = true;
      } else {
        msg_mem = zmq_msg.data();
      }

      switch(msg_type) {

      case kCreateTable:
        {
          CreateTableMsg create_table_msg(msg_mem);
          HandleCreateTable(sender_id, create_table_msg);
          break;
        }
      default:
        LOG(FATAL) << "Unrecognized message type " << msg_type;
        break;
      }

      if(destroy_mem) {
        MemTransfer::DestroyTransferredMem(msg_mem);
      }

    } // end while -- infinite loop
  }


  long AggregatorThread::ServerIdleWork() {
    return 0;
  }

  long AggregatorThread::ResetServerIdleMilli() {
    return 0;
  }
}

// Local Variables:
// mode: C++
// origami-fold-style: triple-braces
// End:
