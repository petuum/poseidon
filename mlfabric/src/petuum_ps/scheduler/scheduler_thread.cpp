// scheduler_thread.cpp
// author: raajay

#include <iostream>
#include <petuum_ps/scheduler/scheduler_thread.hpp>
#include <petuum_ps/thread/context.hpp>
#include <pthread.h>
#include <petuum_ps/util/pthread_barrier.hpp>
#include <petuum_ps/thread/ps_msgs.hpp>

namespace petuum {


  /**
     Constructor.
   */
  SchedulerThread::SchedulerThread(pthread_barrier_t *init_barrier):
    my_id_(GlobalContext::get_scheduler_id()), // the id of the scheduler is by default 900
    init_barrier_(init_barrier),
    comm_bus_(GlobalContext::comm_bus),
    bg_worker_ids_(GlobalContext::get_num_total_bg_threads()),
    server_ids_(GlobalContext::get_num_total_server_threads()),
    num_registered_servers_(0),
    num_registered_replicas_(0),
    num_registered_workers_(0),
    num_registered_aggregators_(0) {

    scheduler = new LowestDelayScheduler();
  }


  /**
     Destructor.
   */
  SchedulerThread::~SchedulerThread() {
    delete scheduler;
  }


  /**
   * InitScheduler completes the handshake with all other entities in the system.
   */
  void SchedulerThread::InitScheduler() {

    // we expect connections from all bg workers threads on all clients
    int32_t num_expected_conns = GlobalContext::get_num_total_bg_threads() +
      GlobalContext::get_num_total_aggregator_threads() +
      GlobalContext::get_num_total_server_threads() + GlobalContext::get_num_total_replica_threads();

    VLOG(10) << "Number of expected connections from workers="
             << GlobalContext::get_num_total_bg_threads();
    VLOG(10) << "Number of expected connections from aggregators="
             << GlobalContext::get_num_total_aggregator_threads();
    VLOG(10) << "Number of expected connections from servers="
             << GlobalContext::get_num_total_server_threads();
    VLOG(10) << "Number of expected connections from replicas="
             << GlobalContext::get_num_total_replica_threads();
    VLOG(10) << "Number of expected connections at scheduler=" << num_expected_conns;

    int32_t num_connections;
    for(num_connections = 0; num_connections < num_expected_conns; ++num_connections) {
      GetConnection();
    }

    CHECK_EQ(num_registered_workers_, GlobalContext::get_num_total_bg_threads());
    CHECK_EQ(num_registered_servers_, GlobalContext::get_num_total_server_threads());
    CHECK_EQ(num_registered_replicas_, GlobalContext::get_num_total_replica_threads());
    CHECK_EQ(num_registered_aggregators_, GlobalContext::get_num_total_aggregator_threads());

    scheduler->Init(server_ids_);

    ConnectServerMsg connect_server_msg;
    VLOG(5) << "Scheduler - send connect server to all bg threads";
    SendToAllBgThreads(reinterpret_cast<MsgBase*>(&connect_server_msg));

    ClientStartMsg client_start_msg;
    VLOG(5) << "Scheduler - send client start to all bg threads";
    SendToAllBgThreads(reinterpret_cast<MsgBase*>(&client_start_msg));

  }


  /**
   * Set up communication bus: Register the thread with comm_bus and use it for all further
   * communications.
   */
  void SchedulerThread::SetupCommBus() {
    CommBus::Config comm_config;
    comm_config.entity_id_ = my_id_;
    if(GlobalContext::get_num_clients() > 1) {
      comm_config.ltype_ = CommBus::kInProc | CommBus::kInterProc;
      HostInfo host_info = GlobalContext::get_scheduler_info();
      comm_config.network_addr_ = "*:" + host_info.port;
    } else {
      comm_config.ltype_ = CommBus::kInProc;
    }
    // register the server thread with the commbus. This, basically,
    // creates sockets for this thread, and updates some static variables
    // in comm_bus.
    comm_bus_->ThreadRegister(comm_config);
    std::cout << "The scheduler is up and running!" << std::endl;
  } // end function  -- set up comm bus


  /**
   * Receive a connection initiating message from all interacting entities.
   */
  int32_t SchedulerThread::GetConnection() {

    int32_t sender_id;
    zmq::message_t zmq_msg;
    (comm_bus_->*(comm_bus_->RecvAny_))(&sender_id, &zmq_msg);
    MsgType msg_type = MsgBase::get_msg_type(zmq_msg.data());

    if(msg_type == kConnect) {
      ConnectMsg msg(zmq_msg.data());
      int32_t entity_id = msg.get_entity_id();
      //VLOG(5) << "Receive connection from entity: " << entity_id;
      CHECK_EQ(sender_id, entity_id);

      EntityType entity_type = msg.get_entity_type();
      if(entity_type == petuum::SERVER) {
        server_ids_[num_registered_servers_++] = sender_id;
        VLOG(5) << "Receive connection from server: " << entity_id;
      } else if(entity_type == petuum::WORKER) {
        bg_worker_ids_[num_registered_workers_++] = sender_id;
        VLOG(5) << "Receive connection from worker: " << entity_id;
      } else if(entity_type == petuum::AGGREGATOR) {
        num_registered_aggregators_++;
        VLOG(5) << "Receive connection from aggregator: " << entity_id;
      } else if(entity_type == petuum::REPLICA) {
        num_registered_replicas_++;
        VLOG(5) << "Receive connection from replica: " << entity_id;
      } else {
        LOG(FATAL) << "Unknown type of connect message type. msg_type=" << entity_type
                   << " sender_id=" << sender_id;
      }
    } else {
      LOG(FATAL) << "Unknown type of message. Expected kConnect. Received=" << msg_type
                 << " sender_id=" << sender_id;
    }
    return sender_id;
  } // end function -- get connection


  /*
   * SendToAllBgThreads: A utility function to broadcast message to all background threads.
   */
  void SchedulerThread::SendToAllBgThreads(MsgBase *msg) {
    for(const auto &bg_id : bg_worker_ids_) {
      size_t sent_size = (comm_bus_->*(comm_bus_->SendAny_))(bg_id, msg->get_mem(), msg->get_size());
      CHECK_EQ(sent_size, msg->get_size());
    }
  }


  /**
     Helper function to transfer message to any destination.
  */
  size_t SchedulerThread::SendMsg(int32_t destination_id, MsgBase *msg) {
    size_t sent_size = (comm_bus_->*(comm_bus_->SendAny_))(destination_id, msg->get_mem(), msg->get_size());
    CHECK_EQ(sent_size, msg->get_size());
    return sent_size;
  }


  /**
   * Transfer request are send from individual worker threads. We buffer them an
   * release them in order.
   */
  bool SchedulerThread::HandleTransferRequest(int32_t bg_id, TransferRequestMsg &request_msg) {

    int32_t unique_id = request_msg.get_unique_id();
    int32_t server_id = request_msg.get_server_id();
    int32_t client_version = request_msg.get_gradient_version();
    int32_t gradient_size = request_msg.get_gradient_size();
    //int32_t queueing_key = GetQueueingKey(server_id);

    VLOG(2) << "GET transfer request "
            << " sender_id=" << bg_id
            << " unique_id=" << unique_id
            << " server_id=" << server_id
            << " size=" << gradient_size
            << " update_version=" << client_version
            ;

    scheduler->UpdateTransferRequest(bg_id, server_id, gradient_size, client_version, unique_id);

    VLOG(20) << "Check if update freed up responses. has_response=" << scheduler->HasResponse(server_id);
    while(scheduler->HasResponse(server_id)) {
      ClientRequestAggregate requests = scheduler->GetResponse(server_id);
      for(uint i = 0; i < requests.Count(); i++) {
        TransferResponseMsg msg;
        msg.get_destination_id() = requests.GetServerID(i);
        msg.get_unique_id() = requests.GetUniqueID(i);
        msg.get_transmission_rate() = 1000000000;
        int32_t worker_id = requests.GetWorkerID(i);
        SendMsg(worker_id, &msg);
        VLOG(2) << "RELEASE transfer response "
                << " worker_id=" << worker_id
                << " unique_id=" << requests.GetUniqueID(i)
                << " server_id=" <<  requests.GetServerID(i);
      }
    }

    while(scheduler->HasDiscardResponse()) {
      // for now we never enter this loop
    }

    if(version_counter_.find(server_id) == version_counter_.end()) {
      version_counter_[server_id] = 0; // init to zero
    }

    return false;
  }


  /**
   * Respond to completion of transfer and corresponding notification from the server.
   */
  bool SchedulerThread::HandleTransferDelivered(int32_t server_id, TransferDeliveredMsg &delivered_msg) {

    int32_t worker_id = delivered_msg.get_worker_id();
    int32_t msg_server_id = delivered_msg.get_server_id();
    CHECK_EQ(msg_server_id, server_id);
    int32_t unique_id = delivered_msg.get_unique_id();
    VLOG(2) << "ACK transfer complete " << " worker_id=" << worker_id
            << " server_id=" << server_id << " unique_id=" << unique_id;

    scheduler->UpdateTransferComplete(worker_id, server_id, unique_id);

    while(scheduler->HasResponse(server_id)) {

      ClientRequestAggregate requests = scheduler->GetResponse(server_id);
      for(uint i = 0; i < requests.Count(); i++) {
        TransferResponseMsg msg;
        msg.get_destination_id() = requests.GetServerID(i);
        msg.get_unique_id() = requests.GetUniqueID(i);
        msg.get_transmission_rate() = 1000000000;
        int32_t worker_id = requests.GetWorkerID(i);
        SendMsg(worker_id, &msg);
        VLOG(2) << "RELEASE transfer response "
                << " worker_id=" << worker_id
                << " unique_id=" << requests.GetUniqueID(i)
                << " server_id=" <<  requests.GetServerID(i);
      }
    }

    return false;
  }


  /*
   * operator(): The entry point for the main function for all threads.
   */
  void *SchedulerThread::operator() () {
    ThreadContext::RegisterThread(my_id_);

    SetupCommBus();
    // one this location has been hit, the thread that initialized the scheduler
    // thread can proceed. this ensure, that comm_bus is set up after the thread
    // has been created.
    pthread_barrier_wait(init_barrier_);
    // this function waits till all background threads have sent their request
    // to connect. it also responds to each background thread with a 'OK'
    // response.
    InitScheduler();

    zmq::message_t zmq_msg;
    int32_t sender_id;

    // poll, for new messages
    while(1) {

        // recv a packet.
        (comm_bus_->*(comm_bus_->RecvAny_))(&sender_id, &zmq_msg);
        MsgType msg_type = MsgBase::get_msg_type(zmq_msg.data());

        switch(msg_type) {
        case kTransferRequest:
          {
            TransferRequestMsg transfer_request_msg(zmq_msg.data());
            HandleTransferRequest(sender_id, transfer_request_msg);
            break;
          }
        case kTransferDelivered:
          {
            TransferDeliveredMsg delivered_msg(zmq_msg.data());
            HandleTransferDelivered(sender_id, delivered_msg);
          }
          break;
        default:
              LOG(FATAL) << "Unrecognized message type " << msg_type
                  << " sender = " << sender_id;
        } // end switch
    } // end while -- infinite loop
  } // end function -- operator

} // end namespace -- petuum
