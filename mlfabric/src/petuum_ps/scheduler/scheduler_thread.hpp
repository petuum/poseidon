// scheduler_thread.hpp
// author: raajay

#pragma once

#include <pthread.h>
#include <petuum_ps/util/pthread_barrier.hpp>

#include <petuum_ps/util/thread.hpp>
#include <petuum_ps/scheduler/scheduler_utils.hpp>
#include <petuum_ps/scheduler/abstract_scheduler.hpp>
#include <petuum_ps/scheduler/delay_scheduler.hpp>
#include <petuum_ps/thread/ps_msgs.hpp>
#include <petuum_ps/comm_bus/comm_bus.hpp>
#include <boost/unordered_map.hpp>
#include <petuum_ps/thread/context.hpp>

namespace petuum {

  class SchedulerThread : public Thread {
  public:
    SchedulerThread(pthread_barrier_t *init_barrier);
    ~SchedulerThread();
    virtual void *operator() ();
    virtual void ShutDown() {
      Join();
    }

  protected:
    virtual void InitWhenStart() {}

  private:

    void InitScheduler();
    void SetupCommBus();

    // communication functions
    int32_t GetConnection();
    void SendToAllBgThreads(MsgBase* msg);
    size_t SendMsg(int32_t destination_id, MsgBase *msg);


    // communication functions
    bool HandleTransferRequest(int32_t bg_id, TransferRequestMsg &request_msg);
    bool HandleTransferDelivered(int32_t server_id, TransferDeliveredMsg &delivered_msg);

    int32_t GetNumQueued(int32_t nic_id) {
      if(storage_.find(nic_id) == storage_.end()) {
        return 0;
      } else {
        return storage_[nic_id].size();
      }
    }


    bool IsRequestQueued(int32_t nic_id) {
      if(storage_.find(nic_id) == storage_.end()) {
        return false;
      } else {
        if(storage_[nic_id].empty()) {
          return false;
        } else {
          return true;
        }
      }
    }


    int32_t GetServerNICId(int32_t server_client_id) {
      return 1;
    }


    int32_t GetQueueingKey(int32_t server_id) {
      if(false) {
        int32_t server_client_id = GlobalContext::thread_id_to_client_id(server_id);
        int32_t nic_id = GetServerNICId(server_client_id);
        return nic_id;
      } else {
        return server_id;
      }
    }

    // internal data structures
    int32_t my_id_;    // the id of the scheduler thread
    pthread_barrier_t *init_barrier_; // a barrier to set up comm_buss
    CommBus *comm_bus_;
    std::vector<int32_t> bg_worker_ids_;
    std::vector<int32_t> server_ids_;

    // these should be indexed by server client id as opposed to server id
    boost::unordered_map<int32_t, std::deque<StoredValue> > storage_;
    boost::unordered_map<int32_t, int32_t> version_counter_;
    boost::unordered_map<int32_t, int32_t> pending_;

    AbstractScheduler *scheduler;

    // private variables
    int32_t num_registered_servers_;
    int32_t num_registered_replicas_;
    int32_t num_registered_workers_;
    int32_t num_registered_aggregators_;
  };
}
