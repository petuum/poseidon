// name_node_thread.hpp
// author: jinliang

#pragma once

#include <vector>
#include <pthread.h>
#include <petuum_ps/util/pthread_barrier.hpp>
#include <queue>

#include <petuum_ps/util/thread.hpp>
#include <petuum_ps/server/server.hpp>
#include <petuum_ps/thread/ps_msgs.hpp>
#include <petuum_ps/comm_bus/comm_bus.hpp>

namespace petuum {

  class NameNodeThread : public Thread{
  public:
    NameNodeThread(pthread_barrier_t *init_barrier);
    ~NameNodeThread() { }
    virtual void *operator() ();
    virtual void ShutDown() {Join();}

  private:

    /**
       Internal structure used to keep track of table creation in all servers.
     */
    struct CreateTableInfo {
      int32_t num_clients_replied_;
      int32_t num_servers_replied_;
      std::queue<int32_t> bgs_to_reply_;

      CreateTableInfo():
        num_clients_replied_(0),
        num_servers_replied_(0),
        bgs_to_reply_(){}

      ~CreateTableInfo(){}

      CreateTableInfo & operator= (const CreateTableInfo& info_obj){
        num_clients_replied_ = info_obj.num_clients_replied_;
        num_servers_replied_ = info_obj.num_servers_replied_;
        bgs_to_reply_ = info_obj.bgs_to_reply_;
        return *this;
      }

      bool ReceivedFromAllServers() const {
        return (num_servers_replied_ == GlobalContext::get_num_total_server_threads()
                + GlobalContext::get_num_total_aggregator_threads());
      }

      bool RepliedToAllClients() const {
        return (num_clients_replied_ == GlobalContext::get_num_worker_clients());
      }
    }; // end structure -- create table info

    // utility communication functions
    int32_t GetConnection();
    void SendToAllServers(MsgBase *msg);
    void SendToAllAggregators(MsgBase *msg);
    void SendToAllBgThreads(MsgBase *msg);

    //
    void SetUpNameNodeContext();
    void SetUpCommBus();
    void InitNameNode();

    // helper functions to handlers
    bool HaveCreatedAllTables();
    void SendCreatedAllTablesMsg();

    // handlers
    bool HandleShutDownMsg();
    void HandleCreateTable(int32_t sender_id, CreateTableMsg &create_table_msg);
    void HandleCreateTableReply(CreateTableReplyMsg &create_table_reply_msg);

    int32_t my_id_;
    pthread_barrier_t *init_barrier_;

    CommBus *comm_bus_;
    Server server_obj_;


    // one bg per client is refered to as head bg
    std::vector<int32_t> bg_worker_ids_;
    std::vector<int32_t> server_ids_;
    std::vector<int32_t> aggregator_ids_;
    std::map<int32_t, CreateTableInfo> create_table_map_;

    // private variables used in init
    int32_t num_registered_servers_;
    int32_t num_registered_replicas_;
    int32_t num_registered_workers_;
    int32_t num_registered_aggregators_;

    // private variables used during shutdown
    int32_t num_shutdown_bgs_;
  };
}
