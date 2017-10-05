// author: jinliang
// author: raajay

#pragma once

#include <vector>
#include <map>
#include <glog/logging.h>
#include <boost/utility.hpp>

#include <petuum_ps/include/host_info.hpp>
#include <petuum_ps/include/abstract_row.hpp>
#include <petuum_ps/comm_bus/comm_bus.hpp>
#include <petuum_ps/include/configs.hpp>
#include <petuum_ps/util/vector_clock_mt.hpp>

namespace petuum {

  // In petuum PS, thread is treated as first-class citizen. Some globaly
  // shared thread information, such as ID, are stored in static variable to
  // avoid having passing some variables every where.

  class ThreadContext {
  public:
    static void RegisterThread(int32_t thread_id) {
      thr_info_ = new Info(thread_id);
    }

    static int32_t get_id() {
      return thr_info_->entity_id_;
    }

    static int32_t get_clock() {
      return thr_info_->clock_;
    }

    static void Clock() {
      ++(thr_info_->clock_);
    }

    static int32_t GetCachedSystemClock() {
      return thr_info_->cached_system_clock_;
    }

    static void SetCachedSystemClock(int32_t system_clock) {
      thr_info_->cached_system_clock_ = system_clock;
    }

  private:
    struct Info : boost::noncopyable {
      explicit Info(int32_t entity_id):
        entity_id_(entity_id),
        clock_(0),
        cached_system_clock_(0) { }

      ~Info(){ }

      const int32_t entity_id_;
      int32_t clock_;
      int32_t cached_system_clock_;
    };

    // We do not use thread_local here because there's a bug in
    // g++ 4.8.1 or lower: https://gcc.gnu.org/bugzilla/show_bug.cgi?id=55800
    static __thread Info *thr_info_;

  }; // class ThreadContext


  // Init function must have "happens-before" relation with all other functions.
  // After Init(), accesses to all other functions are concurrent.
  class GlobalContext : boost::noncopyable {
  public:

    // ************** START -- Functions that DO NOT depend on Init()

    static int32_t get_thread_id_min(int32_t client_id) {
      return client_id * kMaxNumThreadsPerClient;
    }

    static int32_t get_thread_id_max(int32_t client_id) {
      return (client_id + 1) * kMaxNumThreadsPerClient - 1;
    }

    static int32_t get_name_node_id() {
      return 0;
    }

    static int32_t get_name_node_client_id() {
      return kNameNodeClientId;
    }

    static int32_t get_scheduler_id() {
      return kSchedulerThreadIDOffset;
    }

    static int32_t get_scheduler_client_id() {
      return kSchedulerClientId;
    }

    static int32_t get_bg_thread_id(int32_t client_id, int32_t comm_channel_idx) {
      return get_thread_id_min(client_id) + kBgThreadIDStartOffset + comm_channel_idx;
    }

    static int32_t get_head_bg_id(int32_t client_id) {
      // the bg thread with index=0 is the head bg
      return get_bg_thread_id(client_id, 0);
    }

    static int32_t get_server_thread_id(int32_t client_id, int32_t comm_channel_idx) {
      return get_thread_id_min(client_id) + kServerThreadIDStartOffset + comm_channel_idx;
    }

    static int32_t get_aggregator_thread_id(int32_t client_id, int32_t comm_channel_idx) {
      return get_thread_id_min(client_id) + kAggregatorThreadIDStartOffset + comm_channel_idx;
    }

    static int32_t get_replica_thread_id(int32_t client_id, int32_t comm_channel_idx) {
      return get_thread_id_min(client_id) + kReplicaThreadIDStartOffset + comm_channel_idx;
    }

    static int32_t thread_id_to_client_id(int32_t thread_id) {
      return thread_id / kMaxNumThreadsPerClient;
    }

    static int32_t get_serialized_table_separator() {
      return -1;
    }

    static int32_t get_serialized_table_end() {
      return -2;
    }

    static int32_t is_server_client(int32_t client_id) {
      return client_id >= kServerClientMinId && client_id <= kServerClientMaxId;
    }

     static int32_t is_worker_client(int32_t client_id) {
      return client_id >= kWorkerClientMinId && client_id <= kWorkerClientMaxId;
     }

     static int32_t is_aggregator_client(int32_t client_id) {
      return client_id >= kAggregatorClientMinId && client_id <= kAggregatorClientMaxId;
     }

    static int32_t is_replica_client(int32_t client_id) {
      return client_id >= kReplicaClientMinId && client_id <= kReplicaClientMaxId;
    }

    static int32_t get_replica_for_server(int32_t server_id) {
      int32_t server_client = thread_id_to_client_id(server_id);
      int32_t comm_channel_idx = server_id - get_thread_id_min(server_client) - kServerThreadIDStartOffset;
      int32_t replica_client = kReplicaClientMinId - 1 + (server_client % kMaxClientsOfAType);
      return get_replica_thread_id(replica_client, comm_channel_idx);
    }

    static int32_t get_server_for_replica(int32_t replica_id) {
      int32_t replica_client = thread_id_to_client_id(replica_id);
      int32_t comm_channel_idx = replica_id - get_thread_id_min(replica_client) - kReplicaThreadIDStartOffset;
      int32_t server_client = kServerClientMinId - 1 + (replica_client % kMaxClientsOfAType);
      return get_server_thread_id(server_client, comm_channel_idx);
    }

    static bool use_replication() {
      return use_replication_;
    }

    static void set_replication(bool state) {
      use_replication_ = state;
    }

    // ************** END -- Functions that DO NOT depend on Init()


    // "server" is different from name node.
    // Name node is not considered as server.
    static inline void Init(
                            int32_t num_comm_channels_per_client,
                            int32_t num_app_threads,
                            int32_t num_table_threads,
                            int32_t num_tables,
                            int32_t num_clients,
                            const std::map<int32_t, HostInfo> &host_map,
                            int32_t client_id,
                            int32_t server_ring_size,
                            ConsistencyModel consistency_model,
                            bool aggressive_cpu,
                            int32_t snapshot_clock,
                            const std::string &snapshot_dir,
                            int32_t resume_clock,
                            const std::string &resume_dir,
                            UpdateSortPolicy update_sort_policy,
                            long bg_idle_milli,
                            double bandwidth_mbps,
                            size_t oplog_push_upper_bound_kb,
                            int32_t oplog_push_staleness_tolerance,
                            size_t thread_oplog_batch_size,
                            size_t server_push_row_threshold,
                            long server_idle_milli,
                            int32_t server_row_candidate_factor) {

      num_comm_channels_per_client_ = num_comm_channels_per_client;
      num_total_comm_channels_ = num_comm_channels_per_client*num_clients;
      num_app_threads_ = num_app_threads;
      num_table_threads_ = num_table_threads;
      num_tables_ = num_tables;
      num_clients_ = num_clients;
      host_map_ = host_map;
      client_id_ = client_id;
      server_ring_size_ = server_ring_size;
      consistency_model_ = consistency_model;
      local_id_min_ = get_thread_id_min(client_id);
      aggressive_cpu_ = aggressive_cpu;
      snapshot_clock_ = snapshot_clock;
      snapshot_dir_ = snapshot_dir;
      resume_clock_ = resume_clock;
      resume_dir_ = resume_dir;
      update_sort_policy_ = update_sort_policy;
      bg_idle_milli_ = bg_idle_milli;
      bandwidth_mbps_ = bandwidth_mbps;
      oplog_push_upper_bound_kb_ = oplog_push_upper_bound_kb;
      oplog_push_staleness_tolerance_ = oplog_push_staleness_tolerance;
      thread_oplog_batch_size_ = thread_oplog_batch_size;
      server_push_row_threshold_ = server_push_row_threshold;
      server_idle_milli_ = server_idle_milli;
      server_row_candidate_factor_ = server_row_candidate_factor;

      // TODO make this a configurable parameter
      is_asynchronous_mode_  = true;
      use_replication_ = false;
      use_fabric_ = true;

      // process host map information
      for (auto host_iter = host_map.begin(); host_iter != host_map.end(); ++host_iter) {

        if(is_server_client(host_iter->first)) {
          server_clients_.push_back(host_iter->first);
        }

        if(is_worker_client(host_iter->first)) {
          worker_clients_.push_back(host_iter->first);
        }

        if(is_aggregator_client(host_iter->first)) {
          aggregator_clients_.push_back(host_iter->first);
        }

        HostInfo host_info = host_iter->second;

        // the base port num to use for the host
        int port_num = std::stoi(host_info.port, 0, 10);

        // update name node host info
        if (host_iter->first == get_name_node_client_id()) {
          name_node_host_info_ = host_info;
          ++port_num; // increment the port number that can be used for that client
          std::stringstream ss; ss << port_num;
          host_info.port = ss.str();
        }

        // update scheduler host info
        if(host_iter->first == get_scheduler_client_id()) {
          scheduler_host_info_ = host_info;
          ++port_num; // increment the port number that can be used for that client
          std::stringstream ss; ss << port_num;
          host_info.port = ss.str();
        }

        // to populate server and aggregator ids info

        if(is_server_client(host_iter->first)) {
          // update server host info
          for (int i = 0; i < num_comm_channels_per_client_; ++i) {
            int32_t server_id = get_server_thread_id(host_iter->first, i);
            server_map_.insert(std::make_pair(server_id, host_info));
            ++port_num;
            std::stringstream ss; ss << port_num;
            host_info.port = ss.str();
            server_ids_.push_back(server_id);
          } // end for -- over number of comm channels
        }

        if(is_aggregator_client(host_iter->first)) {
          // update aggregator host info
          for (int i = 0; i < num_comm_channels_per_client_; ++i) {
            int32_t aggregator_id = get_aggregator_thread_id(host_iter->first, i);
            aggregator_map_.insert(std::make_pair(aggregator_id, host_info));
            ++port_num;
            std::stringstream ss; ss << port_num;
            host_info.port = ss.str();
            aggregator_ids_.push_back(aggregator_id);
          } // end for -- over number of comm channels
        }

        if(is_replica_client(host_iter->first)) {
          // update replica host info
          for (int i = 0; i < num_comm_channels_per_client_; ++i) {
            int32_t replica_id = get_replica_thread_id(host_iter->first, i);
            replica_map_.insert(std::make_pair(replica_id, host_info));
            ++port_num;
            std::stringstream ss; ss << port_num;
            host_info.port = ss.str();
            replica_ids_.push_back(replica_id);
          } // end for -- over number of comm channels
        }


      } // end for -- over hosts
    } // end function -- Init()


    // ********* START - Functions that depend on Init()

    static bool am_i_name_node_client() {
      return (client_id_ == get_name_node_client_id());
    }

    static bool am_i_scheduler_client() {
      return (client_id_ == get_scheduler_client_id());
    }

    static bool am_i_server_client() {
      return is_server_client(client_id_);
    }

    static bool am_i_worker_client() {
      return is_worker_client(client_id_);
    }

    static bool am_i_aggregator_client() {
      return is_aggregator_client(client_id_);
    }

    static bool am_i_replica_client() {
      return is_replica_client(client_id_);
    }

    static size_t get_server_row_candidate_factor() {
      return server_row_candidate_factor_;
    }

    /**
     * Get the server thread ids on a particular channel across all clients.
     * Each BgWorker will connect to server threads on its own channel.
     */
    static void GetServerThreadIDs(int32_t comm_channel_idx, std::vector<int32_t> *server_thread_ids) {
      (*server_thread_ids).clear();
      for(auto server_client_id : server_clients_) {
        (*server_thread_ids).push_back(get_server_thread_id(server_client_id, comm_channel_idx));
      }
      // for (int32_t i = 0; i < num_clients_; ++i) {
      //   (*server_thread_ids).push_back(get_server_thread_id(i, comm_channel_idx));
      // }
    }

    static void GetAggregatorThreadIDs(int32_t comm_channel_idx, std::vector<int32_t> *aggregator_thread_ids) {
      (*aggregator_thread_ids).clear();
      for(auto server_client_id : aggregator_clients_) {
        (*aggregator_thread_ids).push_back(get_aggregator_thread_id(server_client_id, comm_channel_idx));
      }
    }

    static void GetReplicaThreadIDs(int32_t comm_channel_idx, std::vector<int32_t> *replica_thread_ids) {
      (*replica_thread_ids).clear();
      for(auto replica_client_id : replica_clients_) {
        (*replica_thread_ids).push_back(get_replica_thread_id(replica_client_id, comm_channel_idx));
      }
    }

    static inline int32_t get_num_comm_channels_per_client() {
      return num_comm_channels_per_client_;
    }

    // total number of application threads including init thread
    static inline int32_t get_num_app_threads() {
      return num_app_threads_;
    }

    static inline int32_t get_num_total_bg_threads() {
      return get_num_comm_channels_per_client() * get_num_worker_clients();
    }

    static inline int32_t get_num_total_server_threads() {
      return server_ids_.size();
    }

    static inline int32_t get_num_total_aggregator_threads() {
      return aggregator_ids_.size();
    }

    static inline int32_t get_num_total_replica_threads() {
      return replica_ids_.size();
    }

    // Total number of application threads that needs table access
    // num_app_threads = num_table_threads_ or num_app_threads_
    // = num_table_threads_ + 1
    static inline int32_t get_num_table_threads() {
      return num_table_threads_;
    }

    static inline int32_t get_head_table_thread_id() {
      int32_t init_thread_id = get_thread_id_min(client_id_) + kInitThreadIDOffset;
      return (num_table_threads_ == num_app_threads_) ? init_thread_id : init_thread_id + 1;
    }

    static inline int32_t get_num_tables() {
      return num_tables_;
    }

    static inline int32_t get_num_clients() {
      return num_clients_;
    }

    static inline int32_t get_num_server_clients() {
      return server_clients_.size();
    }

    static inline int32_t get_num_worker_clients() {
      return worker_clients_.size();
    }

    static inline int32_t get_num_aggregator_clients() {
      return aggregator_clients_.size();
    }

    static inline int32_t get_num_replica_clients() {
      return replica_clients_.size();
    }

    static HostInfo get_server_info(int32_t server_id) {
      std::map<int32_t, HostInfo>::const_iterator iter = server_map_.find(server_id);
      CHECK(iter != server_map_.end()) << "id not found " << server_id;
      return iter->second;
    }

    static HostInfo get_aggregator_info(int32_t aggregator_id) {
      std::map<int32_t, HostInfo>::const_iterator iter = aggregator_map_.find(aggregator_id);
      CHECK(iter != aggregator_map_.end()) << "id not found " << aggregator_id;
      return iter->second;
    }

    static HostInfo get_replica_info(int32_t replica_id) {
      std::map<int32_t, HostInfo>::const_iterator iter = replica_map_.find(replica_id);
      CHECK(iter != replica_map_.end()) << "id not found " << replica_id;
      return iter->second;
    }

    static HostInfo get_name_node_info() {
      return name_node_host_info_;
    }

    static HostInfo get_scheduler_info() {
      return scheduler_host_info_;
    }

    static HostInfo get_destination_info(int32_t entity_id) {
      if(entity_id == GlobalContext::get_name_node_id()) {
        return GlobalContext::get_name_node_info();

      } else if(entity_id == GlobalContext::get_scheduler_id()) {
        return GlobalContext::get_scheduler_info();

      } else {
        int32_t client_id = GlobalContext::thread_id_to_client_id(entity_id);

        if(GlobalContext::is_aggregator_client(client_id)) {
          return GlobalContext::get_aggregator_info(entity_id);

        } else if(GlobalContext::is_server_client(client_id)) {
          return GlobalContext::get_server_info(entity_id);

        } else if(GlobalContext::is_replica_client(client_id)) {
          return GlobalContext::get_replica_info(entity_id);

        } else {
          LOG(FATAL) << "Unknown type of destination. Entity: " << entity_id;
          return HostInfo();
        }
      }
    }

    static const std::vector<int32_t> &get_all_server_ids() {
      return server_ids_;
    }

    static const std::vector<int32_t> &get_all_aggregator_ids() {
      return aggregator_ids_;
    }

    static const std::vector<int32_t> &get_all_replica_ids() {
      return replica_ids_;
    }

    static const std::vector<int32_t> &get_worker_client_ids() {
      return worker_clients_;
    }

    static const std::vector<int32_t> &get_server_client_ids() {
      return server_clients_;
    }

    static int32_t get_worker_client_index(int client_id) {
      int32_t num_worker_clients = get_num_worker_clients();
      for(int32_t index = 0; index < num_worker_clients; index++) {
        if(client_id == worker_clients_[index]) {
          return index;
        }
      }
      return num_worker_clients;
    }

    static int32_t get_client_id() {
      return client_id_;
    }

    static int32_t GetPartitionCommChannelIndex(int32_t row_id) {
      return row_id % num_comm_channels_per_client_;
    }

    static int32_t GetPartitionServerID(int32_t row_id, int32_t comm_channel_idx) {
      int32_t server_client_id = GetPartitionServerClientID(row_id); // use a private helper function
      return get_server_thread_id(server_client_id, comm_channel_idx);
    }

    static int32_t GetCommChannelIndexServer(int32_t server_id) {
      int32_t index = server_id % kMaxNumThreadsPerClient - kServerThreadIDStartOffset;
      return index;
    }

    static int32_t get_server_ring_size(){
      return server_ring_size_;
    }

    static ConsistencyModel get_consistency_model(){
      return consistency_model_;
    }

    static int32_t get_local_id_min(){
      return local_id_min_;
    }

    static bool get_aggressive_cpu() {
      return aggressive_cpu_;
    }

    // # locks in a StripedLock pool.
    static int32_t GetLockPoolSize() {
      static const int32_t kStripedLockExpansionFactor = 20;
      return (num_app_threads_ + num_comm_channels_per_client_) * kStripedLockExpansionFactor;
    }

    static int32_t GetLockPoolSize(size_t table_capacity) {
      static const int32_t kStripedLockReductionFactor = 1;
      return (table_capacity <= 2*kStripedLockReductionFactor) ? table_capacity : table_capacity / kStripedLockReductionFactor;
    }

    static int32_t get_snapshot_clock() {
      return snapshot_clock_;
    }

    static const std::string &get_snapshot_dir() {
      return snapshot_dir_;
    }

    static int32_t get_resume_clock() {
      return resume_clock_;
    }

    static const std::string &get_resume_dir() {
      return resume_dir_;
    }

    static UpdateSortPolicy get_update_sort_policy() {
      return update_sort_policy_;
    }

    static long get_bg_idle_milli() {
      return bg_idle_milli_;
    }

    static double get_bandwidth_mbps() {
      return bandwidth_mbps_;
    }

    static size_t get_oplog_push_upper_bound_kb() {
      return oplog_push_upper_bound_kb_;
    }

    static int32_t get_oplog_push_staleness_tolerance() {
      return oplog_push_staleness_tolerance_;
    }

    static size_t get_thread_oplog_batch_size() {
      return thread_oplog_batch_size_;
    }

    static size_t get_server_push_row_threshold() {
      return server_push_row_threshold_;
    }

    static long get_server_idle_milli() {
      return server_idle_milli_;
    }

    static bool is_asynchronous_mode() {
      return is_asynchronous_mode_;
    }

    static void set_asynchronous(bool state) {
      is_asynchronous_mode_ = state;
    }

    // ********* END - Functions that depend on Init()


    static CommBus* comm_bus;

    // name node thread id - 0
    // server thread ids - 1~99
    // bg thread ids - 100~199
    // init thread id - 200
    // app threads - 201~899
    // scheduler thread - 900

    static const int32_t kMaxClientsOfAType = 100;
    static const int32_t kMaxNumThreadsPerClient = 1000;
    // num of server + name node threads per node <= 100
    static const int32_t kBgThreadIDStartOffset = 101;
    static const int32_t kInitThreadIDOffset = 200;
    static const int32_t kServerThreadIDStartOffset = 1;

    static const int32_t kSchedulerThreadIDOffset = 900;
    static const int32_t kAggregatorThreadIDStartOffset = 500; // 500 - 600
    static const int32_t kReplicaThreadIDStartOffset = 500; // 500 - 600

    static const int32_t kNameNodeClientId = 0;
    static const int32_t kSchedulerClientId = 0;
    static const int32_t kServerClientMinId = 1;
    static const int32_t kServerClientMaxId = 100;
    static const int32_t kWorkerClientMinId = 101;
    static const int32_t kWorkerClientMaxId = 200;
    static const int32_t kAggregatorClientMinId = 201;
    static const int32_t kAggregatorClientMaxId = 300;
    static const int32_t kReplicaClientMinId = 301;
    static const int32_t kReplicaClientMaxId = 400;


  private:
    // private functions
    // get the id of the server who is responsible for holding that row
    static int32_t GetPartitionServerClientID(int32_t row_id) {
      int index = (row_id / num_comm_channels_per_client_) % server_clients_.size();
      return server_clients_[index];
    }

    // private variables
    static int32_t client_id_;
    static int32_t num_clients_;
    static int32_t num_comm_channels_per_client_;
    static int32_t num_total_comm_channels_;
    static int32_t num_app_threads_;
    static int32_t num_table_threads_;
    static int32_t num_tables_;
    static int32_t server_ring_size_;
    static ConsistencyModel consistency_model_;
    static int32_t local_id_min_;
    static bool aggressive_cpu_;
    static int32_t snapshot_clock_;
    static std::string snapshot_dir_;
    static int32_t resume_clock_;
    static std::string resume_dir_;
    static UpdateSortPolicy update_sort_policy_;
    static long bg_idle_milli_;
    static double bandwidth_mbps_;
    static size_t oplog_push_upper_bound_kb_;
    static int32_t oplog_push_staleness_tolerance_;
    static size_t thread_oplog_batch_size_;
    static size_t server_oplog_push_batch_size_;
    static size_t server_push_row_threshold_;
    static long server_idle_milli_;
    static int32_t server_row_candidate_factor_;
    static std::map<int32_t, HostInfo> host_map_;
    static std::map<int32_t, HostInfo> server_map_;
    static std::map<int32_t, HostInfo> aggregator_map_;
    static std::map<int32_t, HostInfo> replica_map_;
    static HostInfo name_node_host_info_;
    static HostInfo scheduler_host_info_;
    static std::vector<int32_t> server_ids_;
    static std::vector<int32_t> aggregator_ids_;
    static std::vector<int32_t> replica_ids_;
    static std::vector<int32_t> server_clients_;
    static std::vector<int32_t> worker_clients_;
    static std::vector<int32_t> aggregator_clients_;
    static std::vector<int32_t> replica_clients_;

    // (Raajay) new private variables
    static bool is_asynchronous_mode_;
    static bool use_replication_;
    static bool use_fabric_;

  }; // class GlobalContext

}   // namespace petuum
