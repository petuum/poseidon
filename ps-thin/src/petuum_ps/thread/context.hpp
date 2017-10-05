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

/**
 * @brief Class that maintains id and clock for each application thread.
 *
 * In petuum PS, thread is treated as first-class citizen. Some globaly
 * shared thread information, such as ID, are stored in static variable to
 * avoid having passing some variables every where.
 */
class ThreadContext {

public:
  static void RegisterThread(int32_t thread_id) {
    thr_info_ = new Info(thread_id);
  }

  static int32_t get_id() { return thr_info_->entity_id_; }

  static int32_t get_clock() { return thr_info_->clock_; }

  static void Clock() { ++(thr_info_->clock_); }

  static int32_t GetCachedSystemClock() {
    return thr_info_->cached_system_clock_;
  }

  static void SetCachedSystemClock(int32_t system_clock) {
    thr_info_->cached_system_clock_ = system_clock;
  }

private:
  struct Info : boost::noncopyable {
    explicit Info(int32_t entity_id)
        : entity_id_(entity_id), clock_(0), cached_system_clock_(0) {}

    ~Info() {}

    const int32_t entity_id_;
    int32_t clock_;
    int32_t cached_system_clock_;
  };

  // We do not use thread_local here because there's a bug in
  // g++ 4.8.1 or lower: https://gcc.gnu.org/bugzilla/show_bug.cgi?id=55800
  static __thread Info *thr_info_;
};

/**
 * @brief Class that stores context information accessible by all threads
 * (application, worker, server) threads.
 *
 * Init function must have "happens-before" relation with all other functions.
 * After Init(), accesses to all other functions are concurrent.
 */
class GlobalContext : boost::noncopyable {

public:
  /**
   * Get the smallest id that can be assigned to any thread in this client.
   * This includes all application, worker, server, replica threads.
   */
  static int32_t get_thread_id_min(int32_t client_id) {
    return client_id * kMaxNumThreadsPerClient;
  }

  /**
   * Get the largest id that can be assigned to any thread in this client.
   * This includes all application, worker, server, replica threads.
   */
  static int32_t get_thread_id_max(int32_t client_id) {
    return (client_id + 1) * kMaxNumThreadsPerClient - 1;
  }

  /**
   * @brief The thread id of the name node.
   */
  static int32_t get_name_node_id() { return 0; }

  /**
   * @brief The client id of the name node.
   */
  static int32_t get_name_node_client_id() { return kNameNodeClientId; }

  /**
   * @brief The comm_channel_idx ^th background worker thread for a client.
   */
  static int32_t get_bg_thread_id(int32_t client_id, int32_t comm_channel_idx) {
    return get_thread_id_min(client_id) + kBgThreadIDStartOffset +
           comm_channel_idx;
  }

  /**
   * @brief The 0^th background worker thread for a client.
   */
  static int32_t get_head_bg_id(int32_t client_id) {
    // the bg thread with index=0 is the head bg
    return get_bg_thread_id(client_id, 0);
  }

  /**
   * @brief The comm_channel_idx ^th server thread for a client
   */
  static int32_t get_server_thread_id(int32_t client_id,
                                      int32_t comm_channel_idx) {
    return get_thread_id_min(client_id) + kServerThreadIDStartOffset +
           comm_channel_idx;
  }

  /**
   * @brief Return the client id based on the thread id.
   */
  static int32_t thread_id_to_client_id(int32_t thread_id) {
    return thread_id / kMaxNumThreadsPerClient;
  }

  static int32_t get_serialized_table_separator() { return -1; }

  static int32_t get_serialized_table_end() { return -2; }

  /**
   * @brief Check if a given client is a server
   */
  static bool is_server_client(int32_t client_id) {
    return client_id >= kServerClientMinId && client_id <= kServerClientMaxId;
  }

  /**
   * @brief Check if a given client is a worker
   */
  static bool is_worker_client(int32_t client_id) {
    return client_id >= kWorkerClientMinId && client_id <= kWorkerClientMaxId;
  }

  /**
   * @brief Initialize the GlobalContext with values from table config.
   */
  static inline void Init(const TableGroupConfig &table_group_config,
                          bool table_access) {
    table_group_config_ = table_group_config;
    num_table_threads_ = table_access
                             ? table_group_config_.num_local_app_threads
                             : table_group_config_.num_local_app_threads - 1;
    InitClientsAndThreads(table_group_config.host_map);
  }

  /**
   * @brief Initialize global data structures that identify servers,
   * workers, aggregators, etc.
   */
  static inline void
  InitClientsAndThreads(const std::map<int32_t, HostInfo> &host_map) {
    // process host map information
    for (auto host_iter = host_map.begin(); host_iter != host_map.end();
         ++host_iter) {
      if (is_server_client(host_iter->first)) {
        server_clients_.push_back(host_iter->first);
      }
      if (is_worker_client(host_iter->first)) {
        worker_clients_.push_back(host_iter->first);
      }
      HostInfo host_info = host_iter->second;
      // the base port num to use for the host
      int port_num = std::stoi(host_info.port, 0, 10);
      // update name node host info
      if (host_iter->first == get_name_node_client_id()) {
        name_node_host_info_ = host_info;
        // increment the aval. port number for the client
        ++port_num;
        std::stringstream ss;
        ss << port_num;
        host_info.port = ss.str();
      }
      // to populate server ids info
      if (is_server_client(host_iter->first)) {
        // update server host info
        for (int i = 0; i < get_num_comm_channels_per_client(); ++i) {
          int32_t server_id = get_server_thread_id(host_iter->first, i);
          server_map_.insert(std::make_pair(server_id, host_info));
          ++port_num;
          std::stringstream ss;
          ss << port_num;
          host_info.port = ss.str();
          server_ids_.push_back(server_id);
        }
      }
    }
  }

  /**
   * @brief Is the current client a name node.
   */
  static bool am_i_name_node_client() {
    return (get_client_id() == get_name_node_client_id());
  }

  /**
   * @brief Is the current client a server
   */
  static bool am_i_server_client() {
    return true;
    // return is_server_client(client_id_);
  }

  /**
   * @brief Is the current client a worker
   */
  static bool am_i_worker_client() {
    return true;
    // return is_worker_client(client_id_);
  }

  /**
   * @brief Get the server thread ids on a particular channel across all
   * clients. Each worker thread (bg worker) will connect to server threads on
   * its own channel.
   */
  static void GetServerThreadIDs(int32_t comm_channel_idx,
                                 std::vector<int32_t> *server_thread_ids) {
    (*server_thread_ids).clear();
    for (auto server_client_id : server_clients_) {
      (*server_thread_ids)
          .push_back(get_server_thread_id(server_client_id, comm_channel_idx));
    }
  }

  /**
   * @brief Get number of bg worker, server threads to use in each client.
   * The number of bg worker or server threads is the number of communication
   * channels.
   */
  static inline int32_t get_num_comm_channels_per_client() {
    return table_group_config_.num_comm_channels_per_client;
  }

  /**
   * @brief The number of application threads (includes the main thread and all
   * the other threads that that main thread spawned).
   */
  static inline int32_t get_num_app_threads() {
    return table_group_config_.num_local_app_threads;
  }

  /**
   * @brief Get the total number of background threads across all the clients.
   * It is used by name node thread to determine the number of connect messages
   * to expect from background worker threads.
   */
  static inline int32_t get_num_total_bg_threads() {
    return get_num_comm_channels_per_client() * get_num_worker_clients();
  }

  /**
   * @brief Get the total number of server threads across all the clients.
   * It is used by name node thread to determine the number of connect messages
   * to expect from server threads.
   */
  static inline int32_t get_num_total_server_threads() {
    return server_ids_.size();
  }

  /**
   * @brief Total number of application threads that needs table access
   * num_app_threads = num_table_threads or num_app_threads
   * = num_table_threads + 1
   */
  static inline int32_t get_num_table_threads() { return num_table_threads_; }

  static inline int32_t get_head_table_thread_id() {
    int32_t init_thread_id =
        get_thread_id_min(get_client_id()) + kInitThreadIDOffset;
    return (get_num_table_threads() == get_num_app_threads())
               ? init_thread_id
               : init_thread_id + 1;
  }

  static inline int32_t get_head_ephemeral_thread_id() {
    return get_thread_id_min(get_client_id()) + kEphemeralThreadIIDStartOffset;
  }

  /**
   * @brief Get the number of tables.
   */
  static inline int32_t get_num_tables() {
    return table_group_config_.num_tables;
  }

  /**
   * @brief Get total number of clients.
   */
  static inline int32_t get_num_clients() {
    return table_group_config_.num_total_clients;
  }

  /**
   * @brief Get the total number of clients that store and serve models.
   */
  static inline int32_t get_num_server_clients() {
    return server_clients_.size();
  }

  /**
   * @brief Get the number of clients that act as workers.
   */
  static inline int32_t get_num_worker_clients() {
    return worker_clients_.size();
  }

  /**
   * @brief Get the address information for a particular server id
   */
  static HostInfo get_server_info(int32_t server_id) {
    std::map<int32_t, HostInfo>::const_iterator iter =
        server_map_.find(server_id);
    CHECK(iter != server_map_.end()) << "id not found " << server_id;
    return iter->second;
  }

  /**
   * @brief Get the address of the name node
   */
  static HostInfo get_name_node_info() { return name_node_host_info_; }

  /**
   * @brief Get ids of all servers across all the clients.
   */
  static const std::vector<int32_t> &get_all_server_ids() {
    return server_ids_;
  }

  /**
   * @brief Get ids of all the clients that have worker threads running.
   */
  static const std::vector<int32_t> &get_worker_client_ids() {
    return worker_clients_;
  }

  /**
   * @brief Get ids of all the clients that have server threads running.
   */
  static const std::vector<int32_t> &get_server_client_ids() {
    return server_clients_;
  }

  /**
   * @brief Given a client get the index of the client among all the worker
   * clients.
   */
  static int32_t get_worker_client_index(int client_id) {
    int32_t num_worker_clients = get_num_worker_clients();
    for (int32_t index = 0; index < num_worker_clients; index++) {
      if (client_id == worker_clients_[index]) {
        return index;
      }
    }
    return num_worker_clients;
  }

  /**
   * @brief Get id of the current client.
   */
  static int32_t get_client_id() { return table_group_config_.client_id; }

  /**
   * @brief Get the channel index that should handle the given row.
   */
  static int32_t GetPartitionCommChannelIndex(int32_t row_id) {
    return row_id % get_num_comm_channels_per_client();
  }

  static int32_t GetPartitionServerID(int32_t row_id,
                                      int32_t comm_channel_idx) {
    int32_t server_client_id =
        GetPartitionServerClientID(row_id); // use a private helper function
    return get_server_thread_id(server_client_id, comm_channel_idx);
  }

  /**
   * @brief Get the index of the server thread.
   */
  static int32_t GetCommChannelIndexServer(int32_t server_id) {
    int32_t index =
        server_id % kMaxNumThreadsPerClient - kServerThreadIDStartOffset;
    return index;
  }

  /**
   * @brief the consistency model used for SGD
   */
  static ConsistencyModel get_consistency_model() {
    return table_group_config_.consistency_model;
  }

  /**
   * @brief Get the smallest thread id for the current client.
   */
  static int32_t get_local_id_min() {
    return get_thread_id_min(get_client_id());
  }

  /**
   * @brief Get the largest thread id for the current client.
   */
  static int32_t get_local_id_max() {
    return get_thread_id_max(get_client_id());
  }

  /**
   * @brief Determines if the worker and the server threads should poll
   * continuously.
   */
  static bool get_aggressive_cpu() {
    return table_group_config_.aggressive_cpu;
  }

  // # locks in a StripedLock pool.
  static int32_t GetLockPoolSize() {
    static const int32_t kStripedLockExpansionFactor = 20;
    return (get_num_app_threads() + get_num_comm_channels_per_client()) *
           kStripedLockExpansionFactor;
  }

  static int32_t GetLockPoolSize(size_t table_capacity) {
    static const int32_t kStripedLockReductionFactor = 1;
    return (table_capacity <= 2 * kStripedLockReductionFactor)
               ? table_capacity
               : table_capacity / kStripedLockReductionFactor;
  }

  /**
   * @brief Get the clock of the snapshot (used when recovering from snapshot)
   */
  static int32_t get_snapshot_clock() {
    return table_group_config_.snapshot_clock;
  }

  /**
   * @brief Get the directory where snapshots are stored
   */
  static const std::string &get_snapshot_dir() {
    return table_group_config_.snapshot_dir;
  }

  /**
   * @brief Get the clock to resume from
   */
  static int32_t get_resume_clock() { return table_group_config_.resume_clock; }

  /**
   * @brief Get the location of the model to resume from
   * TODO(raajay) what is the diff between snapshot and resume dir
   */
  static const std::string &get_resume_dir() {
    return table_group_config_.resume_dir;
  }

  /**
   * @brief Get the order (policy) in which updates are sorted
   * TODO(raajay) check if we have to delete this?
   */
  static UpdateSortPolicy get_update_sort_policy() {
    return table_group_config_.update_sort_policy;
  }

  /**
   * @brief The polling wait time for the background worker thread.
   * TODO(raajay) check if we have to delete this?
   */
  static long get_bg_idle_milli() { return table_group_config_.bg_idle_milli; }

  /**
   * @brief Get the bandwidth available on the NIC
   * TODO(raajay) Check where is this used and how
   */
  static double get_bandwidth_mbps() {
    return table_group_config_.bandwidth_mbps;
  }

  /**
   * @brief The polling wiat time for server thread
   */
  static long get_server_idle_milli() {
    return table_group_config_.server_idle_milli;
  }

  /**
   * @brief Check if the servers should update model asynchronously.
   * TODO(raajay) rename this to is_server_asynchronous
   */
  static bool is_asynchronous_mode() {
    return table_group_config_.is_asynchronous_mode;
  }

  /**
   * @brief Check if we need pre-table clock or one clock for all tables
   */
  static bool use_table_clock() { return table_group_config_.use_table_clock; }

  static CommBus *comm_bus;

  // name node thread id - 0
  // server thread ids - 1~99
  // bg thread ids - 101~199
  // init thread id - 200
  // app threads - 201~899
  // aggregator thread >500

  static const int32_t kMaxClientsOfAType = 100;
  static const int32_t kMaxNumThreadsPerClient = 1000;
  // num of server + name node threads per node <= 100
  static const int32_t kServerThreadIDStartOffset = 1;
  static const int32_t kBgThreadIDStartOffset = 101;
  static const int32_t kInitThreadIDOffset = 200;
  static const int32_t kAggregatorThreadIDStartOffset = 301; // 301 - 400
  static const int32_t kReplicaThreadIDStartOffset = 401;    // 401 - 500
  static const int32_t kEphemeralThreadIIDStartOffset = 501;

  static const int32_t kNameNodeClientId = 0;
  static const int32_t kServerClientMinId = 0;
  static const int32_t kServerClientMaxId = 1000;
  static const int32_t kWorkerClientMinId = 0;
  static const int32_t kWorkerClientMaxId = 1000;
  static const int32_t kAggregatorClientMinId = 201;
  static const int32_t kAggregatorClientMaxId = 300;
  static const int32_t kReplicaClientMinId = 301;
  static const int32_t kReplicaClientMaxId = 400;

private:
  static int32_t num_table_threads_;
  static TableGroupConfig table_group_config_;

  static HostInfo name_node_host_info_;
  static std::map<int32_t, HostInfo> server_map_;

  static std::vector<int32_t> server_ids_;
  static std::vector<int32_t> server_clients_;
  static std::vector<int32_t> worker_clients_;

  /**
   * @brief Return the server id responsible for a row
   */
  static int32_t GetPartitionServerClientID(int32_t row_id) {
    int index =
        (row_id / get_num_comm_channels_per_client()) % server_clients_.size();
    return server_clients_[index];
  }
};
}
