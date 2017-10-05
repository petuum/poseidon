#pragma once

#include <inttypes.h>
#include <stdint.h>
#include <map>
#include <vector>
#include <string>

#include <petuum_ps/include/host_info.hpp>
#include <petuum_ps/include/constants.hpp>

namespace petuum {

/**
 * @brief The consistency model to use for updating and fetching model
 * SSP: Stale Synchronous Parallel
 */
enum ConsistencyModel { SSP = 0, LocalOOC = 6 };

/**
 * @brief The policy used to sort the updates, i.e., how to sort the oplog
 * content.
 */
enum UpdateSortPolicy {
  FIFO = 0,
  Random = 1,
  RelativeMagnitude = 2,
  FIFO_N_ReMag = 3
};

/**
 * @brief The format of per row updates.
 */
struct RowOpLogType {
  static const int32_t kDenseRowOpLog = 0;
  static const int32_t kSparseRowOpLog = 1;
  static const int32_t kSparseVectorRowOpLog = 2;
};

/**
 * @brief OpLog stores the updates. If the updates can either be sparse or
 * dense. AppendOnly is a mystery!!!  TODO(raajay)
 */
enum OpLogType { Sparse = 0, AppendOnly = 1, Dense = 2 };

/**
 */
enum AppendOnlyOpLogType { Inc = 0, BatchInc = 1, DenseBatchInc = 2 };

/**
 * @brief ProcessStorage stores the current value of the model in the client
 * table. Bounded means that there is limited capacity (memory) to store the
 * model.
 */
enum ProcessStorageType { BoundedDense = 0, BoundedSparse = 1 };

/**
 * @brief Configuration for all tables in the group. Will be used to init the
 * global context.
 */
struct TableGroupConfig {

  /**
   * Constructor
   */
  TableGroupConfig()
      : stats_path(""), num_comm_channels_per_client(1), num_tables(1),
        num_total_clients(1), num_local_app_threads(2), aggressive_clock(false),
        aggressive_cpu(false), snapshot_clock(-1), resume_clock(-1),
        update_sort_policy(Random), bg_idle_milli(0), bandwidth_mbps(4000),
        oplog_push_upper_bound_kb(1000), oplog_push_staleness_tolerance(2),
        thread_oplog_batch_size(100 * 1000 * 1000),
        server_row_candidate_factor(5), use_table_clock(true) {}

  std::string stats_path;

  // ================= Global Parameters ===================
  // Global parameters have to be the same across all processes.

  /**
   * Total number of servers in the system.
   * Global parameters have to be the same across all processes.
   */
  int32_t num_comm_channels_per_client;

  /**
   * Total number of tables the PS will have. Each init thread must make
   * num_tables CreateTable() calls.
   * Global parameters have to be the same across all processes.
   */
  int32_t num_tables;

  /**
   * Total number of clients in the system.
   * Global parameters have to be the same across all processes.
   */
  int32_t num_total_clients;

  // ===================== Local Parameters ===================
  // Local parameters can differ between processes, but have to sum up to global
  // parameters.

  /**
   * Number of local applications threads, including init thread.
   * Local parameters can differ between processes, but have to sum up to global
   * parameters.
   */
  int32_t num_local_app_threads;

  /**
   * mapping server ID to host info.
   */
  std::map<int32_t, HostInfo> host_map;

  /**
   * My client id.
   */
  int32_t client_id;

  /**
   * If set to true, oplog send is triggered on every Clock() call.
   * If set to false, oplog is only sent if the process clock (representing all
   * app threads) has advanced.
   * Aggressive clock may reduce memory footprint and improve the per-clock
   * convergence rate in the cost of performance.
   * Default is false (suggested).
   */
  bool aggressive_clock;

  /**
   * The type of consistency between workers. Currently, only support SSP
   */
  ConsistencyModel consistency_model;

  /**
   * Is the server synchronous or asynchronous?
   */
  bool is_asynchronous_mode;

  /**
   * Determines the wait time on polling?
   */
  int32_t aggressive_cpu;

  /**
   * In Async+pushing,
   */
  int32_t server_ring_size;

  int32_t snapshot_clock;

  int32_t resume_clock;

  std::string snapshot_dir;

  std::string resume_dir;

  std::string ooc_path_prefix;

  UpdateSortPolicy update_sort_policy;

  /**
   * In number of milliseconds.
   * If the bg thread wakes up and finds there's no work to do,
   * it goes back to sleep for this much time or until it receives
   * a message.
   */
  long bg_idle_milli;

  /**
   * Bandwidth in Megabits per second
   */
  double bandwidth_mbps;

  /**
   * upper bound on update message size in kilobytes
   */
  size_t oplog_push_upper_bound_kb;

  /**
   */
  int32_t oplog_push_staleness_tolerance;

  /**
   */
  size_t thread_oplog_batch_size;

  /**
   */
  size_t server_push_row_threshold;

  /**
   */
  long server_idle_milli;

  /**
   */
  long server_row_candidate_factor;

  /**
   * (raajay): Petuum traditionally uses one clock for an application thread (a
   * single clock will indicate updates to all tables have been applied).
   * However, on integrating with caffe, per-table sync threads are used to Inc
   * and Get table values. To avoid race conditions between App and Sync
   * threads, the clocks have to be issued from the sync threads after the Inc
   * completes. Hence we introduce per table clocks.
   *
   * At any given point in time, only one thread uses table API. Hence, we do
   * not need a multi-threaded clock for each table.
   */
  bool use_table_clock;

  /**
   * Stringify table group configs
   */
  std::string toString() {
    std::stringstream ss;
    ss << "TableGroupConfig:" << std::endl;
    ss << "  num_comm_channels_per_client: " << num_comm_channels_per_client
       << std::endl;
    ss << "  num_tables: " << num_tables << std::endl;
    ss << "  num_total_clients: " << num_total_clients << std::endl;
    ss << "  num_local_app_threads: " << num_local_app_threads << std::endl;
    ss << "  client_id: " << client_id << std::endl;
    ss << "  aggressive_clock: " << aggressive_clock << std::endl;
    ss << "  consistency_model: " << consistency_model << std::endl;
    ss << "  aggressive_cpu: " << aggressive_cpu << std::endl;
    ss << "  server_ring_size: " << server_ring_size << std::endl;
    ss << "  snapshot_clock: " << snapshot_clock << std::endl;
    ss << "  resume_clock: " << resume_clock << std::endl;
    ss << "  snapshot_dir: " << snapshot_dir << std::endl;
    ss << "  resume_dir: " << resume_dir << std::endl;
    ss << "  ooc_path_prefix: " << ooc_path_prefix << std::endl;
    ss << "  update_sort_policy: " << update_sort_policy << std::endl;
    ss << "  bg_idle_milli: " << bg_idle_milli << std::endl;
    ss << "  bandwidth_mbps: " << bandwidth_mbps << std::endl;
    ss << "  oplog_push_upper_bound_kb: " << oplog_push_upper_bound_kb
       << std::endl;
    ss << "  oplog_push_staleness_tolerance: " << oplog_push_staleness_tolerance
       << std::endl;
    ss << "  thread_oplog_batch_size: " << thread_oplog_batch_size << std::endl;
    ss << "  server_push_row_threshold: " << server_push_row_threshold
       << std::endl;
    ss << "  server_idle_milli: " << server_idle_milli << std::endl;
    ss << "  server_row_candidate_factor: " << server_row_candidate_factor
       << std::endl;
    return ss.str();
  }
};

/**
 * TableInfo is shared between client and server; i.e., its values are used for
 * creation of Client as well as Server table
 */
struct TableInfo {

  /**
   * @brief Constructor
   */

  TableInfo()
      : table_staleness(0), row_type(-1), row_capacity(0),
        oplog_dense_serialized(false), row_oplog_type(1),
        dense_row_oplog_capacity(0) {}

  /**
   * table_staleness is used for SSP and ClockVAP.
   */
  int32_t table_staleness;

  /**
   * A table can only have one type of row. The row_type is defined when
   * calling TableGroup::RegisterRow().
   */
  int32_t row_type;

  /**
   * row_capacity can mean different thing for different row_type. For example
   * in vector-backed dense row it is the max number of columns. This
   * parameter is ignored for sparse row.
   */
  size_t row_capacity;

  bool oplog_dense_serialized;

  int32_t row_oplog_type;

  size_t dense_row_oplog_capacity;

  std::string toString() {
    std::stringstream ss;
    ss << "TableInfo:" << std::endl;
    ss << "  table_staleness: " << table_staleness << std::endl;
    ss << "  row_type: " << row_type << std::endl;
    ss << "  row_capacity: " << row_capacity << std::endl;
    ss << "  row_capacity: " << row_capacity << std::endl;
    ss << "  oplog_dense_serialized: " << oplog_dense_serialized << std::endl;
    ss << "  row_oplog_type: " << row_oplog_type << std::endl;
    ss << "  dense_row_oplog_capacity: " << dense_row_oplog_capacity
       << std::endl;
    return ss.str();
  }
};

/**
 * @brief ClientTableConfig is used by client only.
 */
struct ClientTableConfig {

  /**
   * @brief Constructor
   */
  ClientTableConfig()
      : process_cache_capacity(0), thread_cache_capacity(1), oplog_capacity(0),
        oplog_type(Dense), append_only_oplog_type(Inc),
        append_only_buff_capacity(10 * k1_Mi),
        per_thread_append_only_buff_pool_size(3), bg_apply_append_oplog_freq(1),
        process_storage_type(BoundedSparse), no_oplog_replay(false) {}

  TableInfo table_info;

  /**
   * In # of rows.
   */
  size_t process_cache_capacity;

  /**
   * In # of rows.
   */
  size_t thread_cache_capacity;

  /**
   * Estimated upper bound # of pending oplogs in terms of # of rows. For SSP
   * this is the # of rows all threads collectively touches in a Clock().
   */
  size_t oplog_capacity;

  OpLogType oplog_type;

  AppendOnlyOpLogType append_only_oplog_type;

  size_t append_only_buff_capacity;

  /**
   * per partition
   */
  size_t per_thread_append_only_buff_pool_size;

  int32_t bg_apply_append_oplog_freq;

  ProcessStorageType process_storage_type;

  bool no_oplog_replay;
};

} // namespace petuum
