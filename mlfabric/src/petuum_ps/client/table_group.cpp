#include <petuum_ps/util/stats.hpp>
#include <petuum_ps/client/table_group.hpp>
#include <petuum_ps/thread/context.hpp>
#include <petuum_ps/server/server_threads.hpp>
#include <petuum_ps/replica/replica_threads.hpp>
#include <petuum_ps/aggregator/aggregator_threads.hpp>
#include <petuum_ps/namenode/name_node.hpp>
#include <petuum_ps/scheduler/scheduler.hpp>
#include <petuum_ps/thread/bg_workers.hpp>
#include <sstream>
#include <iostream>
#include <algorithm>


namespace petuum {

  TableGroup::TableGroup(const TableGroupConfig &table_group_config, bool table_access, int32_t *init_thread_id):
    AbstractTableGroup(),
    max_table_staleness_(0) {

    int32_t num_comm_channels_per_client = table_group_config.num_comm_channels_per_client;
    int32_t num_local_app_threads = table_group_config.num_local_app_threads;
    int32_t num_local_table_threads = table_access ? num_local_app_threads : (num_local_app_threads - 1);
    int32_t num_tables = table_group_config.num_tables;
    int32_t num_total_clients = table_group_config.num_total_clients;
    const std::map<int32_t, HostInfo> &host_map = table_group_config.host_map;

    int32_t client_id = table_group_config.client_id;
    int32_t server_ring_size = table_group_config.server_ring_size;
    ConsistencyModel consistency_model = table_group_config.consistency_model;
    int32_t local_id_min = GlobalContext::get_thread_id_min(client_id);
    int32_t local_id_max = GlobalContext::get_thread_id_max(client_id);
    num_app_threads_registered_ = 1;  // init thread is the first one

    STATS_INIT(table_group_config);
    STATS_REGISTER_THREAD(kAppThread);

    // can be Inited after CommBus but must be before everything else
    GlobalContext::Init(
                        num_comm_channels_per_client,
                        num_local_app_threads,
                        num_local_table_threads,
                        num_tables,
                        num_total_clients,
                        host_map,
                        client_id,
                        server_ring_size,
                        consistency_model,
                        table_group_config.aggressive_cpu,
                        table_group_config.snapshot_clock,
                        table_group_config.snapshot_dir,
                        table_group_config.resume_clock,
                        table_group_config.resume_dir,
                        table_group_config.update_sort_policy,
                        table_group_config.bg_idle_milli,
                        table_group_config.bandwidth_mbps,
                        table_group_config.oplog_push_upper_bound_kb,
                        table_group_config.oplog_push_staleness_tolerance,
                        table_group_config.thread_oplog_batch_size,
                        table_group_config.server_push_row_threshold,
                        table_group_config.server_idle_milli,
                        table_group_config.server_row_candidate_factor);

    CommBus *comm_bus = new CommBus(local_id_min, local_id_max, num_total_clients, 6);
    GlobalContext::comm_bus = comm_bus;

    *init_thread_id = local_id_min + GlobalContext::kInitThreadIDOffset;

    CommBus::Config comm_config(*init_thread_id, CommBus::kNone, "");
    GlobalContext::comm_bus->ThreadRegister(comm_config);
    ThreadContext::RegisterThread(*init_thread_id);

    if(GlobalContext::am_i_scheduler_client()) {
      Scheduler::Init();
    }

    if (GlobalContext::am_i_name_node_client())  {
      NameNode::Init();
    }

    if (GlobalContext::am_i_server_client()) {
      ServerThreads::Init();
    }

    if (GlobalContext::am_i_worker_client()) {
      BgWorkers::Start(&tables_);
      BgWorkers::AppThreadRegister();
    }

    if (GlobalContext::am_i_aggregator_client()) {
      AggregatorThreads::Init();
    }

    if (GlobalContext::am_i_replica_client()) {
      ReplicaThreads::Init();
    }

    if (table_access) {
      vector_clock_.AddClock(*init_thread_id, 0);
    }

    if (table_group_config.aggressive_clock)
      ClockInternal = &TableGroup::ClockAggressive;
    else
      ClockInternal = &TableGroup::ClockConservative;
  }


  TableGroup::~TableGroup() {
    pthread_barrier_destroy(&register_barrier_);

    if(GlobalContext::am_i_worker_client()) {
      BgWorkers::AppThreadDeregister();
    }

    if (GlobalContext::am_i_server_client()) {
      ServerThreads::ShutDown();
    }

    if (GlobalContext::am_i_name_node_client()) {
      NameNode::ShutDown();
    }

    if (GlobalContext::am_i_scheduler_client()) {
      Scheduler::ShutDown();
    }

    if (GlobalContext::am_i_worker_client()) {
      BgWorkers::ShutDown();
    }

    if (GlobalContext::am_i_aggregator_client()) {
      AggregatorThreads::ShutDown();
    }

    if (GlobalContext::am_i_replica_client()) {
      ReplicaThreads::ShutDown();
    }

    GlobalContext::comm_bus->ThreadDeregister();
    delete GlobalContext::comm_bus;

    for(auto iter = tables_.begin(); iter != tables_.end(); iter++){
      delete iter->second;
    }

    STATS_DEREGISTER_THREAD();
    STATS_PRINT();

  }


  bool TableGroup::CreateTable(int32_t table_id, const ClientTableConfig& table_config) {
    CHECK_EQ(true, GlobalContext::am_i_worker_client()) << "Only (application threads on) worker clients can create tables.";
    max_table_staleness_ = std::max(max_table_staleness_, table_config.table_info.table_staleness);
    bool suc = BgWorkers::CreateTable(table_id, table_config);
    if (suc && (GlobalContext::get_num_app_threads() == GlobalContext::get_num_table_threads())) {
      auto iter = tables_.find(table_id);
      iter->second->RegisterThread();
    }
    return suc;
  }


  void TableGroup::CreateTableDone() {
    CHECK_EQ(true, GlobalContext::am_i_worker_client()) << "Only (application threads on) worker clients can create tables.";
    BgWorkers::WaitCreateTable();
    pthread_barrier_init(&register_barrier_, 0, GlobalContext::get_num_table_threads());
  }


  void TableGroup::WaitThreadRegister() {
    if (GlobalContext::get_num_table_threads() == GlobalContext::get_num_app_threads()) {
      pthread_barrier_wait(&register_barrier_);
    }
  }


  int32_t TableGroup::RegisterThread() {
    CHECK_EQ(true, GlobalContext::am_i_worker_client()) << "Only (application threads on) worker clients can create tables.";
    STATS_REGISTER_THREAD(kAppThread);
    int app_thread_id_offset = num_app_threads_registered_++;
    int32_t thread_id = GlobalContext::get_local_id_min() + GlobalContext::kInitThreadIDOffset + app_thread_id_offset;
    ThreadContext::RegisterThread(thread_id);

    petuum::CommBus::Config comm_config(thread_id, petuum::CommBus::kNone, "");
    GlobalContext::comm_bus->ThreadRegister(comm_config);
    BgWorkers::AppThreadRegister();

    vector_clock_.AddClock(thread_id, 0);

    for (auto table_iter = tables_.cbegin(); table_iter != tables_.cend(); table_iter++) {
      table_iter->second->RegisterThread();
    }

    pthread_barrier_wait(&register_barrier_);
    return thread_id;
  }


  void TableGroup::DeregisterThread(){
    CHECK_EQ(true, GlobalContext::am_i_worker_client()) << "Only (application threads on) worker clients can create tables.";
    for (auto table_iter = tables_.cbegin(); table_iter != tables_.cend(); table_iter++) {
      table_iter->second->DeregisterThread();
    }
    BgWorkers::AppThreadDeregister();
    GlobalContext::comm_bus->ThreadDeregister();
    STATS_DEREGISTER_THREAD();
  }


  // this is just a wrapper over aggressive or conservative clock.
  void TableGroup::Clock() {
    STATS_APP_ACCUM_TG_CLOCK_BEGIN();
    // move the clock of the application thread by a step
    ThreadContext::Clock();
    // Clock the table group, It can either be ClockAggresive or ClockConservative
    (this->*ClockInternal)();
    STATS_APP_ACCUM_TG_CLOCK_END();
  }


  void TableGroup::GlobalBarrier() {
    for (int i = 0; i < max_table_staleness_ + 1; ++i) {
      Clock();
    }
  }


  void TableGroup::ClockAggressive() {
    CHECK_EQ(true, GlobalContext::am_i_worker_client()) << "Only (application threads on) worker clients can create tables.";
    // Clocking the table, flushes the values in the thread_cache_ to
    // process_storage_ and, dumps the per thread oplog_index_ into a
    // table_oplog_index_ data structure that is visible from all the threads.
    for (auto table_iter = tables_.cbegin(); table_iter != tables_.cend(); table_iter++) {
      // clock each table, this in turn clocks the consistency controller (only does that), which
      // flushes the per thread oplog values to process_storage_ and also flushes the oplog_index_
      // to the per table (per process) op log index.
      table_iter->second->Clock();
    }
    // vector_clock_ used is a clock with locking support (enabled through
    // mutexes). Tick increments a thread specific clock. The return value is zero
    // is the current thread is not the slowest. Else, it returns a non zero
    // value, equal to the min_clock among all the threads.
    int clock = vector_clock_.Tick(ThreadContext::get_id());
    if (clock != 0) {
      BgWorkers::ClockAllTables();
    } else {
      // If Clock is aggressive, then even when the "Client" has not clocked
      // (i.e., all the threads have not clocked), Oplogs are sent to the server
      // to update values at the server.
      BgWorkers::SendOpLogsAllTables();
    }
  }


  void TableGroup::ClockConservative() {
    CHECK_EQ(true, GlobalContext::am_i_worker_client()) << "Only (application threads on) worker clients can create tables.";
    for (auto table_iter = tables_.cbegin(); table_iter != tables_.cend(); table_iter++) {
      table_iter->second->Clock();
    }
    int clock = vector_clock_.Tick(ThreadContext::get_id());
    // If Clock is Conservative, oplogs are send to the server only when all the
    // threads have finished an iteration.
    if (clock != 0) {
      // ClockAllTables will make the current thread invoke ClockAllTables in all bg threads.
      // Note that the bg_threads_ are accessible from worker groups. They inturn send a bg_clock_msg with
      // through SendInProc to themselves..
      BgWorkers::ClockAllTables();
    }
  }

} // end namespace -- petuum
