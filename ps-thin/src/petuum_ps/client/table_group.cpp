#include <petuum_ps/util/stats.hpp>
#include <petuum_ps/client/table_group.hpp>
#include <petuum_ps/server/server_threads.hpp>
#include <petuum_ps/server/name_node.hpp>
#include <petuum_ps/thread/bg_workers.hpp>

namespace petuum {

/**
 * @brief Constructor for a Table Group.
 *
 * An application creates a table group. When a table group is created, a
 * global context is populated with values from table group config; these
 * values can then be accessible from all threads. Further, we also initialize
 * a communication bus for threads to talk to one another using ZMQ.
 */
TableGroup::TableGroup(const TableGroupConfig &table_group_config,
                       bool table_access, int32_t *init_thread_id)
    : AbstractTableGroup(), max_table_staleness_(0) {

  int32_t num_total_clients = table_group_config.num_total_clients;

  // atomic int (the main thread is also registered)
  num_app_threads_registered_ = 1;

  // atomic int (ephemeral threads are sync threads)
  num_ephemeral_threads_registered_ = 0;

  STATS_INIT(table_group_config);
  STATS_REGISTER_THREAD(kAppThread);

  GlobalContext::Init(table_group_config, table_access);

  int32_t local_id_min = GlobalContext::get_local_id_min();
  int32_t local_id_max = GlobalContext::get_local_id_max();

  CommBus *comm_bus =
      new CommBus(local_id_min, local_id_max, num_total_clients, 6);

  GlobalContext::comm_bus = comm_bus;

  *init_thread_id = local_id_min + GlobalContext::kInitThreadIDOffset;

  CommBus::Config comm_config(*init_thread_id, CommBus::kNone, "");

  GlobalContext::comm_bus->ThreadRegister(comm_config);

  ThreadContext::RegisterThread(*init_thread_id);

  if (GlobalContext::am_i_name_node_client()) {
    NameNode::Init();
  }

  if (GlobalContext::am_i_server_client()) {
    ServerThreads::Init();
  }

  if (GlobalContext::am_i_worker_client()) {
    BgWorkers::Start(&tables_);
    BgWorkers::AppThreadRegister();
  }

  if (table_access) {
    vector_clock_.AddClock(*init_thread_id, 0);
  }

  if (table_group_config.aggressive_clock)
    ClockInternal = &TableGroup::ClockAggressive;
  else
    ClockInternal = &TableGroup::ClockConservative;
}

/**
 * @brief Destructor.
 */
TableGroup::~TableGroup() {
  pthread_barrier_destroy(&register_barrier_);

  if (GlobalContext::am_i_worker_client()) {
    BgWorkers::AppThreadDeregister();
  }

  if (GlobalContext::am_i_server_client()) {
    ServerThreads::ShutDown();
  }

  if (GlobalContext::am_i_name_node_client()) {
    NameNode::ShutDown();
  }

  if (GlobalContext::am_i_worker_client()) {
    BgWorkers::ShutDown();
  }

  GlobalContext::comm_bus->ThreadDeregister();
  delete GlobalContext::comm_bus;

  for (auto &table : tables_) {
    delete table.second;
  }

  STATS_DEREGISTER_THREAD();
  STATS_PRINT();
}

/**
 * @brief Create a table in the table group. And register the main thread to be
 * able to access the tables.
 */
bool TableGroup::CreateTable(int32_t table_id,
                             const ClientTableConfig &table_config) {
  CHECK_EQ(true, GlobalContext::am_i_worker_client())
      << "Only (application threads on) worker clients can create tables.";
  max_table_staleness_ =
      std::max(max_table_staleness_, table_config.table_info.table_staleness);
  bool suc = BgWorkers::CreateTable(table_id, table_config);
  if (suc && (GlobalContext::get_num_app_threads() ==
              GlobalContext::get_num_table_threads())) {
    auto iter = tables_.find(table_id);
    iter->second->RegisterThread();
  }

  if (suc) {
    table_clock_.AddClock(table_id, 0);
  }
  return suc;
}

/**
 * @brief Function to notify that the main thread is done creating all the
 * required a tables. Once tables are created, we wait for other application
 * threads to register.
 */
void TableGroup::CreateTableDone() {
  CHECK_EQ(true, GlobalContext::am_i_worker_client())
      << "Only (application threads on) worker clients can create tables.";
  BgWorkers::WaitCreateTable();
  // create
  pthread_barrier_init(&register_barrier_, 0,
                       GlobalContext::get_num_table_threads());
}

/**
 * @brief Barrier to wait for all application threads to register.
 */
void TableGroup::WaitThreadRegister() {
  if (GlobalContext::get_num_table_threads() ==
      GlobalContext::get_num_app_threads()) {
    pthread_barrier_wait(&register_barrier_);
  }
}

/**
 * @brief Helper function to allow any application thread (other than the main
 * thread responsible for creating tables) to register.
 */
int32_t TableGroup::RegisterThread() {
  CHECK_EQ(true, GlobalContext::am_i_worker_client())
      << "Only (application threads on) worker clients can create tables.";
  STATS_REGISTER_THREAD(kAppThread);
  int app_thread_id_offset = num_app_threads_registered_++;
  int32_t thread_id = GlobalContext::get_local_id_min() +
                      GlobalContext::kInitThreadIDOffset + app_thread_id_offset;

  ThreadContext::RegisterThread(thread_id);

  petuum::CommBus::Config comm_config(thread_id, petuum::CommBus::kNone, "");
  GlobalContext::comm_bus->ThreadRegister(comm_config);

  BgWorkers::AppThreadRegister();

  vector_clock_.AddClock(thread_id, 0);

  for (const auto &table : tables_) {
    table.second->RegisterThread();
  }

  pthread_barrier_wait(&register_barrier_);

  return thread_id;
}

/**
 * @brief Helper function for ephemeral threads to register. These threads, do
 * not use barriers. They can still use the table API; however, such threads
 * should only created by application threads that have previously registered.
 */
int32_t TableGroup::RegisterCaffeSyncThread(int32_t thread_offset) {
  CHECK_EQ(true, GlobalContext::am_i_worker_client())
      << "Only (application threads on) worker clients can create tables.";
  STATS_REGISTER_THREAD(kAppThread);
  ++num_ephemeral_threads_registered_;
  int ephemeral_thread_id =
      GlobalContext::get_head_ephemeral_thread_id() + thread_offset;

  // Register with comm bus
  petuum::CommBus::Config comm_config;
  comm_config.entity_id_ = ephemeral_thread_id;
  comm_config.ltype_ = petuum::CommBus::kNone;
  GlobalContext::comm_bus->ThreadRegister(comm_config);

  // Connect to BgWorkers -- sends a dummy message to that ZMQ is setup
  BgWorkers::SyncThreadRegister();
  return ephemeral_thread_id;
}

/**
 * @brief Deregister application thread
 */
void TableGroup::DeregisterThread() {
  CHECK_EQ(true, GlobalContext::am_i_worker_client())
      << "Only (application threads on) worker clients can create tables.";
  for (auto table_iter = tables_.cbegin(); table_iter != tables_.cend();
       table_iter++) {
    table_iter->second->DeregisterThread();
  }
  BgWorkers::AppThreadDeregister();
  GlobalContext::comm_bus->ThreadDeregister();
  STATS_DEREGISTER_THREAD();
}

/**
 * @brief Deregister ephemeral threads
 */
void TableGroup::DeregisterCaffeSyncThread() {
  CHECK_EQ(true, GlobalContext::am_i_worker_client())
      << "Only (application threads on) worker clients can create tables.";
  BgWorkers::SyncThreadDeregister();
  GlobalContext::comm_bus->ThreadDeregister();
  // (raajay) On de-registering we reduce the number of ephemeral threads.
  // This enables us to re-use the thread ids, when new sync threads are
  // spawned at the next iteration.
  int remaining_threads = --num_ephemeral_threads_registered_;
  CHECK_GE(remaining_threads, 0)
      << "Number of ephemeral threads is less than zero";
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

/**
 */
void TableGroup::ClockTable(int32_t table_id) {
  CHECK_EQ(true, GlobalContext::am_i_worker_client())
      << "Only (application threads on) worker clients can create tables.";
  // clock the table
  auto iter = tables_.find(table_id);
  iter->second->Clock();
  //
  int32_t min_clock = table_clock_.Tick(table_id);
}

/**
 */
void TableGroup::GlobalBarrier() {
  for (int i = 0; i < max_table_staleness_ + 1; ++i) {
    Clock();
  }
}

void TableGroup::ClockAggressive() {
  CHECK_EQ(true, GlobalContext::am_i_worker_client())
      << "Only (application threads on) worker clients can create tables.";
  // Clocking the table, flushes the values in the thread_cache_ to
  // process_storage_ and, dumps the per thread oplog_index_ into a
  // table_oplog_index_ data structure that is visible from all the threads.
  for (const auto &table : tables_) {
    // clock each table, this in turn clocks the consistency controller (only
    // does that), which
    // flushes the per thread oplog values to process_storage_ and also flushes
    // the oplog_index_
    // to the per table (per process) op log index.
    table.second->Clock();
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
  CHECK_EQ(true, GlobalContext::am_i_worker_client())
      << "Only (application threads on) worker clients can create tables.";
  // Clocking the table, flushes the values in the thread_cache_ to
  // process_storage_ and, dumps the per thread oplog_index_ into a
  // table_oplog_index_ data structure that is visible from all the threads.
  for (const auto &table : tables_) {
    table.second->Clock();
  }
  int clock = vector_clock_.Tick(ThreadContext::get_id());
  // If Clock is Conservative, oplogs are send to the server only when all the
  // threads have finished an iteration.
  if (clock != 0) {
    // ClockAllTables will make the current thread invoke ClockAllTables in all
    // bg threads.
    // Note that the bg_threads_ are accessible from worker groups. They in turn
    // send a bg_clock_msg with
    // through SendInProc to themselves..
    BgWorkers::ClockAllTables();
  }
}
}
