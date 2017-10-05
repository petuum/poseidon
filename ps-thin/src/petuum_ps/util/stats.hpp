// Author: Jinliang Wei
#pragma once

#include <petuum_ps/util/high_resolution_timer.hpp>
#include <petuum_ps/include/configs.hpp>
#include <boost/thread/tss.hpp>
#include <boost/unordered_map.hpp>
#include <vector>
#include <cstdint>
#include <cstddef>
#include <string>
#include <mutex>
#include <iostream>
#include <yaml-cpp/yaml.h>

#ifdef PETUUM_STATS

#define STATS_INIT(table_group_config) Stats::Init(table_group_config)

#define STATS_REGISTER_THREAD(thread_type) Stats::RegisterThread(thread_type)

#define STATS_DEREGISTER_THREAD() Stats::DeregisterThread()

#define STATS_APP_ACCUM_TG_CLOCK_BEGIN() Stats::AppAccumTgClockBegin()

#define STATS_APP_ACCUM_TG_CLOCK_END() Stats::AppAccumTgClockEnd()

#define STATS_APP_SAMPLE_SSP_GET_BEGIN(table_id)                               \
  Stats::AppSampleSSPGetBegin(table_id)

#define STATS_APP_SAMPLE_SSP_GET_END(table_id, hit)                            \
  Stats::AppSampleSSPGetEnd(table_id, hit)

#define STATS_APP_ACCUM_SSP_GET_SERVER_FETCH_BEGIN(table_id)                   \
  Stats::AppAccumSSPGetServerFetchBegin(table_id)

#define STATS_APP_ACCUM_SSP_GET_SERVER_FETCH_END(table_id)                     \
  Stats::AppAccumSSPGetServerFetchEnd(table_id)

#define STATS_APP_SAMPLE_INC_BEGIN(table_id) Stats::AppSampleIncBegin(table_id)

#define STATS_APP_SAMPLE_INC_END(table_id) Stats::AppSampleIncEnd(table_id)

#define STATS_APP_SAMPLE_BATCH_INC_BEGIN(table_id)                             \
  Stats::AppSampleBatchIncBegin(table_id)

#define STATS_APP_SAMPLE_BATCH_INC_END(table_id)                               \
  Stats::AppSampleBatchIncEnd(table_id)

#define STATS_APP_SAMPLE_BATCH_INC_OPLOG_BEGIN()                               \
  Stats::AppSampleBatchIncOplogBegin()

#define STATS_APP_SAMPLE_BATCH_INC_OPLOG_END()                                 \
  Stats::AppSampleBatchIncOplogEnd()

#define STATS_APP_SAMPLE_BATCH_INC_PROCESS_STORAGE_BEGIN()                     \
  Stats::AppSampleBatchIncProcessStorageBegin()

#define STATS_APP_SAMPLE_BATCH_INC_PROCESS_STORAGE_END()                       \
  Stats::AppSampleBatchIncProcessStorageEnd()

#define STATS_APP_SAMPLE_CLOCK_BEGIN(table_id)                                 \
  Stats::AppSampleTableClockBegin(table_id)

#define STATS_APP_SAMPLE_CLOCK_END(table_id)                                   \
  Stats::AppSampleTableClockEnd(table_id)

#define STATS_APP_ACCUM_APPEND_ONLY_FLUSH_OPLOG_BEGIN()                        \
  petuum::Stats::AppAccumAppendOnlyFlushOpLogBegin()

#define STATS_APP_ACCUM_APPEND_ONLY_FLUSH_OPLOG_END()                          \
  petuum::Stats::AppAccumAppendOnlyFlushOpLogEnd()

#define STATS_BG_ACCUM_CLOCK_END_OPLOG_SERIALIZE_BEGIN()                       \
  Stats::BgAccumClockEndOpLogSerializeBegin()

#define STATS_BG_ACCUM_CLOCK_END_OPLOG_SERIALIZE_END()                         \
  Stats::BgAccumClockEndOpLogSerializeEnd()

#define STATS_BG_CLOCK() Stats::BgClock()

#define STATS_BG_ADD_PER_CLOCK_OPLOG_SIZE(oplog_size)                          \
  Stats::BgAddPerClockOpLogSize(oplog_size)

#define STATS_SERVER_ACCUM_APPLY_OPLOG_BEGIN()                                 \
  Stats::ServerAccumApplyOpLogBegin()

#define STATS_SERVER_ACCUM_APPLY_OPLOG_END() Stats::ServerAccumApplyOpLogEnd()

#define STATS_SERVER_CLOCK() Stats::ServerClock()

#define STATS_SERVER_ADD_PER_CLOCK_OPLOG_SIZE(oplog_size)                      \
  Stats::ServerAddPerClockOpLogSize(oplog_size)

#define STATS_SERVER_OPLOG_MSG_RECV_INC_ONE() Stats::ServerOpLogMsgRecvIncOne();

#define STATS_PRINT() petuum::Stats::PrintStats()

#define STATS_SYNCHRONIZE() petuum::Stats::SynchronizeThreadStatistics()

// MLfabric stats
#define STATS_MLFABRIC_CLIENT_PUSH_BEGIN(server_id, version_id)                \
  Stats::MLFabricClientPushBegin(server_id, version_id)

#define STATS_MLFABRIC_CLIENT_PUSH_END(server_id, version_id)                  \
  Stats::MLFabricClientPushEnd(server_id, version_id)

#define STATS_MLFABRIC_SERVER_RECORD_DELAY(delay)                              \
  Stats::MLFabricServerRecordDelay(delay)

#else
#define STATS_INIT(table_group_config) ((void)0)
#define STATS_REGISTER_THREAD(thread_type) ((void)0)
#define STATS_DEREGISTER_THREAD() ((void)0)
#define STATS_APP_ACCUM_TG_CLOCK_BEGIN() ((void)0)
#define STATS_APP_ACCUM_TG_CLOCK_END() ((void)0)
#define STATS_APP_SAMPLE_SSP_GET_BEGIN(table_id) ((void)0)
#define STATS_APP_SAMPLE_SSP_GET_END(table_id, hit) ((void)0)

#define STATS_APP_ACCUM_SSP_GET_SERVER_FETCH_BEGIN(table_id) ((void)0)
#define STATS_APP_ACCUM_SSP_GET_SERVER_FETCH_END(table_id) ((void)0)
#define STATS_APP_SAMPLE_INC_BEGIN(table_id) ((void)0)
#define STATS_APP_SAMPLE_INC_END(table_id) ((void)0)
#define STATS_APP_SAMPLE_BATCH_INC_BEGIN(table_id) ((void)0)
#define STATS_APP_SAMPLE_BATCH_INC_END(table_id) ((void)0)
#define STATS_APP_SAMPLE_BATCH_INC_OPLOG_BEGIN() ((void)0)
#define STATS_APP_SAMPLE_BATCH_INC_OPLOG_END() ((void)0)
#define STATS_APP_SAMPLE_BATCH_INC_PROCESS_STORAGE_BEGIN() ((void)0)
#define STATS_APP_SAMPLE_BATCH_INC_PROCESS_STORAGE_END() ((void)0)
#define STATS_APP_SAMPLE_CLOCK_BEGIN(table_id) ((void)0)
#define STATS_APP_SAMPLE_CLOCK_END(table_id) ((void)0)

#define STATS_APP_ACCUM_APPEND_ONLY_FLUSH_OPLOG_BEGIN() ((void)0)
#define STATS_APP_ACCUM_APPEND_ONLY_FLUSH_OPLOG_END() ((void)0)

#define STATS_BG_ACCUM_CLOCK_END_OPLOG_SERIALIZE_BEGIN() ((void)0)
#define STATS_BG_ACCUM_CLOCK_END_OPLOG_SERIALIZE_END() ((void)0)
#define STATS_BG_CLOCK() ((void)0)
#define STATS_BG_ADD_PER_CLOCK_OPLOG_SIZE(oplog_size) ((void)0)

#define STATS_SERVER_ACCUM_APPLY_OPLOG_BEGIN() ((void)0)
#define STATS_SERVER_ACCUM_APPLY_OPLOG_END() ((void)0)
#define STATS_SERVER_CLOCK() ((void)0)
#define STATS_SERVER_ADD_PER_CLOCK_OPLOG_SIZE(oplog_size) ((void)0)
#define STATS_SERVER_OPLOG_MSG_RECV_INC_ONE() ((void)0)

#define STATS_PRINT() petuum::Stats::DummyPrintStats()

#define STATS_SYNCHRONIZE() ((void)0)
#define STATS_MLFABRIC_CLIENT_PUSH_BEGIN(server_id, version_id) ((void)0)
#define STATS_MLFABRIC_CLIENT_PUSH_END(server_id, version_id) ((void)0)
#define STATS_MLFABRIC_SERVER_RECORD_DELAY(delay) ((void)0)
#endif

namespace petuum {

enum ThreadType {
  kAppThread = 0,
  kBgThread = 1,
  kServerThread = 2,
  kNameNodeThread = 3
};

struct AppThreadPerTableStats {
  // System timers:
  HighResolutionTimer get_timer; // ct = client table
  HighResolutionTimer inc_timer;
  HighResolutionTimer batch_inc_timer;

  HighResolutionTimer clock_timer;

  HighResolutionTimer ssp_get_server_fetch_timer;

  uint64_t num_get;
  uint64_t num_ssp_get_hit;
  uint64_t num_ssp_get_miss;
  uint64_t num_ssp_get_hit_sampled;
  uint64_t num_ssp_get_miss_sampled;
  double accum_sample_ssp_get_hit_sec;
  double accum_sample_ssp_get_miss_sec;

  // Number of Gets that are blocked waiting on receiving
  // server pushed messages.
  uint64_t num_ssppush_get_comm_block;
  // Number of seconds spent on waiting for server push message.
  double accum_ssppush_get_comm_block_sec;

  double accum_ssp_get_server_fetch_sec;

  uint64_t num_inc;
  uint64_t num_inc_sampled;
  double accum_sample_inc_sec;
  uint64_t num_batch_inc;
  uint32_t num_batch_inc_sampled;
  double accum_sample_batch_inc_sec;

  uint64_t num_thread_get;
  double accum_sample_thread_get_sec;
  uint64_t num_thread_inc;
  double accum_sample_thread_inc_sec;
  uint64_t num_thread_batch_inc;
  double accum_sample_thread_batch_inc_sec;

  uint64_t num_clock;
  uint64_t num_clock_sampled;
  double accum_sample_clock_sec;

  AppThreadPerTableStats()
      : num_get(0), num_ssp_get_hit(0), num_ssp_get_miss(0),
        num_ssp_get_hit_sampled(0), num_ssp_get_miss_sampled(0),
        accum_sample_ssp_get_hit_sec(0), accum_sample_ssp_get_miss_sec(0),
        num_ssppush_get_comm_block(0), accum_ssppush_get_comm_block_sec(0.0),
        accum_ssp_get_server_fetch_sec(0.0), num_inc(0), num_inc_sampled(0),
        accum_sample_inc_sec(0), num_batch_inc(0), num_batch_inc_sampled(0),
        accum_sample_batch_inc_sec(0), num_thread_get(0),
        accum_sample_thread_get_sec(0), num_thread_inc(0),
        accum_sample_thread_inc_sec(0), num_thread_batch_inc(0),
        accum_sample_thread_batch_inc_sec(0), num_clock(0),
        num_clock_sampled(0), accum_sample_clock_sec(0) {}
};

struct AppThreadStats {
  // Application timers:
  HighResolutionTimer thread_life_timer;
  // System timers:
  HighResolutionTimer tg_clock_timer; // tg = table group
  HighResolutionTimer batch_inc_oplog_timer;
  HighResolutionTimer batch_inc_process_storage_timer;

  boost::unordered::unordered_map<int32_t, AppThreadPerTableStats> table_stats;

  double load_data_sec;
  double init_sec;
  double bootstrap_sec;
  double accum_comp_sec;
  double accum_obj_comp_sec;
  double accum_tg_clock_sec;

  // Detailed BatchInc stats (sum over all tables).
  uint64_t num_batch_inc_oplog;
  uint64_t num_batch_inc_process_storage;
  uint32_t num_batch_inc_oplog_sampled;
  uint32_t num_batch_inc_process_storage_sampled;
  double accum_sample_batch_inc_oplog_sec;
  double accum_sample_batch_inc_process_storage_sec;

  double app_defined_accum_sec;

  double app_defined_accum_val;

  HighResolutionTimer append_only_oplog_flush_timer;
  double accum_append_only_oplog_flush_sec;
  size_t append_only_flush_oplog_count;

  AppThreadStats()
      : load_data_sec(0), init_sec(0), accum_comp_sec(0), accum_obj_comp_sec(0),
        accum_tg_clock_sec(0), num_batch_inc_oplog(0),
        num_batch_inc_process_storage(0), num_batch_inc_oplog_sampled(0),
        num_batch_inc_process_storage_sampled(0),
        accum_sample_batch_inc_oplog_sec(0),
        accum_sample_batch_inc_process_storage_sec(0), app_defined_accum_sec(0),
        app_defined_accum_val(0), accum_append_only_oplog_flush_sec(0),
        append_only_flush_oplog_count(0) {}
};

struct BgThreadStats {
  HighResolutionTimer oplog_serialize_timer;

  double accum_clock_end_oplog_serialize_sec;
  double accum_total_oplog_serialize_sec;

  double accum_server_push_row_apply_sec;

  double accum_oplog_sent_kb;
  double accum_server_push_row_recv_kb;

  size_t accum_server_push_oplog_row_applied;
  size_t accum_server_push_update_applied;
  size_t accum_server_push_version_diff;

  HighResolutionTimer process_cache_insert_timer;
  double sample_process_cache_insert_sec;
  size_t num_process_cache_insert;
  size_t num_process_cache_insert_sampled;

  double sample_server_push_deserialize_sec;
  size_t num_server_push_deserialize;
  size_t num_server_push_deserialize_sampled;

  std::vector<double> per_clock_oplog_sent_kb;
  std::vector<double> per_clock_server_push_row_recv_kb;

  uint32_t clock_num;

  size_t accum_num_idle_invoke;
  size_t accum_num_idle_send;
  size_t accum_num_push_row_msg_recv;
  double accum_idle_send_sec;

  size_t accum_idle_send_bytes;

  double accum_handle_append_oplog_sec;
  size_t num_append_oplog_buff_handled;

  size_t num_row_oplog_created;
  size_t num_row_oplog_recycled;

  // MLfabric stats storage

  // indexed server_id, version_id
  boost::unordered::unordered_map<
      int32_t, boost::unordered::unordered_map<int32_t, HighResolutionTimer *>>
      mlfabric_client_push_timers;
  boost::unordered::unordered_map<
      int32_t, boost::unordered::unordered_map<int32_t, double>>
      mlfabric_client_push_elapsed_time;

  boost::unordered::unordered_map<
      int32_t, boost::unordered::unordered_map<int32_t, HighResolutionTimer *>>
      mlfabric_client_pull_timers;

  BgThreadStats()
      : accum_clock_end_oplog_serialize_sec(0.0),
        accum_total_oplog_serialize_sec(0.0),
        accum_server_push_row_apply_sec(0.0), accum_oplog_sent_kb(0.0),
        accum_server_push_row_recv_kb(0.0),
        accum_server_push_oplog_row_applied(0),
        accum_server_push_update_applied(0), accum_server_push_version_diff(0),
        sample_process_cache_insert_sec(0.0), num_process_cache_insert(0),
        num_process_cache_insert_sampled(0),
        sample_server_push_deserialize_sec(0.0), num_server_push_deserialize(0),
        num_server_push_deserialize_sampled(0), per_clock_oplog_sent_kb(1, 0.0),
        per_clock_server_push_row_recv_kb(1, 0.0), clock_num(0),
        accum_num_idle_invoke(0), accum_num_idle_send(0),
        accum_num_push_row_msg_recv(0), accum_idle_send_sec(0),
        accum_idle_send_bytes(0), accum_handle_append_oplog_sec(0),
        num_row_oplog_created(0), num_row_oplog_recycled(0) {}

  ~BgThreadStats() {
    // delete hr timers, if any
  }
};

struct ServerThreadStats {
  HighResolutionTimer apply_oplog_timer;

  double accum_apply_oplog_sec;
  double accum_push_row_sec;

  double accum_oplog_recv_kb;
  double accum_push_row_kb;

  std::vector<double> per_clock_oplog_recv_kb;
  std::vector<double> per_clock_push_row_kb;

  uint32_t clock_num;

  size_t accum_num_oplog_msg_recv;
  size_t accum_num_push_row_msg_send;

  std::vector<int32_t> observed_delays;

  ServerThreadStats()
      : accum_apply_oplog_sec(0.0), accum_push_row_sec(0.0),
        accum_oplog_recv_kb(0.0), accum_push_row_kb(0.0),
        per_clock_oplog_recv_kb(1, 0.0), per_clock_push_row_kb(1, 0.0),
        clock_num(0), accum_num_oplog_msg_recv(0),
        accum_num_push_row_msg_send(0) {}
};

struct NameNodeThreadStats {};

// Functions are thread-safe unless otherwise specified.
class Stats {
public:
  static void Init(const TableGroupConfig &table_group_config);

  static void RegisterThread(ThreadType thread_type);
  static void DeregisterThread();

  static void AppAccumTgClockBegin();
  static void AppAccumTgClockEnd();

  static void AppSampleSSPGetBegin(int32_t table_id);
  static void AppSampleSSPGetEnd(int32_t table_id, bool hit);

  static void AppAccumSSPGetServerFetchBegin(int32_t table_id);
  static void AppAccumSSPGetServerFetchEnd(int32_t table_id);

  static void AppSampleIncBegin(int32_t table_id);
  static void AppSampleIncEnd(int32_t table_id);

  static void AppSampleBatchIncBegin(int32_t table_id);
  static void AppSampleBatchIncEnd(int32_t table_id);

  static void AppSampleBatchIncOplogBegin();
  static void AppSampleBatchIncOplogEnd();

  static void AppSampleBatchIncProcessStorageBegin();
  static void AppSampleBatchIncProcessStorageEnd();

  static void AppSampleTableClockBegin(int32_t table_id);
  static void AppSampleTableClockEnd(int32_t table_id);

  static void AppAccumAppendOnlyFlushOpLogBegin();
  static void AppAccumAppendOnlyFlushOpLogEnd();

  // the following functions are not thread safe
  static void BgAccumOpLogSerializeBegin();
  static void BgAccumOpLogSerializeEnd();

  static void BgAccumClockEndOpLogSerializeBegin();
  static void BgAccumClockEndOpLogSerializeEnd();

  static void BgSampleProcessCacheInsertBegin();
  static void BgSampleProcessCacheInsertEnd();

  static void BgClock();
  static void BgAddPerClockOpLogSize(size_t oplog_size);
  static void ServerAccumApplyOpLogBegin();
  static void ServerAccumApplyOpLogEnd();

  static void ServerClock();
  static void ServerAddPerClockOpLogSize(size_t oplog_size);

  static void ServerOpLogMsgRecvIncOne();

  static void PrintStats();
  static void DummyPrintStats();

  // MLfabric stats helper
  static void MLFabricClientPushBegin(int32_t server_id, int32_t version_id);
  static void MLFabricClientPushEnd(int32_t server_id, int32_t version_id);
  static void MLFabricServerRecordDelay(int32_t delay);

private:
  static void DeregisterAppThread();
  static void DeregisterBgThread();
  static void DeregisterServerThread();

  template <typename T>
  static void YamlPrintSequence(YAML::Emitter *yaml_out,
                                const std::vector<T> &sequence);

  static const int32_t kGetSampleFreq = 10000;
  static const int32_t kIncSampleFreq = 10000;
  static const int32_t kBatchIncSampleFreq = 1000;
  static const int32_t kBatchIncOplogSampleFreq = 10000;
  static const int32_t kBatchIncProcessStorageSampleFreq = 10000;
  static const int32_t kClockSampleFreq = 1;

  static const int32_t kProcessCacheInsertSampleFreq = 1000;

  // assuming I have received all server pushed message after this number
  // of Get()s.
  static const int32_t kFirstNGetToSkip = 10;

  static TableGroupConfig table_group_config_;
  static std::string stats_path_;
  static int32_t stats_print_version_;
  static boost::thread_specific_ptr<ThreadType> thread_type_;
  static boost::thread_specific_ptr<AppThreadStats> app_thread_stats_;
  static boost::thread_specific_ptr<BgThreadStats> bg_thread_stats_;
  static boost::thread_specific_ptr<ServerThreadStats> server_thread_stats_;
  static boost::thread_specific_ptr<NameNodeThreadStats>
      name_node_thread_stats_;

  static std::mutex stats_mtx_;

  // App thread stats
  static std::vector<double> app_thread_life_sec_;
  static std::vector<double> app_load_data_sec_;
  static std::vector<double> app_init_sec_;
  static std::vector<double> app_bootstrap_sec_;
  static std::vector<double> app_accum_comp_sec_vec_;

  // including all blocking communication timer per thread.
  static std::vector<double> app_accum_comm_block_sec_;

  // sum over all threads
  static double app_sum_accum_comm_block_sec_;
  // sum over all tables, all threads
  static double app_sum_approx_ssp_get_hit_sec_;
  static double app_sum_approx_inc_sec_;
  static double app_sum_approx_batch_inc_sec_;
  static double app_sum_approx_batch_inc_oplog_sec_;
  static double app_sum_approx_batch_inc_process_storage_sec_;

  static std::string app_defined_accum_sec_name_;
  static std::vector<double> app_defined_accum_sec_vec_;

  static std::string app_defined_vec_name_;
  static std::vector<double> app_defined_vec_;

  static std::string app_defined_accum_val_name_;
  static double app_defined_accum_val_;

  static boost::unordered::unordered_map<int32_t, AppThreadPerTableStats>
      table_stats_;
  static double app_accum_comp_sec_;
  static double app_accum_obj_comp_sec_;
  static double app_accum_tg_clock_sec_;

  static std::vector<double> app_accum_append_only_flush_oplog_sec_;
  static std::vector<size_t> app_append_only_flush_oplog_count_;

  // Bg thread stats
  static double bg_accum_clock_end_oplog_serialize_sec_;
  static double bg_accum_total_oplog_serialize_sec_;

  static double bg_accum_server_push_row_apply_sec_;

  static double bg_accum_oplog_sent_mb_;
  static double bg_accum_server_push_row_recv_mb_;

  static std::vector<double> bg_per_clock_oplog_sent_mb_;
  static std::vector<double> bg_per_clock_server_push_row_recv_mb_;

  static std::vector<size_t> bg_accum_server_push_oplog_row_applied_;
  static std::vector<size_t> bg_accum_server_push_update_applied_;
  static std::vector<size_t> bg_accum_server_push_version_diff_;

  static std::vector<double> bg_sample_process_cache_insert_sec_;
  static std::vector<size_t> bg_num_process_cache_insert_;
  static std::vector<size_t> bg_num_process_cache_insert_sampled_;

  static std::vector<double> bg_sample_server_push_deserialize_sec_;
  static std::vector<size_t> bg_num_server_push_deserialize_;
  static std::vector<size_t> bg_num_server_push_deserialize_sampled_;

  static std::vector<size_t> bg_accum_num_idle_invoke_;
  static std::vector<size_t> bg_accum_num_idle_send_;
  static std::vector<size_t> bg_accum_num_push_row_msg_recv_;
  static std::vector<double> bg_accum_idle_send_sec_;
  static std::vector<size_t> bg_accum_idle_send_bytes_;

  static std::vector<double> bg_accum_handle_append_oplog_sec_;
  static std::vector<size_t> bg_num_append_oplog_buff_handled_;

  static std::vector<size_t> bg_num_row_oplog_created_;
  static std::vector<size_t> bg_num_row_oplog_recycled_;

  // Server thread stats
  static double server_accum_apply_oplog_sec_;

  static double server_accum_push_row_sec_;

  static double server_accum_oplog_recv_mb_;
  static double server_accum_push_row_mb_;

  static std::vector<double> server_per_clock_oplog_recv_mb_;
  static std::vector<double> server_per_clock_push_row_mb_;

  static std::vector<size_t> server_accum_num_oplog_msg_recv_;
  static std::vector<size_t> server_accum_num_push_row_msg_send_;
};

// A wrapper function to call print stats when ever we want from the
// application.
// If we have to use the macro then, the appropriate flags have to be defined
// when compiling the application.
void PrintStatsWrapper();
}
