#include <petuum_ps/include/system_gflags_declare.hpp>
#include <petuum_ps/include/configs.hpp>
#include <petuum_ps/util/utils.hpp>

namespace petuum {

void InitTableGroupConfig(TableGroupConfig *config, int32_t num_tables) {
  config->stats_path = FLAGS_stats_path;
  config->num_comm_channels_per_client = FLAGS_num_comm_channels_per_client;
  config->num_tables = num_tables;
  config->num_total_clients = FLAGS_num_clients;
  config->num_local_app_threads = FLAGS_init_thread_access_table ?
                                  FLAGS_num_table_threads : FLAGS_num_table_threads + 1;

  GetHostInfos(FLAGS_hostfile, &(config->host_map));

  config->client_id = FLAGS_client_id;

  if (FLAGS_consistency_model == "SSP") {
    config->consistency_model = petuum::SSP;
  } else {
    LOG(FATAL) << "Unsupported ssp mode " << FLAGS_consistency_model;
  }

  config->aggressive_clock = false;
  config->aggressive_cpu = false;
  config->server_ring_size = 0;

  config->snapshot_clock = FLAGS_snapshot_clock;
  config->resume_clock = FLAGS_resume_clock;
  config->snapshot_dir = FLAGS_snapshot_dir;
  config->resume_dir = FLAGS_resume_dir;

  if (FLAGS_update_sort_policy == "Random") {
    config->update_sort_policy = Random;
  } else if (FLAGS_update_sort_policy == "FIFO") {
    config->update_sort_policy = FIFO;
  } else if (FLAGS_update_sort_policy == "RelativeMagnitude") {
    config->update_sort_policy = RelativeMagnitude;
  } else {
    LOG(FATAL) << "Unknown update sort policy" << FLAGS_update_sort_policy;
  }

  config->bg_idle_milli = FLAGS_bg_idle_milli;
  config->bandwidth_mbps = FLAGS_bandwidth_mbps;
  config->oplog_push_upper_bound_kb = FLAGS_oplog_push_upper_bound_kb;
  config->oplog_push_staleness_tolerance = FLAGS_oplog_push_staleness_tolerance;
  config->thread_oplog_batch_size = FLAGS_thread_oplog_batch_size;
  config->server_push_row_threshold = FLAGS_server_push_row_threshold;
  config->server_idle_milli = FLAGS_server_idle_milli;
  config->server_row_candidate_factor = FLAGS_server_row_candidate_factor;
}
}


