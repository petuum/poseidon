#include <gflags/gflags.h>
#include <petuum_ps/include/system_gflags_declare.hpp>

DEFINE_string(stats_path, "", "stats file path prefix");

// Topology Configs
DEFINE_int32(num_clients, 1, "total number of clients");
DEFINE_string(hostfile, "", "path to Petuum PS server configuration file");
DEFINE_int32(num_comm_channels_per_client, 1, "no. of comm channels per client");
DEFINE_bool(init_thread_access_table, false, "whether init thread accesses table");
DEFINE_int32(num_table_threads, 1, "no. of worker threads per client");
DEFINE_int32(client_id, 0, "This client's ID");
DEFINE_int32(num_app_threads, 1, "Number of app threads in this client");

// Execution Configs
DEFINE_string(consistency_model, "SSP", "SSP");

// SSPAggr Configs -- client side
DEFINE_uint64(bandwidth_mbps, 40, "per-thread bandwidth limit, in mbps");
DEFINE_uint64(bg_idle_milli, 10, "Bg idle millisecond");

DEFINE_uint64(oplog_push_upper_bound_kb, 100,
             "oplog push upper bound in Kilobytes per comm thread.");
DEFINE_int32(oplog_push_staleness_tolerance, 2,
             "oplog push staleness tolerance");
DEFINE_uint64(thread_oplog_batch_size, 100*1000*1000, "thread oplog batch size");

// SSPAggr Configs -- server side
DEFINE_uint64(server_row_candidate_factor, 5, "server row candidate factor");
DEFINE_int32(server_push_row_threshold, 100, "Server push row threshold");
DEFINE_int32(server_idle_milli, 10, "server idle time out in millisec");
DEFINE_string(update_sort_policy, "Random", "Update sort policy");

// Snapshot Configs
DEFINE_int32(snapshot_clock, -1, "snapshot clock");
DEFINE_int32(resume_clock, -1, "resume clock");
DEFINE_string(snapshot_dir, "", "snap shot directory");
DEFINE_string(resume_dir, "", "resume directory");
