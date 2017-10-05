// Copyright (c) 2014, Sailing Lab
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice,
// this list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright
// notice, this list of conditions and the following disclaimer in the
// documentation and/or other materials provided with the distribution.
//
// 3. Neither the name of the <ORGANIZATION> nor the names of its contributors
// may be used to endorse or promote products derived from this software
// without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.
// Deep Neural Network built on Petuum Parameter Server
// Author: Pengtao Xie (pengtaox@cs.cmu.edu)


#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>
#include <string>
#include <chrono>
#include <ctime>
#include <boost/thread/thread.hpp>
#include <boost/bind.hpp>
#include <boost/ref.hpp>
#include "paras.h"
#include "dnn.h"
#include <stdio.h>
#include <stdlib.h>
#include <io/general_fstream.hpp>

DEFINE_string(hostfile, "", "Path to file containing server ip:port.");
DEFINE_string(parafile,"", "Path to file containing DNN hyperparameters");
DEFINE_int32(num_clients, 1, "Total number of clients");
DEFINE_int32(client_id, 0, "This client's ID");
DEFINE_int32(num_worker_threads, 4, "Number of application worker threads");
DEFINE_int32(num_comm_clients, 4, "Number of worker threads");
DEFINE_int32(staleness, 0, "Staleness");
DEFINE_string(data_ptt_file, "", "Path to file containing the number of data points in each partition");
DEFINE_int32(snapshot_clock, 0, "How often to take PS snapshots; 0 disables");
DEFINE_int32(resume_clock, 0, "If nonzero, resume from the PS snapshot with this ID");
DEFINE_string(model_weight_file, "", "Path to save weight matrices");
DEFINE_string(model_bias_file, "", "Path to save bias vectors");
DEFINE_string(snapshot_dir, "", "Path to save PS snapshots");
DEFINE_string(resume_dir, "", "Where to retrieve PS snapshots for resuming");
DEFINE_int32(ps_row_in_memory_limit, 1000, "Single-machine version only: max # rows of weight matrices that can be held in memory");
DEFINE_string(stats_path, "", "Statistics output file");

petuum::TableGroupConfig populateTableGroupConfig(dnn_paras para) {
    petuum::TableGroupConfig table_group_config;
    // Global params
    table_group_config.num_comm_channels_per_client = FLAGS_num_comm_clients;
    table_group_config.num_total_clients = FLAGS_num_clients;
    table_group_config.num_tables = (para.num_layers-1)*2;  // tables storing weights and biases

    // Single-node-PS versus regular distributed PS
#ifdef PETUUM_SINGLE_NODE
    table_group_config.ooc_path_prefix = "dnn_sn.localooc";
  table_group_config.consistency_model = petuum::LocalOOC;
#else
    petuum::GetHostInfos(FLAGS_hostfile, &table_group_config.host_map);
    //  petuum::GetServerIDsFromHostMap(&table_group_config.server_ids, table_group_config.host_map);
    table_group_config.consistency_model = petuum::SSP;
#endif
    // Local parameters for this process
    table_group_config.num_local_app_threads = FLAGS_num_worker_threads + 1;  // +1 for main() thread
    table_group_config.client_id = FLAGS_client_id;
    table_group_config.stats_path = FLAGS_stats_path;
    table_group_config.aggressive_clock = false; // make clock conservative by default
    // snapshots
    table_group_config.snapshot_clock = FLAGS_snapshot_clock;
    table_group_config.resume_clock = FLAGS_resume_clock;
    table_group_config.snapshot_dir = FLAGS_snapshot_dir;
    table_group_config.resume_dir = FLAGS_resume_dir;
    return table_group_config;
}

// Main function
int main(int argc, char *argv[]) {

  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  //load dnn parameters
  dnn_paras para;
  load_dnn_paras(para, FLAGS_parafile.c_str());

  std::chrono::time_point<std::chrono::system_clock> now;
  now = std::chrono::system_clock::now();
  std::time_t start_time = std::chrono::system_clock::to_time_t(now);

  VLOG(0) << "Experiment started at: " << std::ctime(&start_time);
  VLOG(0) << "daemon " << FLAGS_client_id << " starts working..." << std::endl;
  std::cout<<"daemon "<<FLAGS_client_id << " starts working..." << std::endl;
  std::cout<<"Staleness parameter=" << FLAGS_staleness << std::endl;

  // Configure Petuum parameter server
  petuum::TableGroupConfig table_group_config = populateTableGroupConfig(para);
  VLOG(0) << table_group_config.toString();

  // Configure Petuum parameter server tables
  petuum::PSTableGroup::RegisterRow<petuum::DenseRow<float> >(0);  // Register dense rows as ID 0
  petuum::PSTableGroup::Init(table_group_config, false);  // Initializing thread does not need table access

  // Upon invoking Init, the global context is initialized, and the background workers, server, replicas, aggregators
  // (if any) have started.

  // TODO (raajay) move these configs to table group config?
  petuum::GlobalContext::set_asynchronous(para.asynchronous > 0);
  petuum::GlobalContext::set_replication(para.replication > 0);

  if (petuum::GlobalContext::am_i_worker_client()) {
    // We will create tables and launch application threads only on worker clients.

    petuum::ClientTableConfig table_config; // Common table settings
    table_config.table_info.row_type = 0; // Dense rows
    table_config.oplog_capacity = 10000; // 10000 rows in table can be updated in each iteration

    int32_t total_num_params = 0;
    int32_t total_num_rows = 0;

    //create DNN weight tables
    for(int i = 0; i < para.num_layers - 1; i++) {
      table_config.table_info.table_staleness = FLAGS_staleness;
      table_config.table_info.row_capacity = (size_t) para.num_units_ineach_layer[i];
      table_config.table_info.row_oplog_type = petuum::RowOpLogType::kDenseRowOpLog;
      table_config.table_info.oplog_dense_serialized = true;
      table_config.table_info.dense_row_oplog_capacity = (size_t) para.num_units_ineach_layer[i];

#ifdef PETUUM_SINGLE_NODE
      table_config.process_cache_capacity = std::min(FLAGS_ps_row_in_memory_limit, para.num_units_ineach_layer[i+1]);
#else
      table_config.process_cache_capacity = (size_t) para.num_units_ineach_layer[i+1];
#endif
      CHECK(petuum::PSTableGroup::CreateTable(i,table_config)) << "Failed to create weight table!" ;
      VLOG(2) << "Create weight table with " << table_config.process_cache_capacity
              << " rows and " << table_config.table_info.row_capacity << " columns.";
      total_num_params += para.num_units_ineach_layer[i] * para.num_units_ineach_layer[i+1];
      total_num_rows += para.num_units_ineach_layer[i+1];
    }

    //create DNN biases tables
    for(int i = 0; i < para.num_layers - 1; i++) {
      table_config.table_info.table_staleness = FLAGS_staleness;
      table_config.table_info.row_capacity = para.num_units_ineach_layer[i + 1];
      table_config.table_info.row_oplog_type = petuum::RowOpLogType::kDenseRowOpLog;
      table_config.table_info.oplog_dense_serialized = true;
      table_config.process_cache_capacity = 1;
      table_config.table_info.dense_row_oplog_capacity = para.num_units_ineach_layer[i + 1];
      CHECK(petuum::PSTableGroup::CreateTable(i + para.num_layers-1,table_config)) << "Failed to create bias table!";
      VLOG(2) << "Create bias table with " << table_config.process_cache_capacity
              << " rows and " << table_config.table_info.row_capacity << " columns.";
      total_num_params += para.num_units_ineach_layer[i+1];
      total_num_rows += 1;
    }

    // Finished creating tables
    petuum::PSTableGroup::CreateTableDone();

    // Read the training data in all worker clients.
    char data_file[512];
    int num_train_data = 0;
    int client_index = petuum::GlobalContext::get_worker_client_index(FLAGS_client_id);
    petuum::io::ifstream infile(FLAGS_data_ptt_file);
    VLOG(0) << "data_partition_file: " << FLAGS_data_ptt_file;
    int cnter=0;
    while (true) {
      infile >> data_file >> num_train_data;
      if (client_index <= cnter) break;
      cnter++;
    }
    infile.close();

    // display parameters
    VLOG(0) << "My client id = " << FLAGS_client_id;
    VLOG(0) << "Number of app workers = " << FLAGS_num_worker_threads;
    VLOG(0) << "Staleness value = " << FLAGS_staleness;
    VLOG(0) << "Total number of parameters = " << total_num_params;
    VLOG(0) << "Total number of rows = " << total_num_rows;
    VLOG(0) << "Clock = " << ((table_group_config.aggressive_clock) ? "Aggressive" : "Conservative");
    VLOG(0) << "Data file = " << data_file;
    VLOG(0) << "Expected size of training data = " << num_train_data;

    //run dnn
    dnn mydnn(para, FLAGS_client_id, FLAGS_num_worker_threads, FLAGS_staleness, num_train_data);
    //load data
    VLOG(0)<<"client "<<FLAGS_client_id<<" starts to load "<<num_train_data<<" data from "<<data_file;
    mydnn.load_data(data_file);
    VLOG(0)<<"client "<<FLAGS_client_id<<" load data ends.";

    // start application threads -- which will then use background(bg) worker threads to work.
    boost::thread_group worker_threads; // these are application workers
    for (int i = 0; i < FLAGS_num_worker_threads; ++i) {
      worker_threads.create_thread(boost::bind(&dnn::run, boost::ref(mydnn), FLAGS_model_weight_file, FLAGS_model_bias_file));
    }
    petuum::PSTableGroup::WaitThreadRegister(); // this is a barrier for all application to get initialized.
    worker_threads.join_all(); // then, we allow threads (in particular the run function) to run till completion.
    VLOG(0) << "All application threads on client:"<< FLAGS_client_id << "(except main) have completed";

    if(FLAGS_client_id == 0) {
      VLOG(0) << "DNN training ends." << std::endl;
    }

  } else { // corresponding if -- am i worker client
    // if this process is just the namenode/scheduler (or) server, we should pause.
  }

  // Cleanup and output runtime. Shutdown will block until all the spawned
  // threads (server threads, bg threads, namenode threads) terminate. This is
  // required because we do not want the process to terminate.
  petuum::PSTableGroup::ShutDown();

  std::cout << "...DONE." << std::endl;
  VLOG(0) << "...DONE." << std::endl;
  return 0;
}
