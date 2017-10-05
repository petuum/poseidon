#pragma once
#include <glog/logging.h>
#include <gflags/gflags.h>
#include <petuum_ps/include/configs.hpp>

// just provide the definitions for simplicity

DECLARE_int32(table_staleness);
DECLARE_int32(row_type);
DECLARE_int32(row_oplog_type);
DECLARE_bool(oplog_dense_serialized);
DECLARE_string(oplog_type);
DECLARE_string(append_only_oplog_type);
DECLARE_uint64(append_only_buffer_capacity);
DECLARE_uint64(append_only_buffer_pool_size);
DECLARE_int32(bg_apply_append_oplog_freq);
DECLARE_string(process_storage_type);
