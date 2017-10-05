// ps_msgs.hpp
// author: jinliang

#pragma once

#include <petuum_ps/thread/msg_base.hpp>
#include <petuum_ps/include/configs.hpp>

namespace petuum {

struct ClientConnectMsg : public NumberedMsg {
public:
  ClientConnectMsg() {
    AllocateMemory();
    InitMsg();
  }

  explicit ClientConnectMsg(void *msg) : NumberedMsg(msg) {}

  int32_t &get_client_id() {
    return *(reinterpret_cast<int32_t *>(mem_.get_mem() +
                                         NumberedMsg::get_size()));
  }

  size_t get_size() { return NumberedMsg::get_size() + sizeof(int32_t); }

protected:
  void InitMsg() {
    NumberedMsg::InitMsg();
    get_msg_type() = kClientConnect;
  }
};

struct AggregatorConnectMsg : public NumberedMsg {
public:
  AggregatorConnectMsg() {
    AllocateMemory();
    InitMsg();
  }

  explicit AggregatorConnectMsg(void *msg) : NumberedMsg(msg) {}

  int32_t &get_client_id() {
    return *(reinterpret_cast<int32_t *>(mem_.get_mem() +
                                         NumberedMsg::get_size()));
  }

  size_t get_size() { return NumberedMsg::get_size() + sizeof(int32_t); }

protected:
  void InitMsg() {
    NumberedMsg::InitMsg();
    get_msg_type() = kAggregatorConnect;
  }
};

struct ServerConnectMsg : public NumberedMsg {
public:
  ServerConnectMsg() {
    AllocateMemory();
    InitMsg();
  }

  explicit ServerConnectMsg(void *msg) : NumberedMsg(msg) {}

protected:
  void InitMsg() {
    NumberedMsg::InitMsg();
    get_msg_type() = kServerConnect;
  }
};

struct AppConnectMsg : public NumberedMsg {
public:
  AppConnectMsg() {
    AllocateMemory();
    InitMsg();
  }

  explicit AppConnectMsg(void *msg) : NumberedMsg(msg) {}

protected:
  void InitMsg() {
    NumberedMsg::InitMsg();
    get_msg_type() = kAppConnect;
  }
};

struct SyncThreadConnectMsg : public NumberedMsg {
public:
  SyncThreadConnectMsg() {
    AllocateMemory();
    InitMsg();
  }

  explicit SyncThreadConnectMsg(void *msg) : NumberedMsg(msg) {}

protected:
  void InitMsg() { get_msg_type() = kSyncThreadConnect; }
};

struct SyncThreadDeregMsg : public NumberedMsg {
public:
  SyncThreadDeregMsg() {
    AllocateMemory();
    InitMsg();
  }

  explicit SyncThreadDeregMsg(void *msg) : NumberedMsg(msg) {}

protected:
  void InitMsg() { get_msg_type() = kSyncThreadDereg; }
};

struct BgCreateTableMsg : public NumberedMsg {
public:
  BgCreateTableMsg() {
    AllocateMemory();
    InitMsg();
  }

  explicit BgCreateTableMsg(void *msg) : NumberedMsg(msg) {}

  size_t get_size() {
    return NumberedMsg::get_size() + sizeof(int32_t) + sizeof(int32_t) +
           sizeof(int32_t) + sizeof(size_t) + sizeof(size_t) + sizeof(size_t) +
           sizeof(size_t) + sizeof(bool) + sizeof(int32_t) + sizeof(size_t) +
           sizeof(OpLogType) + sizeof(AppendOnlyOpLogType) + sizeof(size_t) +
           sizeof(size_t) + sizeof(int32_t) + sizeof(ProcessStorageType) +
           sizeof(bool);
  }

  int32_t &get_table_id() {
    return *(reinterpret_cast<int32_t *>(mem_.get_mem() +
                                         NumberedMsg::get_size()));
  }

  int32_t &get_staleness() {
    return *(reinterpret_cast<int32_t *>(
        mem_.get_mem() + NumberedMsg::get_size() + sizeof(int32_t)));
  }

  int32_t &get_row_type() {
    return *(reinterpret_cast<int32_t *>(mem_.get_mem() +
                                         NumberedMsg::get_size() +
                                         sizeof(int32_t) + sizeof(int32_t)));
  }

  size_t &get_row_capacity() {
    return *(reinterpret_cast<size_t *>(
        mem_.get_mem() + NumberedMsg::get_size() + sizeof(int32_t) +
        sizeof(int32_t) + sizeof(int32_t)));
  }

  size_t &get_process_cache_capacity() {
    return *(reinterpret_cast<size_t *>(
        mem_.get_mem() + NumberedMsg::get_size() + sizeof(int32_t) +
        sizeof(int32_t) + sizeof(int32_t) + sizeof(size_t)));
  }

  size_t &get_thread_cache_capacity() {
    return *(reinterpret_cast<size_t *>(
        mem_.get_mem() + NumberedMsg::get_size() + sizeof(int32_t) +
        sizeof(int32_t) + sizeof(int32_t) + sizeof(size_t) + sizeof(size_t)));
  }

  size_t &get_oplog_capacity() {
    return *(reinterpret_cast<size_t *>(
        mem_.get_mem() + NumberedMsg::get_size() + sizeof(int32_t) +
        sizeof(int32_t) + sizeof(int32_t) + sizeof(size_t) + sizeof(size_t) +
        sizeof(size_t)));
  }

  bool &get_oplog_dense_serialized() {
    return *(reinterpret_cast<bool *>(
        mem_.get_mem() + NumberedMsg::get_size() + sizeof(int32_t) +
        sizeof(int32_t) + sizeof(int32_t) + sizeof(size_t) + sizeof(size_t) +
        sizeof(size_t) + sizeof(size_t)));
  }

  int32_t &get_row_oplog_type() {
    return *(reinterpret_cast<int32_t *>(
        mem_.get_mem() + NumberedMsg::get_size() + sizeof(int32_t) +
        sizeof(int32_t) + sizeof(int32_t) + sizeof(size_t) + sizeof(size_t) +
        sizeof(size_t) + sizeof(size_t) + sizeof(bool)));
  }

  size_t &get_dense_row_oplog_capacity() {
    return *(reinterpret_cast<size_t *>(
        mem_.get_mem() + NumberedMsg::get_size() + sizeof(int32_t) +
        sizeof(int32_t) + sizeof(int32_t) + sizeof(size_t) + sizeof(size_t) +
        sizeof(size_t) + sizeof(size_t) + sizeof(bool) + sizeof(int32_t)));
  }

  OpLogType &get_oplog_type() {
    return *(reinterpret_cast<OpLogType *>(
        mem_.get_mem() + NumberedMsg::get_size() + sizeof(int32_t) +
        sizeof(int32_t) + sizeof(int32_t) + sizeof(size_t) + sizeof(size_t) +
        sizeof(size_t) + sizeof(size_t) + sizeof(bool) + sizeof(int32_t) +
        sizeof(size_t)));
  }

  AppendOnlyOpLogType &get_append_only_oplog_type() {
    return *(reinterpret_cast<AppendOnlyOpLogType *>(
        mem_.get_mem() + NumberedMsg::get_size() + sizeof(int32_t) +
        sizeof(int32_t) + sizeof(int32_t) + sizeof(size_t) + sizeof(size_t) +
        sizeof(size_t) + sizeof(size_t) + sizeof(bool) + sizeof(int32_t) +
        sizeof(size_t) + sizeof(OpLogType)));
  }

  size_t &get_append_only_buff_capacity() {
    return *(reinterpret_cast<size_t *>(
        mem_.get_mem() + NumberedMsg::get_size() + sizeof(int32_t) +
        sizeof(int32_t) + sizeof(int32_t) + sizeof(size_t) + sizeof(size_t) +
        sizeof(size_t) + sizeof(size_t) + sizeof(bool) + sizeof(int32_t) +
        sizeof(size_t) + sizeof(OpLogType) + sizeof(AppendOnlyOpLogType)));
  }

  size_t &get_per_thread_append_only_buff_pool_size() {
    return *(reinterpret_cast<size_t *>(
        mem_.get_mem() + NumberedMsg::get_size() + sizeof(int32_t) +
        sizeof(int32_t) + sizeof(int32_t) + sizeof(size_t) + sizeof(size_t) +
        sizeof(size_t) + sizeof(size_t) + sizeof(bool) + sizeof(int32_t) +
        sizeof(size_t) + sizeof(OpLogType) + sizeof(AppendOnlyOpLogType) +
        sizeof(size_t)));
  }

  int32_t &get_bg_apply_append_oplog_freq() {
    return *(reinterpret_cast<int32_t *>(
        mem_.get_mem() + NumberedMsg::get_size() + sizeof(int32_t) +
        sizeof(int32_t) + sizeof(int32_t) + sizeof(size_t) + sizeof(size_t) +
        sizeof(size_t) + sizeof(size_t) + sizeof(bool) + sizeof(int32_t) +
        sizeof(size_t) + sizeof(OpLogType) + sizeof(AppendOnlyOpLogType) +
        sizeof(size_t) + sizeof(size_t)));
  }

  ProcessStorageType &get_process_storage_type() {
    return *(reinterpret_cast<ProcessStorageType *>(
        mem_.get_mem() + NumberedMsg::get_size() + sizeof(int32_t) +
        sizeof(int32_t) + sizeof(int32_t) + sizeof(size_t) + sizeof(size_t) +
        sizeof(size_t) + sizeof(size_t) + sizeof(bool) + sizeof(int32_t) +
        sizeof(size_t) + sizeof(OpLogType) + sizeof(AppendOnlyOpLogType) +
        sizeof(size_t) + sizeof(size_t) + sizeof(int32_t)));
  }

  bool &get_no_oplog_replay() {
    return *(reinterpret_cast<bool *>(
        mem_.get_mem() + NumberedMsg::get_size() + sizeof(int32_t) +
        sizeof(int32_t) + sizeof(int32_t) + sizeof(size_t) + sizeof(size_t) +
        sizeof(size_t) + sizeof(size_t) + sizeof(bool) + sizeof(int32_t) +
        sizeof(size_t) + sizeof(OpLogType) + sizeof(AppendOnlyOpLogType) +
        sizeof(size_t) + sizeof(size_t) + sizeof(int32_t) +
        sizeof(ProcessStorageType)));
  }

protected:
  void InitMsg() {
    NumberedMsg::InitMsg();
    get_msg_type() = kBgCreateTable;
  }
};

struct CreateTableMsg : public NumberedMsg {
public:
  CreateTableMsg() {
    AllocateMemory();
    InitMsg();
  }

  explicit CreateTableMsg(void *msg) : NumberedMsg(msg) {}

  size_t get_size() {
    return NumberedMsg::get_size() + sizeof(int32_t) + sizeof(int32_t) +
           sizeof(int32_t) + sizeof(size_t) + sizeof(bool) + sizeof(int32_t) +
           sizeof(size_t);
  }

  int32_t &get_table_id() {
    return *(reinterpret_cast<int32_t *>(mem_.get_mem() +
                                         NumberedMsg::get_size()));
  }

  int32_t &get_staleness() {
    return *(reinterpret_cast<int32_t *>(
        mem_.get_mem() + NumberedMsg::get_size() + sizeof(int32_t)));
  }

  int32_t &get_row_type() {
    return *(reinterpret_cast<int32_t *>(mem_.get_mem() +
                                         NumberedMsg::get_size() +
                                         sizeof(int32_t) + sizeof(int32_t)));
  }

  size_t &get_row_capacity() {
    return *(reinterpret_cast<size_t *>(
        mem_.get_mem() + NumberedMsg::get_size() + sizeof(int32_t) +
        sizeof(int32_t) + sizeof(int32_t)));
  }

  bool &get_oplog_dense_serialized() {
    return *(reinterpret_cast<bool *>(mem_.get_mem() + NumberedMsg::get_size() +
                                      sizeof(int32_t) + sizeof(int32_t) +
                                      sizeof(int32_t) + sizeof(size_t)));
  }

  int32_t &get_row_oplog_type() {
    return *(reinterpret_cast<int32_t *>(
        mem_.get_mem() + NumberedMsg::get_size() + sizeof(int32_t) +
        sizeof(int32_t) + sizeof(int32_t) + sizeof(size_t) + sizeof(bool)));
  }

  size_t &get_dense_row_oplog_capacity() {
    return *(reinterpret_cast<size_t *>(
        mem_.get_mem() + NumberedMsg::get_size() + sizeof(int32_t) +
        sizeof(int32_t) + sizeof(int32_t) + sizeof(size_t) + sizeof(bool) +
        sizeof(int32_t)));
  }

protected:
  void InitMsg() {
    NumberedMsg::InitMsg();
    get_msg_type() = kCreateTable;
  }
};

struct CreateTableReplyMsg : public NumberedMsg {
public:
  CreateTableReplyMsg() {
    AllocateMemory();
    InitMsg();
  }

  explicit CreateTableReplyMsg(void *msg) : NumberedMsg(msg) {}

  size_t get_size() { return NumberedMsg::get_size() + sizeof(int32_t); }

  int32_t &get_table_id() {
    return *(reinterpret_cast<int32_t *>(mem_.get_mem() +
                                         NumberedMsg::get_size()));
  }

protected:
  void InitMsg() {
    NumberedMsg::InitMsg();
    get_msg_type() = kCreateTableReply;
  }
};

struct RowRequestMsg : public NumberedMsg {
public:
  RowRequestMsg() {
    AllocateMemory();
    InitMsg();
  }

  explicit RowRequestMsg(void *msg) : NumberedMsg(msg) {}

  size_t get_size() {
    return NumberedMsg::get_size() + sizeof(int32_t) + sizeof(int32_t) +
           sizeof(int32_t) + sizeof(bool);
  }

  int32_t &get_table_id() {
    return *(reinterpret_cast<int32_t *>(mem_.get_mem() +
                                         NumberedMsg::get_size()));
  }

  int32_t &get_row_id() {
    return *(reinterpret_cast<int32_t *>(
        mem_.get_mem() + NumberedMsg::get_size() + sizeof(int32_t)));
  }

  int32_t &get_clock() {
    return *(reinterpret_cast<int32_t *>(mem_.get_mem() +
                                         NumberedMsg::get_size() +
                                         sizeof(int32_t) + sizeof(int32_t)));
  }

  bool &get_forced_request() {
    return *(reinterpret_cast<bool *>(mem_.get_mem() + NumberedMsg::get_size() +
                                      sizeof(int32_t) + sizeof(int32_t) +
                                      sizeof(int32_t)));
  }

protected:
  void InitMsg() {
    NumberedMsg::InitMsg();
    get_msg_type() = kApplicationThreadRowRequest;
  }
};

struct RowRequestReplyMsg : public NumberedMsg {
public:
  RowRequestReplyMsg() {
    AllocateMemory();
    InitMsg();
  }

  explicit RowRequestReplyMsg(void *msg) : NumberedMsg(msg) {}

  size_t get_size() { return NumberedMsg::get_size(); }

protected:
  void InitMsg() {
    NumberedMsg::InitMsg();
    get_msg_type() = kRowRequestReply;
  }
};

struct CreatedAllTablesMsg : public NumberedMsg {
public:
  CreatedAllTablesMsg() {
    AllocateMemory();
    InitMsg();
  }

  explicit CreatedAllTablesMsg(void *msg) : NumberedMsg(msg) {}

protected:
  void InitMsg() {
    NumberedMsg::InitMsg();
    get_msg_type() = kCreatedAllTables;
  }
};

struct ConnectServerMsg : public NumberedMsg {
public:
  ConnectServerMsg() {
    AllocateMemory();
    InitMsg();
  }

  explicit ConnectServerMsg(void *msg) : NumberedMsg(msg) {}

protected:
  void InitMsg() {
    NumberedMsg::InitMsg();
    get_msg_type() = kConnectServer;
  }
};

struct ClientStartMsg : public NumberedMsg {
public:
  ClientStartMsg() {
    AllocateMemory();
    InitMsg();
  }

  explicit ClientStartMsg(void *msg) : NumberedMsg(msg) {}

protected:
  void InitMsg() {
    NumberedMsg::InitMsg();
    get_msg_type() = kClientStart;
  }
};

struct AppThreadDeregMsg : public NumberedMsg {
public:
  AppThreadDeregMsg() {
    AllocateMemory();
    InitMsg();
  }

  explicit AppThreadDeregMsg(void *msg) : NumberedMsg(msg) {}

protected:
  void InitMsg() {
    NumberedMsg::InitMsg();
    get_msg_type() = kAppThreadDereg;
  }
};

struct ClientShutDownMsg : public NumberedMsg {
public:
  ClientShutDownMsg() {
    AllocateMemory();
    InitMsg();
  }

  explicit ClientShutDownMsg(void *msg) : NumberedMsg(msg) {}

protected:
  void InitMsg() {
    NumberedMsg::InitMsg();
    get_msg_type() = kClientShutDown;
  }
};

struct ServerShutDownAckMsg : public NumberedMsg {
public:
  ServerShutDownAckMsg() {
    AllocateMemory();
    InitMsg();
  }

  explicit ServerShutDownAckMsg(void *msg) : NumberedMsg(msg) {}

protected:
  void InitMsg() {
    NumberedMsg::InitMsg();
    get_msg_type() = kServerShutDownAck;
  }
};

struct ServerOpLogAckMsg : public NumberedMsg {
public:
  ServerOpLogAckMsg() {
    AllocateMemory();
    InitMsg();
  }

  explicit ServerOpLogAckMsg(void *msg) : NumberedMsg(msg) {}

  size_t get_size() { return NumberedMsg::get_size() + sizeof(uint32_t); }

  uint32_t &get_ack_version() {
    return *(reinterpret_cast<uint32_t *>(mem_.get_mem() +
                                          NumberedMsg::get_size()));
  }

protected:
  void InitMsg() {
    NumberedMsg::InitMsg();
    get_msg_type() = kServerOpLogAck;
  }
};

struct ReplicaOpLogAckMsg : public NumberedMsg {
public:
  ReplicaOpLogAckMsg() {
    AllocateMemory();
    InitMsg();
  }

  explicit ReplicaOpLogAckMsg(void *msg) : NumberedMsg(msg) {}

  size_t get_size() {
    return NumberedMsg::get_size() + sizeof(uint32_t) + sizeof(int32_t);
  }

  uint32_t &get_ack_version() {
    return *(reinterpret_cast<uint32_t *>(mem_.get_mem() +
                                          NumberedMsg::get_size()));
  }

  int32_t &get_original_sender() {
    return *(reinterpret_cast<int32_t *>(
        mem_.get_mem() + NumberedMsg::get_size() + sizeof(uint32_t)));
  }

protected:
  void InitMsg() {
    NumberedMsg::InitMsg();
    get_msg_type() = kReplicaOpLogAck;
  }
};

/**
 * A message structure sent from app thread to bg worker thread, indicating
 * that the app thread has clocked. Other than the identifier, the message
 * also includes the id of the table that clocked. If we are clocking all
 * tables, then the id is set to -1.
 */
struct BgClockMsg : public NumberedMsg {
public:
  BgClockMsg() {
    AllocateMemory();
    InitMsg();
  }

  explicit BgClockMsg(void *msg) : NumberedMsg(msg) {}

  size_t get_size() { return NumberedMsg::get_size() + sizeof(int32_t); }

  int32_t &get_table_id() {
    return *(reinterpret_cast<int32_t *>(mem_.get_mem() +
                                         NumberedMsg::get_size()));
  }

protected:
  void InitMsg() {
    NumberedMsg::InitMsg();
    get_msg_type() = kBgClock;
    get_table_id() = -1;
  }
};

struct BgTableClockMsg : public NumberedMsg {
public:
  BgTableClockMsg() {
    AllocateMemory();
    InitMsg();
  }

  explicit BgTableClockMsg(void *msg) : NumberedMsg(msg) {}

protected:
  void InitMsg() {
    NumberedMsg::InitMsg();
    get_msg_type() = kBgTableClock;
  }
};

/**
 * A message structure sent from app thread to bg worker thread, indicating
 * that the worker has to send oplogs for all or a particular table.
 * table_id is set to -1 if oplogs for all tables need to be sent.
 */
struct BgSendOpLogMsg : public NumberedMsg {
public:
  BgSendOpLogMsg() {
    AllocateMemory();
    InitMsg();
  }

  explicit BgSendOpLogMsg(void *msg) : NumberedMsg(msg) {}

  size_t get_size() { return NumberedMsg::get_size() + sizeof(int32_t); }

  int32_t &get_table_id() {
    return *(reinterpret_cast<int32_t *>(mem_.get_mem() +
                                         NumberedMsg::get_size()));
  }

protected:
  void InitMsg() {
    NumberedMsg::InitMsg();
    get_msg_type() = kBgSendOpLog;
    get_table_id() = -1;
  }
};

struct BgHandleAppendOpLogMsg : public NumberedMsg {
public:
  BgHandleAppendOpLogMsg() {
    AllocateMemory();
    InitMsg();
  }

  explicit BgHandleAppendOpLogMsg(void *msg) : NumberedMsg(msg) {}

  size_t get_size() { return NumberedMsg::get_size() + sizeof(int32_t); }

  int32_t &get_table_id() {
    return *(reinterpret_cast<int32_t *>(mem_.get_mem() +
                                         NumberedMsg::get_size()));
  }

protected:
  void InitMsg() {
    NumberedMsg::InitMsg();
    get_msg_type() = kBgHandleAppendOpLog;
  }
};

struct ServerRowRequestReplyMsg : public ArbitrarySizedMsg {

public:
  explicit ServerRowRequestReplyMsg(int32_t avai_size) {
    own_mem_ = true;
    mem_.Alloc(get_header_size() + avai_size);
    InitMsg(avai_size);
  }

  explicit ServerRowRequestReplyMsg(void *msg) : ArbitrarySizedMsg(msg) {}

  size_t get_header_size() {
    return ArbitrarySizedMsg::get_header_size() + sizeof(int32_t) // table id
           + sizeof(int32_t)                                      // row id
           + sizeof(int32_t)                                      // clock
           + sizeof(uint32_t) // version (worker specific)
           + sizeof(size_t)   // row size
           + sizeof(int32_t)  // global model version
        ;
  }

  int32_t &get_table_id() {
    return *(reinterpret_cast<int32_t *>(mem_.get_mem() +
                                         ArbitrarySizedMsg::get_header_size()));
  }

  int32_t &get_row_id() {
    return *(reinterpret_cast<int32_t *>(mem_.get_mem() +
                                         ArbitrarySizedMsg::get_header_size() +
                                         sizeof(int32_t)));
  }

  int32_t &get_clock() {
    return *(reinterpret_cast<int32_t *>(mem_.get_mem() +
                                         ArbitrarySizedMsg::get_header_size() +
                                         sizeof(int32_t) + sizeof(int32_t)));
  }

  uint32_t &get_version() {
    return *(reinterpret_cast<uint32_t *>(
        mem_.get_mem() + ArbitrarySizedMsg::get_header_size() +
        sizeof(int32_t) + sizeof(int32_t) + sizeof(int32_t)));
  }

  size_t &get_row_size() {
    return *(reinterpret_cast<size_t *>(mem_.get_mem() +
                                        ArbitrarySizedMsg::get_header_size() +
                                        sizeof(int32_t) + sizeof(int32_t) +
                                        sizeof(int32_t) + sizeof(uint32_t)));
  }

  int32_t &get_global_model_version() {
    return *(reinterpret_cast<int32_t *>(
        mem_.get_mem() + ArbitrarySizedMsg::get_header_size() +
        sizeof(int32_t) + sizeof(int32_t) + sizeof(int32_t) + sizeof(uint32_t) +
        sizeof(size_t)));
  }

  void *get_row_data() { return mem_.get_mem() + get_header_size(); }

  size_t get_size() { return get_header_size() + get_avai_size(); }

protected:
  virtual void InitMsg(int32_t avai_size) {
    ArbitrarySizedMsg::InitMsg(avai_size);
    get_msg_type() = kServerRowRequestReply;
  }
};

struct ClientSendOpLogMsg : public ArbitrarySizedMsg {

public:
  explicit ClientSendOpLogMsg(int32_t avai_size) {
    own_mem_ = true;
    mem_.Alloc(get_header_size() + avai_size);
    InitMsg(avai_size);
  }

  explicit ClientSendOpLogMsg(void *msg) : ArbitrarySizedMsg(msg) {}

  size_t get_header_size() {
    return ArbitrarySizedMsg::get_header_size() +
           sizeof(bool) // a bit to denote whether the message is clock or not
           + sizeof(int32_t) // a 32 bit int to get the client id
           +
           sizeof(uint32_t) // a 32 bit unsigned int to denote the local version
           + sizeof(int32_t) // a 32 bit int to get clock value
           + sizeof(int32_t) // a 32 bit int to store the global model version
                             // number on which gradient is calculated
        ;
  }

  bool &get_is_clock() {
    // the indicator is stored immediately after Arbitrary Size Msg header
    // 1st value
    return *(reinterpret_cast<bool *>(mem_.get_mem() +
                                      ArbitrarySizedMsg::get_header_size()));
  }

  int32_t &get_client_id() {
    // 2nd value
    return *(reinterpret_cast<int32_t *>(
        mem_.get_mem() + ArbitrarySizedMsg::get_header_size() + sizeof(bool)));
  }

  uint32_t &get_version() {
    // 3rd value after Arbitrary sized message header
    return *(reinterpret_cast<uint32_t *>(mem_.get_mem() +
                                          ArbitrarySizedMsg::get_header_size() +
                                          sizeof(bool) + sizeof(int32_t)));
  }

  int32_t &get_bg_clock() {
    return *(reinterpret_cast<int32_t *>(
        mem_.get_mem() + ArbitrarySizedMsg::get_header_size() + sizeof(bool) +
        sizeof(int32_t) + sizeof(uint32_t)));
  }

  int32_t &get_global_model_version() {
    return *(reinterpret_cast<int32_t *>(
        mem_.get_mem() + ArbitrarySizedMsg::get_header_size() + sizeof(bool) +
        sizeof(int32_t) + sizeof(uint32_t) + sizeof(int32_t)));
  }

  // data is to be accessed via SerializedOpLogAccessor
  void *get_data() { return mem_.get_mem() + get_header_size(); }

  size_t get_size() { return get_header_size() + get_avai_size(); }

protected:
  virtual void InitMsg(int32_t avai_size) {
    ArbitrarySizedMsg::InitMsg(avai_size);
    get_msg_type() = kClientSendOpLog;
  }
};

struct ServerSendOpLogMsg : public ArbitrarySizedMsg {

public:
  explicit ServerSendOpLogMsg(int32_t avai_size) {
    own_mem_ = true;
    mem_.Alloc(get_header_size() + avai_size);
    InitMsg(avai_size);
  }

  explicit ServerSendOpLogMsg(void *msg) : ArbitrarySizedMsg(msg) {}

  size_t get_header_size() {
    return ArbitrarySizedMsg::get_header_size() +
           sizeof(int32_t) // a 32 bit int to get the original sender
           +
           sizeof(uint32_t) // a 32 bit unsigned int to denote the local version
           + sizeof(int32_t) // a 32 bit int to store the global model version
                             // number on which gradient is calculated
        ;
  }

  int32_t &get_original_sender_id() {
    // 1st value
    return *(reinterpret_cast<int32_t *>(mem_.get_mem() +
                                         ArbitrarySizedMsg::get_header_size()));
  }

  uint32_t &get_original_version() {
    // 2nd value after Arbitrary sized message header
    return *(reinterpret_cast<uint32_t *>(mem_.get_mem() +
                                          ArbitrarySizedMsg::get_header_size() +
                                          sizeof(int32_t)));
  }

  int32_t &get_global_model_version() {
    return *(reinterpret_cast<int32_t *>(mem_.get_mem() +
                                         ArbitrarySizedMsg::get_header_size() +
                                         sizeof(int32_t) + sizeof(uint32_t)));
  }

  // data is to be accessed via SerializedOpLogAccessor
  void *get_data() { return mem_.get_mem() + get_header_size(); }

  size_t get_size() { return get_header_size() + get_avai_size(); }

protected:
  virtual void InitMsg(int32_t avai_size) {
    ArbitrarySizedMsg::InitMsg(avai_size);
    get_msg_type() = kServerSendOpLog;
  }
};

struct ServerPushRowMsg : public ArbitrarySizedMsg {
public:
  explicit ServerPushRowMsg(int32_t avai_size) {
    own_mem_ = true;
    mem_.Alloc(get_header_size() + avai_size);
    InitMsg(avai_size);
  }

  explicit ServerPushRowMsg(void *msg) : ArbitrarySizedMsg(msg) {}

  size_t get_header_size() {
    return ArbitrarySizedMsg::get_header_size() + sizeof(int32_t) +
           sizeof(uint32_t) + sizeof(bool);
  }

  int32_t &get_clock() {
    return *(reinterpret_cast<int32_t *>(mem_.get_mem() +
                                         ArbitrarySizedMsg::get_header_size()));
  }

  uint32_t &get_version() {
    return *(reinterpret_cast<uint32_t *>(mem_.get_mem() +
                                          ArbitrarySizedMsg::get_header_size() +
                                          sizeof(int32_t)));
  }

  bool &get_is_clock() {
    return *(reinterpret_cast<bool *>(mem_.get_mem() +
                                      ArbitrarySizedMsg::get_header_size() +
                                      sizeof(int32_t) + sizeof(uint32_t)));
  }

  // data is to be accessed via SerializedRowReader
  void *get_data() { return mem_.get_mem() + get_header_size(); }

  size_t get_size() { return get_header_size() + get_avai_size(); }

protected:
  virtual void InitMsg(int32_t avai_size) {
    ArbitrarySizedMsg::InitMsg(avai_size);
    get_msg_type() = kServerPushRow;
  }
};
}
