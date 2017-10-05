// ps_msgs.hpp
// author: jinliang

#pragma once

#include <petuum_ps/thread/msg_base.hpp>
#include <petuum_ps/include/configs.hpp>

namespace petuum {

  /**
   * Message send by an entity to connect with another.
   */
  struct ConnectMsg : public NumberedMsg {
  public:
    ConnectMsg() {
      if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
        own_mem_ = true;
        use_stack_buff_ = false;
        mem_.Alloc(get_size());
      } else {
        own_mem_ = false;
        use_stack_buff_ = true;
        mem_.Reset(stack_buff_);
      }
      InitMsg();
    }

    explicit ConnectMsg(void *msg): NumberedMsg(msg) {}

    size_t get_size() {
      return NumberedMsg::get_size()
        + sizeof(EntityType) // 1. entity type
        + sizeof(int32_t) // 2. entity id
        ;
    }

    // 1st element
    EntityType &get_entity_type() {
      return *(reinterpret_cast<EntityType*>(mem_.get_mem()
                                             + NumberedMsg::get_size()
                                             ));
    }

    // 2nd element
    int32_t &get_entity_id() {
      return *(reinterpret_cast<int32_t*>(mem_.get_mem()
                                          + NumberedMsg::get_size()
                                          + sizeof(EntityType)
                                          ));
    }

  protected:
    void InitMsg() {
      NumberedMsg::InitMsg();
      get_msg_type() = kConnect;
    }
  };


  struct AppConnectMsg : public NumberedMsg {
  public:
    AppConnectMsg() {
      if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
        own_mem_ = true;
        use_stack_buff_ = false;
        mem_.Alloc(get_size());
      } else {
        own_mem_ = false;
        use_stack_buff_ = true;
        mem_.Reset(stack_buff_);
      }
      InitMsg();
    }

    explicit AppConnectMsg(void *msg):
      NumberedMsg(msg) {}

  protected:
    void InitMsg() {
      NumberedMsg::InitMsg();
      get_msg_type() = kAppConnect;
    }
  };


  struct BgCreateTableMsg : public NumberedMsg {
  public:
    BgCreateTableMsg() {
      if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
        own_mem_ = true;
        use_stack_buff_ = false;
        mem_.Alloc(get_size());
      } else {
        own_mem_ = false;
        use_stack_buff_ = true;
        mem_.Reset(stack_buff_);
      }
      InitMsg();
    }

    explicit BgCreateTableMsg(void *msg):
      NumberedMsg(msg) {}

    size_t get_size() {
      return NumberedMsg::get_size() + sizeof(int32_t) + sizeof(int32_t)
        + sizeof(int32_t) + sizeof(size_t) + sizeof(size_t) + sizeof(size_t)
        + sizeof(size_t) + sizeof(bool) + sizeof(int32_t)
        + sizeof(size_t)  + sizeof(OpLogType) +sizeof(AppendOnlyOpLogType)
        + sizeof(size_t) + sizeof(size_t) + sizeof(int32_t)
        + sizeof(ProcessStorageType) + sizeof(bool);
    }

    int32_t &get_table_id() {
      return *(reinterpret_cast<int32_t*>(mem_.get_mem()
                                          + NumberedMsg::get_size()));
    }

    int32_t &get_staleness() {
      return *(reinterpret_cast<int32_t *>(mem_.get_mem()
                                           + NumberedMsg::get_size() + sizeof(int32_t)));
    }

    int32_t &get_row_type() {
      return *(reinterpret_cast<int32_t*>(mem_.get_mem()
                                          + NumberedMsg::get_size() + sizeof(int32_t) + sizeof(int32_t)));
    }

    size_t &get_row_capacity() {
      return *(reinterpret_cast<size_t*>(mem_.get_mem()
                                         + NumberedMsg::get_size() + sizeof(int32_t) + sizeof(int32_t)
                                         + sizeof(int32_t)));
    }

    size_t &get_process_cache_capacity() {
      return *(reinterpret_cast<size_t*>(mem_.get_mem()
                                         + NumberedMsg::get_size() + sizeof(int32_t) + sizeof(int32_t)
                                         + sizeof(int32_t) + sizeof(size_t)));
    }

    size_t &get_thread_cache_capacity() {
      return *(reinterpret_cast<size_t*>(mem_.get_mem()
                                         + NumberedMsg::get_size() + sizeof(int32_t) + sizeof(int32_t)
                                         + sizeof(int32_t) + sizeof(size_t) + sizeof(size_t)));
    }

    size_t &get_oplog_capacity() {
      return *(reinterpret_cast<size_t*>(mem_.get_mem()
                                         + NumberedMsg::get_size() + sizeof(int32_t) + sizeof(int32_t)
                                         + sizeof(int32_t) + sizeof(size_t) + sizeof(size_t)
                                         + sizeof(size_t)));
    }

    bool &get_oplog_dense_serialized() {
      return *(reinterpret_cast<bool*>(mem_.get_mem()
                                       + NumberedMsg::get_size() + sizeof(int32_t) + sizeof(int32_t)
                                       + sizeof(int32_t) + sizeof(size_t) + sizeof(size_t)
                                       + sizeof(size_t) + sizeof(size_t)));
    }

    int32_t &get_row_oplog_type() {
      return *(reinterpret_cast<int32_t*>(
                                          mem_.get_mem()
                                          + NumberedMsg::get_size() + sizeof(int32_t) + sizeof(int32_t)
                                          + sizeof(int32_t) + sizeof(size_t) + sizeof(size_t)
                                          + sizeof(size_t) + sizeof(size_t) + sizeof(bool)));
    }

    size_t &get_dense_row_oplog_capacity() {
      return *(reinterpret_cast<size_t*>(
                                         mem_.get_mem()
                                         + NumberedMsg::get_size() + sizeof(int32_t) + sizeof(int32_t)
                                         + sizeof(int32_t) + sizeof(size_t) + sizeof(size_t)
                                         + sizeof(size_t) + sizeof(size_t) + sizeof(bool) + sizeof(int32_t)));
    }

    OpLogType &get_oplog_type() {
      return *(reinterpret_cast<OpLogType*>(
                                            mem_.get_mem()
                                            + NumberedMsg::get_size() + sizeof(int32_t) + sizeof(int32_t)
                                            + sizeof(int32_t) + sizeof(size_t) + sizeof(size_t)
                                            + sizeof(size_t) + sizeof(size_t) + sizeof(bool) + sizeof(int32_t)
                                            + sizeof(size_t)));
    }

    AppendOnlyOpLogType &get_append_only_oplog_type() {
      return *(reinterpret_cast<AppendOnlyOpLogType*>(
                                                      mem_.get_mem()
                                                      + NumberedMsg::get_size() + sizeof(int32_t) + sizeof(int32_t)
                                                      + sizeof(int32_t) + sizeof(size_t) + sizeof(size_t)
                                                      + sizeof(size_t) + sizeof(size_t) + sizeof(bool) + sizeof(int32_t)
                                                      + sizeof(size_t) + sizeof(OpLogType) ));
    }

    size_t &get_append_only_buff_capacity() {
      return *(reinterpret_cast<size_t*>(
                                         mem_.get_mem()
                                         + NumberedMsg::get_size() + sizeof(int32_t) + sizeof(int32_t)
                                         + sizeof(int32_t) + sizeof(size_t) + sizeof(size_t)
                                         + sizeof(size_t) + sizeof(size_t) + sizeof(bool) + sizeof(int32_t)
                                         + sizeof(size_t) + sizeof(OpLogType) +sizeof(AppendOnlyOpLogType) ));
    }

    size_t &get_per_thread_append_only_buff_pool_size() {
      return *(reinterpret_cast<size_t*>(
                                         mem_.get_mem()
                                         + NumberedMsg::get_size() + sizeof(int32_t) + sizeof(int32_t)
                                         + sizeof(int32_t) + sizeof(size_t) + sizeof(size_t)
                                         + sizeof(size_t) + sizeof(size_t) + sizeof(bool) + sizeof(int32_t)
                                         + sizeof(size_t) + sizeof(OpLogType) +sizeof(AppendOnlyOpLogType)
                                         + sizeof(size_t) ));
    }

    int32_t &get_bg_apply_append_oplog_freq() {
      return *(reinterpret_cast<int32_t*>(
                                          mem_.get_mem()
                                          + NumberedMsg::get_size() + sizeof(int32_t) + sizeof(int32_t)
                                          + sizeof(int32_t) + sizeof(size_t) + sizeof(size_t)
                                          + sizeof(size_t) + sizeof(size_t) + sizeof(bool) + sizeof(int32_t)
                                          + sizeof(size_t) + sizeof(OpLogType) +sizeof(AppendOnlyOpLogType)
                                          + sizeof(size_t) + sizeof(size_t) ));
    }

    ProcessStorageType &get_process_storage_type() {
      return *(reinterpret_cast<ProcessStorageType*>(
                                                     mem_.get_mem()
                                                     + NumberedMsg::get_size() + sizeof(int32_t) + sizeof(int32_t)
                                                     + sizeof(int32_t) + sizeof(size_t) + sizeof(size_t)
                                                     + sizeof(size_t) + sizeof(size_t) + sizeof(bool) + sizeof(int32_t)
                                                     + sizeof(size_t) + sizeof(OpLogType) +sizeof(AppendOnlyOpLogType)
                                                     + sizeof(size_t) + sizeof(size_t) + sizeof(int32_t) ));
    }

    bool &get_no_oplog_replay() {
      return *(reinterpret_cast<bool*>(
                                       mem_.get_mem()
                                       + NumberedMsg::get_size() + sizeof(int32_t) + sizeof(int32_t)
                                       + sizeof(int32_t) + sizeof(size_t) + sizeof(size_t)
                                       + sizeof(size_t) + sizeof(size_t) + sizeof(bool) + sizeof(int32_t)
                                       + sizeof(size_t) + sizeof(OpLogType) +sizeof(AppendOnlyOpLogType)
                                       + sizeof(size_t) + sizeof(size_t) + sizeof(int32_t)
                                       + sizeof(ProcessStorageType) ));
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
      if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
        own_mem_ = true;
        use_stack_buff_ = false;
        mem_.Alloc(get_size());
      } else {
        own_mem_ = false;
        use_stack_buff_ = true;
        mem_.Reset(stack_buff_);
      }
      InitMsg();
    }

    explicit CreateTableMsg(void *msg):
      NumberedMsg(msg) {}

    size_t get_size() {
      return NumberedMsg::get_size() + sizeof(int32_t) + sizeof(int32_t)
        + sizeof(int32_t) + sizeof(size_t)
        + sizeof(bool) + sizeof(int32_t) + sizeof(size_t);
    }

    int32_t &get_table_id() {
      return *(reinterpret_cast<int32_t*>(mem_.get_mem()
                                          + NumberedMsg::get_size()));
    }

    int32_t &get_staleness() {
      return *(reinterpret_cast<int32_t*>(mem_.get_mem()
                                          + NumberedMsg::get_size() + sizeof(int32_t)));
    }

    int32_t &get_row_type() {
      return *(reinterpret_cast<int32_t*>(mem_.get_mem() + NumberedMsg::get_size()
                                          + sizeof(int32_t) + sizeof(int32_t)));
    }

    size_t &get_row_capacity() {
      return *(reinterpret_cast<size_t*>(mem_.get_mem() + NumberedMsg::get_size()
                                         + sizeof(int32_t) + sizeof(int32_t) + sizeof(int32_t)));
    }

    bool &get_oplog_dense_serialized() {
      return *(reinterpret_cast<bool*>(
                                       mem_.get_mem() + NumberedMsg::get_size()
                                       + sizeof(int32_t) + sizeof(int32_t) + sizeof(int32_t)
                                       + sizeof(size_t)));
    }

    int32_t &get_row_oplog_type() {
      return *(reinterpret_cast<int32_t*>(
                                          mem_.get_mem() + NumberedMsg::get_size()
                                          + sizeof(int32_t) + sizeof(int32_t) + sizeof(int32_t)
                                          + sizeof(size_t) + sizeof(bool)));
    }

    size_t &get_dense_row_oplog_capacity() {
      return *(reinterpret_cast<size_t*>(
                                         mem_.get_mem() + NumberedMsg::get_size()
                                         + sizeof(int32_t) + sizeof(int32_t) + sizeof(int32_t)
                                         + sizeof(size_t) + sizeof(bool) + sizeof(int32_t)));
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
      if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
        own_mem_ = true;
        use_stack_buff_ = false;
        mem_.Alloc(get_size());
      } else {
        own_mem_ = false;
        use_stack_buff_ = true;
        mem_.Reset(stack_buff_);
      }
      InitMsg();
    }

    explicit CreateTableReplyMsg(void *msg):
      NumberedMsg(msg) {}

    size_t get_size() {
      return NumberedMsg::get_size() + sizeof(int32_t);
    }

    int32_t &get_table_id() {
      return *(reinterpret_cast<int32_t*>(mem_.get_mem()
                                          + NumberedMsg::get_size()));
    }

  protected:
    void InitMsg(){
      NumberedMsg::InitMsg();
      get_msg_type() = kCreateTableReply;
    }
  };

  // TODO should be called Table request msg
  struct BulkRowRequestMsg : public NumberedMsg {
  public:
    BulkRowRequestMsg() {
      if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
        own_mem_ = true;
        use_stack_buff_ = false;
        mem_.Alloc(get_size());
      } else {
        own_mem_ = false;
        use_stack_buff_ = true;
        mem_.Reset(stack_buff_);
      }
      InitMsg();
    }

    explicit BulkRowRequestMsg(void *msg): NumberedMsg(msg) {}

    size_t get_size() {
      return NumberedMsg::get_size()
        + sizeof(int32_t) // 1. table id
        + sizeof(int32_t) // 2. clock
        + sizeof(int32_t) // 3. forced
        ;
    }

    int32_t &get_table_id() {
      return *(reinterpret_cast<int32_t*>(mem_.get_mem()
                                          + NumberedMsg::get_size()));
    }

    int32_t &get_clock() {
      return *(reinterpret_cast<int32_t*>(mem_.get_mem()
                                          + NumberedMsg::get_size()
                                          + sizeof(int32_t)));
    }

    bool &get_forced_request() {
      return *(reinterpret_cast<bool*>(mem_.get_mem()
                                       + NumberedMsg::get_size()
                                       + sizeof(int32_t)
                                       +sizeof(int32_t)));
    }

  protected:
    void InitMsg() {
      NumberedMsg::InitMsg();
      get_msg_type() = kBulkRowRequest;
    }
  };

  struct RowRequestMsg : public NumberedMsg {
  public:
    RowRequestMsg() {
      if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
        own_mem_ = true;
        use_stack_buff_ = false;
        mem_.Alloc(get_size());
      } else {
        own_mem_ = false;
        use_stack_buff_ = true;
        mem_.Reset(stack_buff_);
      }
      InitMsg();
    }

    explicit RowRequestMsg(void *msg):
      NumberedMsg(msg) {}

    size_t get_size() {
      return NumberedMsg::get_size() + sizeof(int32_t) + sizeof(int32_t)
        + sizeof(int32_t) + sizeof(bool);
    }

    int32_t &get_table_id() {
      return *(reinterpret_cast<int32_t*>(mem_.get_mem()
                                          + NumberedMsg::get_size()));
    }

    int32_t &get_row_id() {
      return *(reinterpret_cast<int32_t*>(mem_.get_mem()
                                          + NumberedMsg::get_size() + sizeof(int32_t)));
    }

    int32_t &get_clock() {
      return *(reinterpret_cast<int32_t*>(mem_.get_mem()
                                          + NumberedMsg::get_size() + sizeof(int32_t) + sizeof(int32_t)));
    }

    bool &get_forced_request() {
      return *(reinterpret_cast<bool*>(
                                       mem_.get_mem() + NumberedMsg::get_size() + sizeof(int32_t)
                                       + sizeof(int32_t) + sizeof(int32_t)));
    }

  protected:
    void InitMsg() {
      NumberedMsg::InitMsg();
      get_msg_type() = kRowRequest;
    }
  };

  /**
   * Reply that a bg worker sends to app thread after successful completion of a
   * row request.
   */
  struct RowRequestReplyMsg : public NumberedMsg {
  public:
    RowRequestReplyMsg() {
      if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
        own_mem_ = true;
        use_stack_buff_ = false;
        mem_.Alloc(get_size());
      } else {
        own_mem_ = false;
        use_stack_buff_ = true;
        mem_.Reset(stack_buff_);
      }
      InitMsg();
    }

    explicit RowRequestReplyMsg(void *msg):
      NumberedMsg(msg) {}

    size_t get_size() {
      return NumberedMsg::get_size();
    }

  protected:
    void InitMsg() {
      NumberedMsg::InitMsg();
      get_msg_type() = kRowRequestReply;
    }
  };

  /**
   * Notification that a name node sends to a client after all the tables have
   * been created.
   */
  struct CreatedAllTablesMsg : public NumberedMsg {
  public:
    CreatedAllTablesMsg() {
      if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
        own_mem_ = true;
        use_stack_buff_ = false;
        mem_.Alloc(get_size());
      } else {
        own_mem_ = false;
        use_stack_buff_ = true;
        mem_.Reset(stack_buff_);
      }
      InitMsg();
    }

    explicit CreatedAllTablesMsg(void *msg):
      NumberedMsg(msg) {}

  protected:
    void InitMsg(){
      NumberedMsg::InitMsg();
      get_msg_type() = kCreatedAllTables;
    }
  };

  /**
   * Send a notification from scheduler and name node thread to notification
   * connection of server.
   */
  struct ConnectServerMsg : public NumberedMsg {
  public:
    ConnectServerMsg() {
      if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
        own_mem_ = true;
        use_stack_buff_ = false;
        mem_.Alloc(get_size());
      } else {
        own_mem_ = false;
        use_stack_buff_ = true;
        mem_.Reset(stack_buff_);
      }
      InitMsg();
    }

    explicit ConnectServerMsg(void *msg):
      NumberedMsg(msg) {}

  protected:
    void InitMsg() {
      NumberedMsg::InitMsg();
      get_msg_type() = kConnectServer;
    }
  }; // end class -- connect server message


  /**
   * Send a notification that client starts sent to scheduler and name node
   * thread.
   */
  struct ClientStartMsg : public NumberedMsg {
  public:
    ClientStartMsg() {
      if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
        own_mem_ = true;
        use_stack_buff_ = false;
        mem_.Alloc(get_size());
      } else {
        own_mem_ = false;
        use_stack_buff_ = true;
        mem_.Reset(stack_buff_);
      }
      InitMsg();
    }

    explicit ClientStartMsg(void *msg):
      NumberedMsg(msg) {}

  protected:
    void InitMsg() {
      NumberedMsg::InitMsg();
      get_msg_type() = kClientStart;
    }
  }; // end class -- send a notification that client is started.


  /**
   * Send a message notifying that the application wants to de-register.
   */
  struct AppThreadDeregMsg : public NumberedMsg {
  public:
    AppThreadDeregMsg() {
      if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
        own_mem_ = true;
        use_stack_buff_ = false;
        mem_.Alloc(get_size());
      } else {
        own_mem_ = false;
        use_stack_buff_ = true;
        mem_.Reset(stack_buff_);
      }
      InitMsg();
    }

    explicit AppThreadDeregMsg(void *msg):
      NumberedMsg(msg) {}

  protected:
    void InitMsg() {
      NumberedMsg::InitMsg();
      get_msg_type() = kAppThreadDereg;
    }
  }; // end class -- application thread de-register


  /**
   */
  struct ClientShutDownMsg : public NumberedMsg {
  public:
    ClientShutDownMsg() {
      if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
        own_mem_ = true;
        use_stack_buff_ = false;
        mem_.Alloc(get_size());
      } else {
        own_mem_ = false;
        use_stack_buff_ = true;
        mem_.Reset(stack_buff_);
      }
      InitMsg();
    }

    explicit ClientShutDownMsg(void *msg):
      NumberedMsg(msg) {}

  protected:
    void InitMsg() {
      NumberedMsg::InitMsg();
      get_msg_type() = kClientShutDown;
    }
  }; // end class - client shutdown message


  /**
   * Acknowledgement to shutdown the server.
   */
  struct ServerShutDownAckMsg : public NumberedMsg {
  public:
    ServerShutDownAckMsg() {
      if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
        own_mem_ = true;
        use_stack_buff_ = false;
        mem_.Alloc(get_size());
      } else {
        own_mem_ = false;
        use_stack_buff_ = true;
        mem_.Reset(stack_buff_);
      }
      InitMsg();
    }

    explicit ServerShutDownAckMsg(void *msg):
      NumberedMsg(msg) {}

  protected:
    void InitMsg() {
      NumberedMsg::InitMsg();
      get_msg_type() = kServerShutDownAck;
    }
  }; // end class -- server shutdown acknowledgement message


  /**
   * Acknowledgement that the server gives after receiving the oplog from a client.
   */
  struct ServerOpLogAckMsg : public NumberedMsg {
  public:
    ServerOpLogAckMsg() {
      if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
        own_mem_ = true;
        use_stack_buff_ = false;
        mem_.Alloc(get_size());
      } else {
        own_mem_ = false;
        use_stack_buff_ = true;
        mem_.Reset(stack_buff_);
      }
      InitMsg();
    }

    explicit ServerOpLogAckMsg (void *msg):
      NumberedMsg(msg) { }

    size_t get_size() {
      return NumberedMsg::get_size() + sizeof(uint32_t);
    }

    uint32_t &get_ack_version() {
      return *(reinterpret_cast<uint32_t*>(
                                           mem_.get_mem() + NumberedMsg::get_size()));
    }

  protected:
    void InitMsg() {
      NumberedMsg::InitMsg();
      get_msg_type() = kServerOpLogAck;
    }
  }; // end class -- server oplog acknowledgement message


  /**
   * Send a acknowledgement from replica after the oplog is received. Useful for
   * chain replication.
   */
  struct ReplicaOpLogAckMsg : public NumberedMsg {
  public:
    ReplicaOpLogAckMsg() {
      if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
        own_mem_ = true;
        use_stack_buff_ = false;
        mem_.Alloc(get_size());
      } else {
        own_mem_ = false;
        use_stack_buff_ = true;
        mem_.Reset(stack_buff_);
      }
      InitMsg();
    }

    explicit ReplicaOpLogAckMsg (void *msg):
      NumberedMsg(msg) { }

    size_t get_size() {
      return NumberedMsg::get_size() + sizeof(uint32_t) + sizeof(int32_t);
    }

    uint32_t &get_ack_version() {
      return *(reinterpret_cast<uint32_t*>(mem_.get_mem()
                                           + NumberedMsg::get_size()));
    }

    int32_t &get_original_sender() {
      return *(reinterpret_cast<int32_t*>(mem_.get_mem()
                                          + NumberedMsg::get_size()
                                          + sizeof(uint32_t)));
    }

  protected:
    void InitMsg() {
      NumberedMsg::InitMsg();
      get_msg_type() = kReplicaOpLogAck;
    }
  }; // end class -- replica oplog ack message


  /**
   * Send a request to scheduler asking for transfer.
   */
  struct TransferRequestMsg : public NumberedMsg {
  public:
    TransferRequestMsg() {
      if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
        own_mem_ = true;
        use_stack_buff_ = false;
        mem_.Alloc(get_size());
      } else {
        own_mem_ = false;
        use_stack_buff_ = true;
        mem_.Reset(stack_buff_);
      }
      InitMsg();
    }

    explicit TransferRequestMsg (void *msg):
      NumberedMsg(msg) { }

    // 1. server thread id
    // 2. size in bytes
    // 3. version
    // 4. norm
    // 5. unique id


    size_t get_size() {
      return NumberedMsg::get_size() +
        sizeof(int32_t) +
        sizeof(int32_t) +
        sizeof(int32_t) +
        sizeof(float) +
        sizeof(int32_t) ;
    }

    int32_t &get_server_id() {
      return *(reinterpret_cast<int32_t*>(mem_.get_mem()
                                           + NumberedMsg::get_size()));
    }

    int32_t &get_gradient_size() {
      return *(reinterpret_cast<int32_t*>(mem_.get_mem()
                                          + NumberedMsg::get_size()
                                          + sizeof(int32_t)));
    }

    int32_t &get_gradient_version() {
      return *(reinterpret_cast<int32_t*>(mem_.get_mem()
                                          + NumberedMsg::get_size()
                                          + sizeof(int32_t)
                                          + sizeof(int32_t)));
    }

    float &get_gradient_norm() {
      return *(reinterpret_cast<float*>(mem_.get_mem()
                                        + NumberedMsg::get_size()
                                        + sizeof(int32_t)
                                        + sizeof(int32_t)
                                        + sizeof(int32_t)));
    }

    int32_t &get_unique_id() {
      return *(reinterpret_cast<int32_t*>(mem_.get_mem()
                                        + NumberedMsg::get_size()
                                        + sizeof(int32_t)
                                        + sizeof(int32_t)
                                        + sizeof(int32_t)
                                        + sizeof(float)
                                        ));
    }

  protected:
    void InitMsg() {
      NumberedMsg::InitMsg();
      get_msg_type() = kTransferRequest;
    }
  }; // end class -- transfer request message


  /**
   * Notification from scheduler to start transfer.
   */
  struct TransferResponseMsg : public NumberedMsg {
  public:
    TransferResponseMsg() {
      if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
        own_mem_ = true;
        use_stack_buff_ = false;
        mem_.Alloc(get_size());
      } else {
        own_mem_ = false;
        use_stack_buff_ = true;
        mem_.Reset(stack_buff_);
      }
      InitMsg();
    }

    explicit TransferResponseMsg (void *msg):
      NumberedMsg(msg) { }

    // 1. new destination, -1 implies drop
    // 2. new rate, in bits per second
    // 3. Unique id

    size_t get_size() {
      return NumberedMsg::get_size() +
        sizeof(int32_t) +
        sizeof(int32_t) +
        sizeof(int32_t)
        ;
    }

    int32_t &get_destination_id() {
      return *(reinterpret_cast<int32_t*>(mem_.get_mem()
                                           + NumberedMsg::get_size()));
    }

    int32_t &get_transmission_rate() {
      return *(reinterpret_cast<int32_t*>(mem_.get_mem()
                                          + NumberedMsg::get_size()
                                          + sizeof(int32_t)));
    }

    int32_t &get_unique_id() {
      return *(reinterpret_cast<int32_t*>(mem_.get_mem()
                                          + NumberedMsg::get_size()
                                          + sizeof(int32_t)
                                          + sizeof(int32_t)));
    }

  protected:
    void InitMsg() {
      NumberedMsg::InitMsg();
      get_msg_type() = kTransferResponse;
    }
  };


  /**
   * Notification that a server sends to the scheduler that a scheduled
   * transfer is complete.
   */
  struct TransferDeliveredMsg : public NumberedMsg {
  public:
    TransferDeliveredMsg() {
      if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
        own_mem_ = true;
        use_stack_buff_ = false;
        mem_.Alloc(get_size());
      } else {
        own_mem_ = false;
        use_stack_buff_ = true;
        mem_.Reset(stack_buff_);
      }
      InitMsg();
    }

    // 1. new destination, -1 implies drop
    // 2. new rate, in bits per second
    // 3. Unique id
    explicit TransferDeliveredMsg (void *msg):
      NumberedMsg(msg) { }

    size_t get_size() {
      return NumberedMsg::get_size() +
        sizeof(int32_t) +
        sizeof(int32_t) +
        sizeof(int32_t)
        ;
    }

    int32_t &get_server_id() {
      return *(reinterpret_cast<int32_t*>(mem_.get_mem()
                                           + NumberedMsg::get_size()));
    }

    int32_t &get_worker_id() {
      return *(reinterpret_cast<int32_t*>(mem_.get_mem()
                                          + NumberedMsg::get_size()
                                          + sizeof(int32_t)));
    }

    int32_t &get_unique_id() {
      return *(reinterpret_cast<int32_t*>(mem_.get_mem()
                                          + NumberedMsg::get_size()
                                          + sizeof(int32_t)
                                          + sizeof(int32_t)));
    }

  protected:
    void InitMsg() {
      NumberedMsg::InitMsg();
      get_msg_type() = kTransferDelivered;
    }
  }; // end class -- Transfer delivered message


  /**
   */
  struct BgClockMsg : public NumberedMsg {
  public:
    BgClockMsg() {
      if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
        own_mem_ = true;
        use_stack_buff_ = false;
        mem_.Alloc(get_size());
      } else {
        own_mem_ = false;
        use_stack_buff_ = true;
        mem_.Reset(stack_buff_);
      }
      InitMsg();
    }

    explicit BgClockMsg(void *msg):
      NumberedMsg(msg) {}

  protected:
    void InitMsg() {
      NumberedMsg::InitMsg();
      get_msg_type() = kBgClock;
    }
  };


  /**
   */
  struct BgSendOpLogMsg : public NumberedMsg {
  public:
    BgSendOpLogMsg() {
      if (get_size() > PETUUM_MSG_STACK_BUFF_SIZE) {
        own_mem_ = true;
        use_stack_buff_ = false;
        mem_.Alloc(get_size());
      } else {
        own_mem_ = false;
        use_stack_buff_ = true;
        mem_.Reset(stack_buff_);
      }
      InitMsg();
    }

    explicit BgSendOpLogMsg(void *msg):
      NumberedMsg(msg) {}

  protected:
    void InitMsg() {
      NumberedMsg::InitMsg();
      get_msg_type() = kBgSendOpLog;
    }
  };


  /**
   * Reply from server having all the model parameters from a server.
   */
  struct ServerBulkRowRequestReplyMsg : public ArbitrarySizedMsg {
  public:
    explicit ServerBulkRowRequestReplyMsg(int32_t avai_size) {
      own_mem_ = true;
      mem_.Alloc(get_header_size() + avai_size);
      InitMsg(avai_size);
    }

    explicit ServerBulkRowRequestReplyMsg(void *msg):
      ArbitrarySizedMsg(msg) {}

    size_t get_header_size() {
      return ArbitrarySizedMsg::get_header_size()
        + sizeof(int32_t)
        ;
    }

    int32_t &get_table_id() {
      return *(reinterpret_cast<int32_t*>(mem_.get_mem()
                                          + ArbitrarySizedMsg::get_header_size()));
    }

    void *get_row_data() {
      return mem_.get_mem() + get_header_size();
    }

    size_t get_size() {
      return get_header_size() + get_avai_size();
    }

  protected:
    virtual void InitMsg(int32_t avai_size) {
      ArbitrarySizedMsg::InitMsg(avai_size);
      get_msg_type() = kBulkServerRowRequestReply;
    }
  }; // end class -- bulk row request reply message



  /**
   * Reply from the server having the model values. We currently focus only on
   * split based on row_id.
   */
  struct ServerRowRequestReplyMsg : public ArbitrarySizedMsg {

  public:

    explicit ServerRowRequestReplyMsg(int32_t avai_size) {
      own_mem_ = true;
      mem_.Alloc(get_header_size() + avai_size);
      InitMsg(avai_size);
    }

    explicit ServerRowRequestReplyMsg(void *msg):
      ArbitrarySizedMsg(msg) {}

    size_t get_header_size() {
      return ArbitrarySizedMsg::get_header_size()
        + sizeof(int32_t) // table id
        + sizeof(int32_t) // row id
        + sizeof(int32_t) // clock
        + sizeof(uint32_t) // version (worker specific)
        + sizeof(size_t) // row size
        + sizeof(int32_t) // global model version
        ;
    }

    int32_t &get_table_id() {
      return *(reinterpret_cast<int32_t*>(mem_.get_mem()
                                          + ArbitrarySizedMsg::get_header_size()));
    }

    int32_t &get_row_id() {
      return *(reinterpret_cast<int32_t*>(mem_.get_mem()
                                          + ArbitrarySizedMsg::get_header_size()
                                          + sizeof(int32_t) ));
    }

    int32_t &get_clock() {
      return *(reinterpret_cast<int32_t*>(mem_.get_mem()
                                          + ArbitrarySizedMsg::get_header_size()
                                          + sizeof(int32_t)
                                          + sizeof(int32_t) ));
    }

    uint32_t &get_version() {
      return *(reinterpret_cast<uint32_t*>(mem_.get_mem()
                                           + ArbitrarySizedMsg::get_header_size()
                                           + sizeof(int32_t)
                                           + sizeof(int32_t)
                                           +sizeof(int32_t)));
    }

    size_t &get_row_size() {
      return *(reinterpret_cast<size_t*>(mem_.get_mem()
                                         + ArbitrarySizedMsg::get_header_size()
                                         + sizeof(int32_t)
                                         + sizeof(int32_t)
                                         + sizeof(int32_t)
                                         + sizeof(uint32_t)));
    }

    int32_t &get_global_model_version() {
      return *(reinterpret_cast<int32_t*>(mem_.get_mem()
                                         + ArbitrarySizedMsg::get_header_size()
                                         + sizeof(int32_t)
                                         + sizeof(int32_t)
                                         + sizeof(int32_t)
                                         + sizeof(uint32_t)
                                         + sizeof(size_t)));
    }

    void *get_row_data() {
      return mem_.get_mem() + get_header_size();
    }

    size_t get_size() {
      return get_header_size() + get_avai_size();
    }

  protected:
    virtual void InitMsg(int32_t avai_size) {
      ArbitrarySizedMsg::InitMsg(avai_size);
      get_msg_type() = kServerRowRequestReply;
    }
  }; // end class - Server Row Request reply message


  /**
   * Message with the gradient updates that an client sends to the server.
   */
  struct ClientSendOpLogMsg : public ArbitrarySizedMsg {

  public:

    explicit ClientSendOpLogMsg(int32_t avai_size) {
      own_mem_ = true;
      mem_.Alloc(get_header_size() + avai_size);
      InitMsg(avai_size);
    }

    explicit ClientSendOpLogMsg(void *msg):
      ArbitrarySizedMsg(msg) {}

    size_t get_header_size() {
      return ArbitrarySizedMsg::get_header_size()
        + sizeof(bool) // a bit to denote whether the message is clock or not
        + sizeof(int32_t) // a 32 bit int to get the client id
        + sizeof(uint32_t) // a 32 bit unsigned int to denote the local version
        + sizeof(int32_t) // a 32 bit int to get clock value
        + sizeof(int32_t) // a 32 bit int to store the global model version number on which gradient is calculated
        ;
    }

    bool &get_is_clock() {
      // the indicator is stored immediately after Arbitrary Size Msg header
      // 1st value
      return *(reinterpret_cast<bool*>(mem_.get_mem()
                                       + ArbitrarySizedMsg::get_header_size()));
    }

    int32_t &get_client_id() {
      // 2nd value
      return *(reinterpret_cast<int32_t*>(mem_.get_mem()
                                          + ArbitrarySizedMsg::get_header_size()
                                          + sizeof(bool)));
    }

    uint32_t &get_version() {
      // 3rd value after Arbitrary sized message header
      return *(reinterpret_cast<uint32_t*>(mem_.get_mem()
                                           + ArbitrarySizedMsg::get_header_size()
                                           + sizeof(bool)
                                           + sizeof(int32_t)));
    }

    int32_t &get_bg_clock() {
      return *(reinterpret_cast<int32_t*>(mem_.get_mem()
                                          + ArbitrarySizedMsg::get_header_size()
                                          + sizeof(bool)
                                          + sizeof(int32_t)
                                          + sizeof(uint32_t)));
    }

    int32_t &get_global_model_version() {
      return *(reinterpret_cast<int32_t*>(mem_.get_mem()
                                          + ArbitrarySizedMsg::get_header_size()
                                          + sizeof(bool)
                                          + sizeof(int32_t)
                                          + sizeof(uint32_t)
                                          + sizeof(int32_t)));
    }

    // data is to be accessed via SerializedOpLogAccessor
    void *get_data() {
      return mem_.get_mem() + get_header_size();
    }

    size_t get_size() {
      return get_header_size() + get_avai_size();
    }

  protected:

    virtual void InitMsg(int32_t avai_size) {
      ArbitrarySizedMsg::InitMsg(avai_size);
      get_msg_type() = kClientSendOpLog;
    }

  }; // end class - Client send operation log message


  /**
   * Message that the server sends to the replica server, after receiving an
   * oplog from client.
   */
  struct ServerSendOpLogMsg : public ArbitrarySizedMsg {

  public:

    explicit ServerSendOpLogMsg(int32_t avai_size) {
      own_mem_ = true;
      mem_.Alloc(get_header_size() + avai_size);
      InitMsg(avai_size);
    }

    explicit ServerSendOpLogMsg(void *msg):
      ArbitrarySizedMsg(msg) {}

    size_t get_header_size() {
      return ArbitrarySizedMsg::get_header_size()
        + sizeof(int32_t) // a 32 bit int to get the original sender
        + sizeof(uint32_t) // a 32 bit unsigned int to denote the local version
        + sizeof(int32_t) // a 32 bit int to store the global model version number on which gradient is calculated
        ;
    }

    int32_t &get_original_sender_id() {
      // 1st value
      return *(reinterpret_cast<int32_t*>(mem_.get_mem()
                                          + ArbitrarySizedMsg::get_header_size()));
    }

    uint32_t &get_original_version() {
      // 2nd value after Arbitrary sized message header
      return *(reinterpret_cast<uint32_t*>(mem_.get_mem()
                                           + ArbitrarySizedMsg::get_header_size()
                                           + sizeof(int32_t)));
    }

    int32_t &get_global_model_version() {
      return *(reinterpret_cast<int32_t*>(mem_.get_mem()
                                          + ArbitrarySizedMsg::get_header_size()
                                          + sizeof(int32_t)
                                          + sizeof(uint32_t)));
    }

    // data is to be accessed via SerializedOpLogAccessor
    void *get_data() {
      return mem_.get_mem() + get_header_size();
    }

    size_t get_size() {
      return get_header_size() + get_avai_size();
    }

  protected:

    virtual void InitMsg(int32_t avai_size) {
      ArbitrarySizedMsg::InitMsg(avai_size);
      get_msg_type() = kServerSendOpLog;
    }

  }; // end class - Server send operation log message to replica

}  // namespace petuum
