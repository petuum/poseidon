#pragma once

#include <petuum_ps/thread/ps_msgs.hpp>
#include <petuum_ps/comm_bus/comm_bus.hpp>
#include <boost/unordered_map.hpp>
#include <petuum_ps/thread/context.hpp>

namespace petuum {

  class StoredValue {
  public:
    StoredValue(int32_t bg_id, int32_t unique_id, int32_t server_id);
    int32_t bg_id_;
    int32_t unique_id_;
    int32_t destination_server_id_;
  };



  class ClientRequestAggregate {
  public:

    ClientRequestAggregate(int32_t worker_client_id, int32_t server_client_id);

    /**
       Copy constructor.
     */
    ClientRequestAggregate(ClientRequestAggregate *other);

    ~ClientRequestAggregate();

    void AddRequest(int32_t bg_id,
                    int32_t server_id,
                    int32_t gradient_size,
                    int32_t client_version_id,
                    int32_t bg_unique_id) ;
    void Reset() ;
    int32_t Count() ;
    int32_t GetWorkerID(int index) ;

    int32_t GetServerID(int index) ;

    int32_t GetUniqueID(int index) ;

    int32_t GetVersion() ;

    int32_t GetSize();

    std::string toString();

  private:
    int32_t worker_client_id_;
    int32_t server_client_id_;
    std::vector<int32_t> bg_ids_;
    std::vector<int32_t> server_ids_;
    std::vector<int32_t> bg_unique_ids_;
    std::vector<int32_t> client_version_ids_;
    std::vector<int32_t> gradient_sizes_;
    int32_t total_size_;
    int32_t min_version_;
  };


  struct AggregateComparator {
    bool operator()(ClientRequestAggregate* a, ClientRequestAggregate* b);
  };

}
