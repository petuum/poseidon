#pragma once

#include <petuum_ps/thread/ps_msgs.hpp>
#include <petuum_ps/scheduler/abstract_scheduler.hpp>
#include <petuum_ps/scheduler/scheduler_utils.hpp>
#include <boost/unordered_map.hpp>


namespace petuum {

  class LowestDelayScheduler : public AbstractScheduler {
  public:
    LowestDelayScheduler() ;

    ~LowestDelayScheduler() ;

    void Init(std::vector<int32_t> &server_ids) ;

    void UpdateTransferRequest(int32_t bg_id,
                               int32_t server_id,
                               int32_t gradient_size,
                               int32_t client_version_id,
                               int32_t bg_unique_id) ;

    void UpdateTransferComplete(int32_t bg_id,
                                int32_t server_id,
                                int32_t bg_unique_id) ;

    bool HasResponse(int32_t server_id) ;

    ClientRequestAggregate GetResponse(int32_t server_id);

    bool HasDiscardResponse();

    void GetDiscardResponse();

  private:
    std::set<int32_t> destination_entities_;
    // indexed by a server client
    boost::unordered_map<int32_t, std::deque<ClientRequestAggregate*> > queues_;
    // server client
    boost::unordered_map<int32_t, int32_t> pending_;
    // server client x worker client
    boost::unordered_map<int32_t, boost::unordered_map<int32_t, ClientRequestAggregate*> >aggregates_;
  };

}
