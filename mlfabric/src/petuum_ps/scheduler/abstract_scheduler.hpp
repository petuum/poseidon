#pragma once

#include <petuum_ps/scheduler/scheduler_utils.hpp>


namespace petuum {
  class AbstractScheduler {
  public:
    virtual ~AbstractScheduler() { }
    virtual void Init(std::vector<int32_t> &server_ids) = 0;
    virtual void UpdateTransferRequest(int32_t bg_id,
                                       int32_t server_id,
                                       int32_t gradient_size,
                                       int32_t client_version_id,
                                       int32_t bg_unique_id) = 0;
    virtual void UpdateTransferComplete(int32_t bg_id,
                                        int32_t server_id,
                                        int32_t bg_unique_id) = 0;
    virtual bool HasResponse(int32_t server_id) = 0;
    virtual ClientRequestAggregate GetResponse(int32_t server_id) = 0;
    virtual bool HasDiscardResponse() = 0;
    virtual void GetDiscardResponse() = 0;
  };
}
