#include <petuum_ps/scheduler/delay_scheduler.hpp>

namespace petuum {

  LowestDelayScheduler::LowestDelayScheduler() {}

  LowestDelayScheduler::~LowestDelayScheduler() {
    // TODO iterate over aggregates_ and delete the value
  }

  void LowestDelayScheduler::Init(std::vector<int32_t> &server_ids) {
    for(auto server_id : server_ids) {
      int32_t server_client_id = GlobalContext::thread_id_to_client_id(server_id);
      if(destination_entities_.find(server_client_id) != destination_entities_.end()) {
        // present already
        continue;
      }
      destination_entities_.insert(server_client_id);
      pending_[server_client_id] = 0;
    }
    VLOG(5) << "Finished initializing the LowestDelayScheduler";
  }

  /**
   */
  void LowestDelayScheduler::UpdateTransferRequest(int32_t bg_id,
                                                   int32_t server_id,
                                                   int32_t gradient_size,
                                                   int32_t client_version_id,
                                                   int32_t bg_unique_id) {

    int32_t worker_client_id = GlobalContext::thread_id_to_client_id(bg_id);
    int32_t server_client_id = GlobalContext::thread_id_to_client_id(server_id);

    // create a new aggregate if none present
    if(aggregates_[server_client_id].find(worker_client_id) == aggregates_[server_client_id].end()) {
      aggregates_[server_client_id][worker_client_id] = new ClientRequestAggregate(worker_client_id, server_client_id);
    }

    aggregates_[server_client_id][worker_client_id]->AddRequest(bg_id,
                                                                server_id,
                                                                gradient_size,
                                                                client_version_id,
                                                                bg_unique_id);

    // only after receiving the updates from all the workers of a worker
    // client, is it even considered to be scheduled.
    if(aggregates_[server_client_id][worker_client_id]->Count() == GlobalContext::get_num_comm_channels_per_client()) {
      queues_[server_client_id].push_back(aggregates_[server_client_id][worker_client_id]);
      VLOG(20) << "Pushed client aggregate to back of queue. worker_client=" << worker_client_id
               << " server_client=" << server_client_id;
      std::push_heap(queues_[server_client_id].begin(), queues_[server_client_id].end(), AggregateComparator());
    }

  } // end function -- update transfer request


    /**
     */
  void LowestDelayScheduler::UpdateTransferComplete(int32_t bg_id,
                                                    int32_t server_id,
                                                    int32_t bg_unique_id) {
    int32_t server_client_id = GlobalContext::thread_id_to_client_id(server_id);
    pending_[server_client_id]--;
  }


  /**
   */
  bool LowestDelayScheduler::HasResponse(int32_t server_id) {
    int32_t server_client_id = GlobalContext::thread_id_to_client_id(server_id);
    return (pending_[server_client_id] <= 8) && (0 != queues_[server_client_id].size());
  }


  /**
   */
  ClientRequestAggregate LowestDelayScheduler::GetResponse(int32_t server_id) {
    int32_t server_client_id = GlobalContext::thread_id_to_client_id(server_id);

    ClientRequestAggregate* next_agg = queues_[server_client_id].front();

    std::pop_heap(queues_[server_client_id].begin(), queues_[server_client_id].end(), AggregateComparator());
    queues_[server_client_id].pop_back();
    VLOG(20) << "Re-heapify the queue.";

    VLOG(20) << "Creating a duplicate of aggregate: " << next_agg->toString();
    ClientRequestAggregate retval(*next_agg);
    VLOG(20) << "Client request aggregate response: " << retval.toString();
    next_agg->Reset(); // reset the current one
    pending_[server_client_id] += retval.Count();
    return retval;
  }


  /**
   */
  bool LowestDelayScheduler::HasDiscardResponse() {
    return false;
  }

  void LowestDelayScheduler::GetDiscardResponse() {
  }
}
