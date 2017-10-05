#include <petuum_ps/thread/context.hpp>

namespace petuum {

__thread ThreadContext::Info *ThreadContext::thr_info_;

CommBus *GlobalContext::comm_bus;

TableGroupConfig GlobalContext::table_group_config_;

int32_t GlobalContext::num_table_threads_ = 1;

HostInfo GlobalContext::name_node_host_info_;

std::map<int32_t, HostInfo> GlobalContext::server_map_;

std::vector<int32_t> GlobalContext::server_ids_;

std::vector<int32_t> GlobalContext::server_clients_;

std::vector<int32_t> GlobalContext::worker_clients_;

} // namespace petuum
