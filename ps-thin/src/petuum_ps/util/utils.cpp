
#include <petuum_ps/util/utils.hpp>

#include <utility>
#include <fstream>
#include <iostream>
#include <glog/logging.h>

namespace petuum {

void GetHostInfos(std::string server_file,
                  std::map<int32_t, HostInfo> *host_map) {
  std::map<int32_t, HostInfo> &servers = *host_map;

  std::ifstream input(server_file.c_str());
  std::string line;
  while (std::getline(input, line)) {

    size_t pos = line.find_first_of("\t ");
    std::string idstr = line.substr(0, pos);

    size_t pos_ip = line.find_first_of("\t ", pos + 1);
    std::string ip = line.substr(pos + 1, pos_ip - pos - 1);
    std::string port = line.substr(pos_ip + 1);

    int32_t id = atoi(idstr.c_str());
    servers.insert(std::make_pair(id, petuum::HostInfo(id, ip, port)));
    // VLOG(0) << "get server: " << id << ":" << ip << ":" << port;
  }
  input.close();
}

// assuming the namenode id is 0
void GetServerIDsFromHostMap(std::vector<int32_t> *server_ids,
                             const std::map<int32_t, HostInfo> &host_map) {

  unsigned long num_servers = host_map.size() - 1;
  server_ids->resize(num_servers);
  int32_t i = 0;

  for (const auto &host_info_iter : host_map) {
    if (host_info_iter.first == 0)
      continue;
    (*server_ids)[i] = host_info_iter.first;
    ++i;
  }
}

std::string GetTableRowStringId(int32_t table_id, int32_t row_id) {
  std::stringstream retstream;
  retstream << "Table=" << table_id << " "
            << "Row=" << row_id;
  return retstream.str();
}

} // namespace petuum
