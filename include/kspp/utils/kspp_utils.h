#include <vector>
#include <string>
#pragma once

namespace kspp {
  std::string sanitize_filename(std::string s);
  std::vector<int32_t> parse_partition_list(std::string s);
  std::vector<int32_t> get_partition_list(int32_t nr_of_partitions);
  std::string partition_list_to_string(std::vector<int32_t> v);
}