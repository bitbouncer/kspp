#include <vector>
#include <string>
#pragma once

namespace kspp {
  std::string sanitize_filename(std::string s);

  std::vector<int> parse_partition_list(std::string s);

  std::vector<int> get_partition_list(int32_t nr_of_partitions);

  std::string partition_list_to_string(std::vector<int> v);
}