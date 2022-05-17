#include <iostream>
#include <glog/logging.h>
#include <google/protobuf/compiler/importer.h>
#include <kspp/avro/avro_schema_registry.h>
#include <kspp/cluster_config.h>

using namespace google::protobuf::compiler;

class MyMultiFileErrorCollector : public MultiFileErrorCollector {
public:
  MyMultiFileErrorCollector(){}
  virtual void AddError(const std::string & filename, int line, int column, const std::string & message){
  LOG(ERROR) << filename << " line:" << line << ":" << column << " " << message;
  }
};

int main(int argc, char** argv) {
  using namespace google::protobuf;
  using namespace google::protobuf::io;
  using namespace google::protobuf::compiler;

  if (argc!=4){
    std::cerr << "usage " << argv[0] << "source_root_path" ".protofile" "subject" << std::endl;
    return -1;
  }

  std::string source_path = argv[1];
  std::string proto_source = argv[2];
  std::string subject = argv[3];

  MyMultiFileErrorCollector ec;
  DiskSourceTree source_tree;
  source_tree.MapPath("", source_path);
  source_tree.MapPath("", "/usr/local/include/");
  source_tree.MapPath("", "/usr/include/");
  Importer importer(&source_tree, &ec);
  auto file_descriptor = importer.Import(proto_source);
  LOG(INFO) << file_descriptor->DebugString();
  //auto config = std::make_shared<kspp::cluster_config>("", kspp::cluster_config::NONE);
  auto config = std::make_shared<kspp::cluster_config>("");
  config->load_config_from_env();
  nlohmann::json json = kspp::protobuf_register_schema(config->get_schema_registry(), subject, file_descriptor);
}