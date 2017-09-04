#include <kspp/cluster_config.h>
#include <boost/filesystem.hpp>
#include <glog/logging.h>
#include <kspp/utils/cluster_uri.h>

namespace kspp {
  cluster_config::cluster_config()
      : producer_buffering_(std::chrono::milliseconds(1000))
  , conumer_buffering_(std::chrono::milliseconds(1000)){
  }

  void cluster_config::set_brokers(std::string brokers){
    cluster_uri cu(brokers, "plaintext");
    LOG_IF(FATAL, !cu.good()) << "bad broker config - bad uri: " << brokers;
    LOG_IF(FATAL, cu.path() != "") << "bad broker config - cannot have a path here: " << brokers;
    brokers_ = cu.str();
  }

  void cluster_config::set_storage_root(std::string root_path){
    if (!boost::filesystem::exists(root_path)) {
      auto res = boost::filesystem::create_directories(root_path);
      // seems to be a bug in boost - always return false...
      if (!boost::filesystem::exists(root_path))
        LOG(FATAL) << "failed to create storage path at : " << root_path;
    }
    root_path_ = root_path;
  }

  void cluster_config::set_ca_cert_path(std::string path){
    LOG_IF(FATAL, !boost::filesystem::exists(path)) << "ca cert not found at: " << path;
    ca_cert_path_ = path;
  }

  void cluster_config::set_private_key_path(std::string client_cert_path, std::string private_key_path, std::string passprase){
    LOG_IF(FATAL, !boost::filesystem::exists(private_key_path)) << "client_cert not found at: " << client_cert_path;
    LOG_IF(FATAL, !boost::filesystem::exists(private_key_path)) << "private key not found at: " << private_key_path;
    client_cert_path_ = client_cert_path;
    private_key_path_ = private_key_path;
    private_key_passphrase_ = passprase;
  }

  void cluster_config::set_schema_registry(std::string uri){
    cluster_uri cu(uri, "http");
    LOG_IF(FATAL, !cu.good()) << "bad schema registry uri: " << uri;
    schema_registry_uri_ = cu.str();
  }

  void cluster_config::set_consumer_buffering_time(std::chrono::milliseconds timeout){
    conumer_buffering_ = timeout;
  }

  void cluster_config::set_producer_buffering_time(std::chrono::milliseconds timeout){
    producer_buffering_ = timeout;
  }

  void cluster_config::validate() const
  {
    LOG_IF(FATAL, brokers_.size()==0) << "no brokers defined";
    {
      cluster_uri cu(brokers_);
      if (cu.scheme() == "ssl") {
        LOG_IF(FATAL, ca_cert_path_.size() == 0) << "brokers using ssl and no ca cert configured";
      }
    }
    if (schema_registry_uri_.size()>0) {
      cluster_uri cu(schema_registry_uri_);
      if (cu.scheme() == "ssl") {
        LOG_IF(FATAL, ca_cert_path_.size() == 0) << "schema registry using ssl and no ca cert configured";
      }
    }
  }
}