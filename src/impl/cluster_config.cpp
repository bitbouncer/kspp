#include <kspp/cluster_config.h>
#include <boost/filesystem.hpp>
#include <glog/logging.h>
#include <kspp/utils/cluster_uri.h>
#include <kspp/utils/env.h>

namespace kspp {
  cluster_config::cluster_config()
      : producer_buffering_(std::chrono::milliseconds(1000)), consumer_buffering_(std::chrono::milliseconds(1000)),
        schema_registry_timeout_(std::chrono::milliseconds(1000)), _fail_fast(true) {
  }

  void cluster_config::load_config_from_env() {
    set_brokers(default_kafka_broker_uri());
    set_storage_root(default_statestore_directory());
    //set_consumer_buffering_time()
    //set_producer_buffering_time
    set_ca_cert_path(default_ca_cert_path());
    set_private_key_path(default_client_cert_path(),
                         default_client_key_path(),
                         default_client_key_passphrase());
    set_schema_registry(default_schema_registry_uri());
    //set_schema_registry_timeout()
    //set_fail_fast()
  }

  std::string cluster_config::get_brokers() const {
    return brokers_;
  }

  void cluster_config::set_brokers(std::string brokers) {
    cluster_uri cu(brokers, "plaintext");
    LOG_IF(FATAL, !cu.good()) << "cluster_config, bad broker config - bad uri: " << brokers;
    LOG_IF(FATAL, cu.path() != "") << "cluster_config, bad broker config - cannot have a path here: " << brokers;
    brokers_ = cu.str();
  }

  void cluster_config::set_storage_root(std::string root_path) {
    if (!boost::filesystem::exists(root_path)) {
      auto res = boost::filesystem::create_directories(root_path);
      // seems to be a bug in boost - always return false...
      if (!boost::filesystem::exists(root_path))
        LOG(FATAL) << "cluster_config, failed to create storage path at : " << root_path;
    }
    root_path_ = root_path;
  }

  std::string cluster_config::get_storage_root() const {
    return root_path_;
  }

  std::string cluster_config::get_ca_cert_path() const {
    return ca_cert_path_;
  }

  void cluster_config::set_ca_cert_path(std::string path) {
    if (!boost::filesystem::exists(path)) {
      LOG(WARNING) << "cluster_config, ca cert not found at: " << path << " ignoring ssl config";
    } else {
      ca_cert_path_ = path;
    }
  }

  void cluster_config::set_private_key_path(std::string client_cert_path, std::string private_key_path,
                                            std::string passprase) {
    bool all_ok = true;
    if (!boost::filesystem::exists(private_key_path)) {
      LOG(WARNING) << "cluster_config, private_key_path not found at: " << private_key_path;
      all_ok = false;
    }

    if (!boost::filesystem::exists(client_cert_path_)) {
      LOG(WARNING) << "cluster_config, client_cert not found at: " << client_cert_path;
      all_ok = false;
    }

    if (!all_ok) {
      LOG(WARNING) << "cluster_config, ssl client auth config incomplete, ignoring config";
    } else {
      client_cert_path_ = client_cert_path;
      private_key_path_ = private_key_path;
      private_key_passphrase_ = passprase;
    }
  }

  std::string cluster_config::get_client_cert_path() const {
    return client_cert_path_;
  }

  std::string cluster_config::get_private_key_path() const {
    return private_key_path_;
  }

  std::string cluster_config::get_private_key_passphrase() const {
    return private_key_passphrase_;
  }


  void cluster_config::set_schema_registry(std::string uri) {
    cluster_uri cu(uri, "http");
    LOG_IF(FATAL, !cu.good()) << "cluster_config, bad schema registry uri: " << uri;
    schema_registry_uri_ = cu.str();
  }

  std::string cluster_config::get_schema_registry() const {
    return schema_registry_uri_;
  }

  void cluster_config::set_schema_registry_timeout(std::chrono::milliseconds timeout) {
    schema_registry_timeout_ = timeout;
  }

  std::chrono::milliseconds cluster_config::get_schema_registry_timeout() const {
    return schema_registry_timeout_;
  }

  void cluster_config::set_consumer_buffering_time(std::chrono::milliseconds timeout) {
    consumer_buffering_ = timeout;
  }

  std::chrono::milliseconds cluster_config::get_consumer_buffering_time() const {
    return consumer_buffering_;
  }

  void cluster_config::set_producer_buffering_time(std::chrono::milliseconds timeout) {
    producer_buffering_ = timeout;
  }

  std::chrono::milliseconds cluster_config::get_producer_buffering_time() const {
    return producer_buffering_;
  }


  void cluster_config::set_fail_fast(bool state) {
    _fail_fast = state;
  }

  bool cluster_config::get_fail_fast() const {
    return _fail_fast;
  }

  void cluster_config::validate() const {
    LOG_IF(FATAL, brokers_.size() == 0) << "cluster_config, no brokers defined";
    {
      cluster_uri cu(brokers_);
      if (cu.scheme() == "ssl") {
        LOG_IF(FATAL, ca_cert_path_.size() == 0) << "cluster_config, brokers using ssl and no ca cert configured";
      }
    }

    if (schema_registry_uri_.size() > 0) {
      cluster_uri cu(schema_registry_uri_);
      if (cu.scheme() == "https") {
        LOG_IF(FATAL, ca_cert_path_.size() == 0) << "cluster_config, schema registry using https and no ca cert configured";
      }
    }
  }




  void cluster_config::log() const {
    LOG(INFO) << "cluster_config, kafka broker(s): " << get_brokers();
    LOG_IF(INFO, get_ca_cert_path().size() > 0) << "cluster_config, ca cert: " << get_ca_cert_path();
    LOG_IF(INFO, get_client_cert_path().size() > 0) << "cluster_config, client cert: " << get_client_cert_path();
    LOG_IF(INFO, get_private_key_path().size() > 0) << "cluster_config, client key: " << get_private_key_path();
    LOG_IF(INFO, get_private_key_passphrase().size() > 0) << "cluster_config, client key passphrase: [withheld]";

    LOG_IF(INFO, get_storage_root().size() > 0) << "cluster_config, storage root: " << get_storage_root();
    LOG(INFO) << "cluster_config, kafka consumer_buffering_time: " << get_consumer_buffering_time().count() << " ms";
    LOG(INFO) << "cluster_config, kafka producer_buffering_time: " << get_producer_buffering_time().count() << " ms";


    LOG_IF(INFO, get_schema_registry().size() > 0) << "cluster_config, schema_registry: " << get_schema_registry();
    LOG_IF(INFO, get_schema_registry().size() > 0) << "cluster_config, schema_registry_timeout: " << get_schema_registry_timeout().count() << " ms";


  }
}