#include <thread>
#include <kspp/cluster_config.h>
#include <boost/filesystem.hpp>
#include <glog/logging.h>
#include <kspp/utils/url_parser.h>
#include <kspp/utils/env.h>
#include <kspp/cluster_metadata.h>


using namespace std::chrono_literals;

namespace kspp {
  cluster_config::cluster_config(std::string consumer_group)
      : consumer_group_(consumer_group)
      , min_topology_buffering_(std::chrono::milliseconds(1000))
      , producer_buffering_(std::chrono::milliseconds(1000))
      , producer_message_timeout_(std::chrono::milliseconds(0))
      , consumer_buffering_(std::chrono::milliseconds(1000))
      , schema_registry_timeout_(std::chrono::milliseconds(10000))
      , cluster_state_timeout_(std::chrono::seconds(60))
      , max_pending_sink_messages_(50000)
      , fail_fast_(true) {
  }

  void cluster_config::load_config_from_env() {
    set_brokers(default_kafka_broker_uri());
    set_storage_root(default_statestore_root());
    //set_consumer_buffering_time()
    //set_producer_buffering_time
    set_ca_cert_path(default_ca_cert_path());
    set_private_key_path(default_client_cert_path(),
                         default_client_key_path(),
                         default_client_key_passphrase());
    set_schema_registry_uri(default_schema_registry_uri());
    set_kafka_rest_uri(default_kafka_rest_uri());
    //set_schema_registry_timeout()
    //set_fail_fast()
  }

  std::string cluster_config::get_brokers() const {
    return brokers_;
  }

  std::string cluster_config::get_consumer_group() const {
    return consumer_group_;
  }

  void cluster_config::set_brokers(std::string brokers) {
    auto v = kspp::split_url_list(brokers, "plaintext");

    LOG_IF(FATAL, v.size() == 0) << "cluster_config, bad broker config - bad uri: " << brokers;
    brokers_ = brokers;
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
      LOG(WARNING) << "cluster_config, ca_cert not found at: " << path << " ignoring ssl config";
    } else {
      ca_cert_path_ = path;
    }
  }

  void cluster_config::set_private_key_path(std::string client_cert_path, std::string private_key_path,
                                            std::string passprase) {
    bool all_ok = true;
    if (!boost::filesystem::exists(private_key_path)) {
      LOG(WARNING) << "cluster_config, private_key_path not found at:" << private_key_path;
      all_ok = false;
    }

    //boost::filesystem::path p(client_cert_path);
    if (boost::filesystem::exists(client_cert_path) == false) {
      LOG(WARNING) << "cluster_config, client_cert not found at:" << client_cert_path;
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

  void cluster_config::set_schema_registry_uri(std::string urls) {
    auto v = kspp::split_url_list(urls, "http");
    LOG_IF(FATAL, v.size() == 0) << "cluster_config, bad schema registry urls: " << urls;
    schema_registry_uri_ = urls;
  }

  std::string cluster_config::get_schema_registry_uri() const {
    return schema_registry_uri_;
  }

  void cluster_config::set_kafka_rest_uri(std::string urls) {
    auto v = kspp::split_url_list(urls, "http");
    LOG_IF(FATAL, v.size() == 0) << "cluster_config, bad kafka_rest urls: " << urls;
    kafka_rest_uri_ = urls;
  }

  std::string cluster_config::get_kafka_rest_uri() const {
    return kafka_rest_uri_;
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

  void cluster_config::set_producer_message_timeout(std::chrono::milliseconds timeout) {
    producer_message_timeout_ = timeout;
  }

  std::chrono::milliseconds cluster_config::get_producer_message_timeout() const {
    return producer_message_timeout_;
  }

  void cluster_config::set_min_topology_buffering(std::chrono::milliseconds timeout) {
    min_topology_buffering_ = timeout;
  }

  std::chrono::milliseconds cluster_config::get_min_topology_buffering() const{
    return min_topology_buffering_;
  }

  void cluster_config::set_max_pending_sink_messages(size_t sz){
    max_pending_sink_messages_ = sz;
  }

  size_t cluster_config::get_max_pending_sink_messages() const {
    return max_pending_sink_messages_;
  }

  void cluster_config::set_fail_fast(bool state) {
    fail_fast_ = state;
  }

  bool cluster_config::get_fail_fast() const {
    return fail_fast_;
  }

  std::shared_ptr<cluster_metadata> cluster_config::get_cluster_metadata() const {
    if (meta_data_==nullptr)
      meta_data_ = std::make_shared<cluster_metadata>(this);
    return meta_data_;
  }

  void cluster_config::set_cluster_state_timeout(std::chrono::seconds timeout){
    cluster_state_timeout_ = timeout;
  }
  std::chrono::seconds cluster_config::get_cluster_state_timeout() const {
    return cluster_state_timeout_;
  }

  std::shared_ptr<kspp::avro_serdes> cluster_config::avro_serdes(bool relaxed_parsing)
  {
    //TODO some error handling would be fine...
    if (!avro_serdes_)
    {
      avro_schema_registry_ =  std::make_shared<kspp::avro_schema_registry>(*this);
      avro_serdes_ = std::make_shared<kspp::avro_serdes>(avro_schema_registry_, relaxed_parsing);
    }
    return avro_serdes_;
  }

  void cluster_config::validate() const {
    LOG_IF(FATAL, brokers_.size() == 0) << "cluster_config, no brokers defined";
    {
      auto v = kspp::split_url_list(brokers_, "plaintext");
      for (auto url : v) {
        if (url.scheme() == "ssl")
          LOG_IF(FATAL, ca_cert_path_.size() == 0) << "cluster_config, brokers using ssl and no ca cert configured";
      }
    }

    {
      auto v = kspp::split_url_list(schema_registry_uri_, "http");
      for (auto url : v) {
        if (url.scheme() == "ssl")
          LOG_IF(FATAL, ca_cert_path_.size() == 0)
          << "cluster_config, schema registry using https and no ca cert configured";
      }
    }

    // creates and validates...
    get_cluster_metadata()->validate();

  }

  void cluster_config::log() const {
    LOG(INFO) << "cluster_config, kafka broker(s): " << get_brokers();
    LOG(INFO) << "cluster_config, consumer_group: " << get_consumer_group();
    LOG_IF(INFO, get_ca_cert_path().size() > 0) << "cluster_config, ca cert: " << get_ca_cert_path();
    LOG_IF(INFO, get_client_cert_path().size() > 0) << "cluster_config, client cert: " << get_client_cert_path();
    LOG_IF(INFO, get_private_key_path().size() > 0) << "cluster_config, client key: " << get_private_key_path();
    LOG_IF(INFO, get_private_key_passphrase().size() > 0) << "cluster_config, client key passphrase: [withheld]";

    LOG_IF(INFO, get_storage_root().size() > 0) << "cluster_config, storage root: " << get_storage_root();
    LOG(INFO) << "cluster_config, kafka consumer_buffering_time: " << get_consumer_buffering_time().count() << " ms";
    LOG(INFO) << "cluster_config, kafka producer_buffering_time: " << get_producer_buffering_time().count() << " ms";

    if (get_producer_message_timeout().count() == 0)
      LOG(INFO) << "cluster_config, kafka producer_message_timeout: disabled";
    else
      LOG(INFO) << "cluster_config, kafka producer_message_timeout: " << get_producer_message_timeout().count()
                << " ms";

    LOG_IF(INFO, get_schema_registry_uri().size() > 0) << "cluster_config, schema_registry: " << get_schema_registry_uri();
    LOG_IF(INFO, get_schema_registry_uri().size() > 0)
    << "cluster_config, schema_registry_timeout: " << get_schema_registry_timeout().count() << " ms";
    LOG(INFO) << "kafka cluster_state_timeout: " << get_cluster_state_timeout().count() << " s";
  }
}