#include <thread>
#include <kspp/cluster_config.h>
#include <boost/filesystem.hpp>
#include <glog/logging.h>
#include <kspp/utils/url_parser.h>
#include <kspp/utils/env.h>

using namespace std::chrono_literals;

namespace kspp {

  // STUFF THATS MISSING IN LIBRDKAFKA C++ API...
  // WE DO THIS THE C WAY INSTEAD
  // NEEDS TO BE REWRITTEN USING USING C++ API

  // copy of c++ code
  static void set_config(rd_kafka_conf_t* rd_conf, std::string key, std::string value) {
    char errstr[128];
    if (rd_kafka_conf_set(rd_conf, key.c_str(), value.c_str(), errstr, sizeof(errstr)) !=  RD_KAFKA_CONF_OK) {
      throw std::invalid_argument("\"" + key + "\" -> " + value + ", error: " + errstr);
    }
    DLOG(INFO) << "rd_kafka set_config: " << key << "->" << value;
  }

  // copy of c++ code
  static void set_broker_config(rd_kafka_conf_t *rd_conf, const cluster_config* config) {
    set_config(rd_conf, "bootstrap.servers", config->get_brokers());

    if (config->get_brokers().substr(0, 3) == "ssl") {
      // SSL no auth - always
      set_config(rd_conf, "security.protocol", "ssl");
      set_config(rd_conf, "ssl.ca.location", config->get_ca_cert_path());

      //client cert
      set_config(rd_conf, "ssl.certificate.location", config->get_client_cert_path());
      set_config(rd_conf, "ssl.key.location", config->get_private_key_path());
      // optional password
      if (config->get_private_key_passphrase().size())
        set_config(rd_conf, "ssl.key.password", config->get_private_key_passphrase());
    }
  }

  metadata_provider::metadata_provider(const cluster_config* config)
    : rk_(nullptr) {
    auto rd_conf = rd_kafka_conf_new();
    try {
      set_broker_config(rd_conf, config);
      set_config(rd_conf, "api.version.request", "true");
    } catch (std::exception& e) {
      LOG(FATAL) << "could not set rd kafka config : " << e.what();
    }

    char errstr[128];
    rk_ = rd_kafka_new(RD_KAFKA_PRODUCER, rd_conf, errstr, sizeof(errstr));
    LOG_IF(FATAL, rk_ == nullptr) << "rd_kafka_new failed err: " <<  errstr;
  }

  metadata_provider::~metadata_provider(){
    if (rk_)
      rd_kafka_destroy(rk_);
  }

  void metadata_provider::validate()
  {
    // TODO - make a call to kafka - maybe __consumer_offsets
  }

  bool metadata_provider::consumer_group_exists(std::string consumer_group, std::chrono::seconds timeout) const {

    char errstr[128];
    rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;
    const struct rd_kafka_group_list *grplist = nullptr;
    int retries = timeout.count();
    /* FIXME: Wait for broker to come up. This should really be abstracted by librdkafka. */
    do {

      if (err) {
        DLOG(ERROR) << "retrying group list in 1s, ec: " << rd_kafka_err2str(err);
        std::this_thread::sleep_for(1s);
      } else if (grplist) {
        // the previous call must have succeded bu returned an empty list -
        // bug in rdkafka when using ssl - we cannot separate this from a non existent group - we must retry...
        LOG_IF(FATAL, grplist->group_cnt != 0) << "group list should be empty";
        rd_kafka_group_list_destroy(grplist);
        LOG(INFO) << "got empty group list - retrying in 1s";
        std::this_thread::sleep_for(1s);
      }
      {
        std::lock_guard<std::mutex> guard(mutex_);
        err = rd_kafka_list_groups(rk_, consumer_group.c_str(), &grplist, 5000);
      }

      DLOG_IF(INFO, err!=0) << "rd_kafka_list_groups: " << consumer_group.c_str() << ", res: " << err;
      DLOG_IF(INFO, err==0) << "rd_kafka_list_groups: " << consumer_group.c_str() << ", res: OK" << " grplist->group_cnt: "
                            << grplist->group_cnt;
    } while ((err == RD_KAFKA_RESP_ERR__TRANSPORT ||
              err == RD_KAFKA_RESP_ERR_GROUP_LOAD_IN_PROGRESS) || (err == 0 && grplist && grplist->group_cnt == 0) &&
                                                                  retries-- > 0);

    if (err) {
      LOG(ERROR) << "failed to retrieve groups, ec: " << rd_kafka_err2str(err);
      return false;
      //throw std::runtime_error(rd_kafka_err2str(err));
    }
    bool found = (grplist->group_cnt > 0);
    rd_kafka_group_list_destroy(grplist);
    return found;
  }


  cluster_config::cluster_config()
      : producer_buffering_(std::chrono::milliseconds(1000)), producer_message_timeout_(std::chrono::milliseconds(0)),
        consumer_buffering_(std::chrono::milliseconds(1000)), schema_registry_timeout_(std::chrono::milliseconds(1000)),
        _fail_fast(true) {
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

  void cluster_config::set_schema_registry(std::string urls) {
    auto v = kspp::split_url_list(urls, "http");
    LOG_IF(FATAL, v.size() == 0) << "cluster_config, bad schema registry urls: " << urls;
    schema_registry_uri_ = urls;
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

  void cluster_config::set_producer_message_timeout(std::chrono::milliseconds timeout) {
    producer_message_timeout_ = timeout;
  }

  std::chrono::milliseconds cluster_config::get_producer_message_timeout() const {
    return producer_message_timeout_;
  }

  void cluster_config::set_fail_fast(bool state) {
    _fail_fast = state;
  }

  bool cluster_config::get_fail_fast() const {
    return _fail_fast;
  }

  std::shared_ptr<metadata_provider> cluster_config::get_metadata_provider() const {
    if (meta_data_==nullptr)
      meta_data_ = std::make_shared<metadata_provider>(this);
    return meta_data_;
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
    get_metadata_provider()->validate();

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

    if (get_producer_message_timeout().count() == 0)
      LOG(INFO) << "cluster_config, kafka producer_message_timeout: disabled";
    else
      LOG(INFO) << "cluster_config, kafka producer_message_timeout: " << get_producer_message_timeout().count()
                << " ms";

    LOG_IF(INFO, get_schema_registry().size() > 0) << "cluster_config, schema_registry: " << get_schema_registry();
    LOG_IF(INFO, get_schema_registry().size() > 0)
    << "cluster_config, schema_registry_timeout: " << get_schema_registry_timeout().count() << " ms";
  }
}