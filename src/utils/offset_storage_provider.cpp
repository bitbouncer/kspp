#include <kspp/utils/offset_storage_provider.h>
#include <glog/logging.h>
#include <kspp/kspp.h>

namespace kspp {
  int64_t offset_storage::start(int64_t offset){
    if (offset == kspp::OFFSET_STORED) {
      return load_offset(_flush_offset_timeout_ms);
    } else if (offset == kspp::OFFSET_BEGINNING) {
      LOG(INFO) << "starting from OFFSET_BEGINNING";
      return kspp::OFFSET_BEGINNING;
    } else if (offset == kspp::OFFSET_END) {
      LOG(INFO) << "starting from OFFSET_END";
      return kspp::OFFSET_END;
    }
    LOG(INFO) << "starting from fixed offset: " << offset << ", overriding storage";
    return offset;
  }

  fs_offset_storage::fs_offset_storage(std::string path)
      : offset_storage()
      , _offset_storage_path(path){
    if (!_offset_storage_path.empty()){
      boost::filesystem::create_directories(boost::filesystem::path(_offset_storage_path).parent_path());
    }
  }

  fs_offset_storage::~fs_offset_storage(){
    persist_offset(_last_commited_offset, 1000);
  }

  /*int64_t fs_offset_storage::start(int64_t offset){
    if (offset == kspp::OFFSET_STORED) {
      return load_offset();
    } else if (offset == kspp::OFFSET_BEGINNING) {
      LOG(INFO) << "starting from OFFSET_BEGINNING";
      return kspp::OFFSET_BEGINNING;
    } else if (offset == kspp::OFFSET_END) {
      LOG(INFO) << "starting from OFFSET_END";
      return kspp::OFFSET_END;
    }

    LOG(INFO) << "starting from fixed offset: " << offset << ", overriding storage";
    return offset;
  }
   */


  int64_t fs_offset_storage::load_offset(int timeout_ms_not_used){
    if (!boost::filesystem::exists(_offset_storage_path)){
      LOG(INFO) << "start(OFFSET_STORED), missing file " << _offset_storage_path << ", starting from OFFSET_BEGINNING";
      return kspp::OFFSET_BEGINNING;
    }

    std::ifstream is(_offset_storage_path.generic_string(), std::ios::binary);
    int64_t tmp;
    is.read((char *) &tmp, sizeof(int64_t));
    if (is.good()) {
      LOG(INFO) << "start(OFFSET_STORED) - > offset:" << tmp;
      return tmp;
    }

    LOG(INFO) << "start(OFFSET_STORED), bad file " << _offset_storage_path << ", starting from OFFSET_BEGINNING";
    return kspp::OFFSET_BEGINNING;
  }

  void fs_offset_storage::persist_offset(int64_t offset, int timeout_ms_not_used) {
    if (_last_flushed_offset != _last_commited_offset) {
      std::ofstream os(_offset_storage_path.generic_string(), std::ios::binary);
      os.write((char *) &_last_commited_offset, sizeof(int64_t));
      _last_flushed_offset = _last_commited_offset;
      os.flush();
    }
  }

  static S3Status responsePropertiesCallback(
      const S3ResponseProperties *properties,
      void *callbackData) {
    return S3StatusOK;
  }

  static void responseCompleteCallback(
      S3Status status,
      const S3ErrorDetails *error,
      void *callbackData) {
    return;
  }

  static S3ResponseHandler responseHandler = {
      &responsePropertiesCallback,
      &responseCompleteCallback
  };

  s3_offset_storage::s3_offset_storage(std::string host, std::string bucket, std::string key, std::string access_key, std::string secret_key, bool use_ssl)
      : _host(host)
      ,_bucket(bucket)
      ,_key(key)
      ,_access_key(access_key)
      ,_secret_key(secret_key)
      , _bucketContext{_host.c_str(),
                       _bucket.c_str(),
                       use_ssl ? S3ProtocolHTTPS : S3ProtocolHTTP,
                       S3UriStylePath,
                       _access_key.c_str(),
                       _secret_key.c_str()}{
    S3Status status;
    if ((status = S3_initialize("kspp_s3", S3_INIT_ALL, _host.c_str())) != S3StatusOK) {
      LOG(ERROR) << "Failed to initialize libs3: " << S3_get_status_name(status);
    }
  }

  s3_offset_storage::~s3_offset_storage(){
    persist_offset(_last_commited_offset, 1000);
    //S3_deinitialize();
  }

  /*
  int64_t s3_offset_storage::start(int64_t offset){
    if (offset == kspp::OFFSET_STORED) {
      return load_offset();
    } else if (offset == kspp::OFFSET_BEGINNING) {
      LOG(INFO) << "starting from OFFSET_BEGINNING";
      return kspp::OFFSET_BEGINNING;
    } else if (offset == kspp::OFFSET_END) {
      LOG(INFO) << "starting from OFFSET_END";
      return kspp::OFFSET_END;
    }

    LOG(INFO) << "starting from fixed offset: " << offset << ", overriding storage";
    return offset;
  }
   */

  static S3Status getObjectDataCallback(int bufferSize, const char *buffer, void *callbackData)
  {
    int64_t* offset = (int64_t*) callbackData;
    if (bufferSize!=sizeof(int64_t))
      return S3StatusAbortedByCallback;
    memcpy(offset, buffer, sizeof(int64_t));
    return S3StatusOK;
  }

  int64_t s3_offset_storage::load_offset(int timeout_ms){
    S3GetObjectHandler getObjectHandler = {
            responseHandler,
            &getObjectDataCallback
        };
    int64_t tmp=kspp::OFFSET_BEGINNING;
    S3_get_object(&_bucketContext, _key.c_str(), NULL, 0, 0, NULL, timeout_ms, &getObjectHandler, &tmp); // we use the "put" timeoute here??
    if (tmp == kspp::OFFSET_BEGINNING){
      LOG(INFO) << "start(OFFSET_STORED), read failed: starting from OFFSET_BEGINNING";
      return kspp::OFFSET_BEGINNING;
    }

    LOG(INFO) << "start(OFFSET_STORED), starting from offset: " << tmp;
    return tmp;
  }

  static int putObjectDataCallback(int bufferSize, char *buffer, void *callbackData)
  {
    if (bufferSize<sizeof(int64_t))
      return -1;

    int64_t offset = *(int64_t *) callbackData;
    memcpy(buffer, &offset, sizeof(int64_t));
    return sizeof(int64_t);
  }

  void s3_offset_storage::persist_offset(int64_t offset, int timeout_ms){
    if (_last_flushed_offset == _last_commited_offset)
      return;

    S3PutObjectHandler putObjectHandler = {
        responseHandler,
        &putObjectDataCallback
    };

    // we should split the name in S3: host:port bucket name
    LOG(INFO) << "begin persist_offset " << offset;
    S3_put_object(&_bucketContext, _key.c_str(), sizeof(int64_t), nullptr, nullptr, timeout_ms, &putObjectHandler, &_last_commited_offset);
    LOG(INFO) << "persist_offset done";
    _last_flushed_offset = _last_commited_offset;
  }

  std::shared_ptr<offset_storage> get_offset_provider(std::string uri) {
    // we should split the name in S3: host:port bucket name
    if (uri.size() < 2)
      return std::make_unique<fs_offset_storage>("/tmp/dummy.offset");

    if (uri.substr(0, 5).compare("s3://")==0 || uri.substr(0, 5).compare("S3://")==0) {
      bool use_ssl = true;
      size_t host_start = 5;
      std::string host = uri.substr(host_start, uri.find("/", host_start) - host_start);
      if (host.empty()) {
        LOG(ERROR) << "bad s3 host";
        return nullptr;
      }

      // this is just to check if the host is a ip address or a name - not that this only works for
      size_t colonInHostPos = host.find(':');
      std::string host_without_port = host.substr(0, colonInHostPos);
      boost::system::error_code ec;
      boost::asio::ip::address::from_string(host_without_port, ec);
      if (!ec){
        // is we get an ip addres here - lets disable ssl
        use_ssl = false;
        LOG(WARNING) << "disabling SSL for " << uri;
      }

      size_t bucket_start = 5 + host.size() + 1;
      std::string bucket = uri.substr(bucket_start, uri.find("/", bucket_start)-bucket_start);
      if (bucket.empty()) {
        LOG(ERROR) << "bad s3 bucket";
        return nullptr;
      }

      size_t key_start = 5 + host.size() + 1 + bucket.size() + 1;
      std::string key = uri.substr(key_start);
      if (key.empty()) {
        LOG(ERROR) << "bad s3 key";
        return nullptr;
      }

      LOG(INFO) <<  "host: " << host << ", bucket: " << bucket << ", key: " << key;

      // todo get this from cluster config???
      std::string access_key = getenv("KSPP_S3_ACCESSKEY");
      std::string secret_key = getenv("KSPP_S3_SECRETKEY");

      if (access_key.empty()){
        LOG(ERROR) << "bad s3 access_key";
        return nullptr;
      }

      if (secret_key.empty()){
        LOG(ERROR) << "bad s3 secret_key";
        return nullptr;
      }

      return std::make_shared<s3_offset_storage>(host, bucket, key, access_key, secret_key, use_ssl);
    } else {
      return std::make_shared<fs_offset_storage>(uri);
    }
  }

}
