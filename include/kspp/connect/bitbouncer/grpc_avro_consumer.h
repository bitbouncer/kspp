#include <chrono>
#include <memory>
#include <kspp/impl/queue.h>
#include <kspp/topology.h>
#include <kspp/avro/generic_avro.h>
#include <grpcpp/grpcpp.h>
#include <glog/logging.h>
#include <bb_streaming.grpc.pb.h>
#include "grpc_avro_schema_resolver.h"
#include "grpc_avro_serdes.h"
#include <kspp/utils/offset_storage_provider.h>
#pragma once

namespace kspp {
  static auto s_null_schema = std::make_shared<const avro::ValidSchema>(avro::compileJsonSchemaFromString("{\"type\":\"null\"}"));

  template<class K, class V>
  class grpc_avro_consumer_base {
  public:
    grpc_avro_consumer_base(int32_t partition,
                            std::string topic_name,
                            std::shared_ptr<offset_storage> offset_store,
                            std::string uri,
                            std::string api_key,
                            std::string secret_access_key)
        : _offset_storage(offset_store)
        , _topic_name(topic_name)
        , _partition(partition)
        , _commit_chain(topic_name, partition)
        , _bg([this](){_thread();})
        , _uri(uri)
        , _api_key(api_key)
        , _secret_access_key(secret_access_key) {
      grpc::ChannelArguments channelArgs;
      kspp::set_channel_args(channelArgs);

      // special for easier debugging
      if (uri == "localhost:50063") {
        _channel = grpc::CreateCustomChannel(uri, grpc::InsecureChannelCredentials(), channelArgs);
      } else {
        auto channel_creds = grpc::SslCredentials(grpc::SslCredentialsOptions());
        _channel = grpc::CreateCustomChannel(uri, channel_creds, channelArgs);
      }
    }

    virtual ~grpc_avro_consumer_base() {
      if (!_closed)
        close();
      LOG(INFO) << "grpc_avro_consumer " << _topic_name << " exiting";
    }

    void close(){
      _exit = true;
      if (_closed)
        return;
      _closed = true;
      LOG(INFO) << "grpc_avro_consumer " << _topic_name << ", closed - consumed " << _msg_cnt << " messages";
    }

    inline bool eof() const {
      return (_incomming_msg.size() == 0) && _eof;
    }

    inline std::string logical_name() const {
      return _topic_name;
    }

    inline int32_t partition() const {
      return _partition;
    }

    void start(int64_t offset) {
      if (_offset_storage) {
        _next_offset = _offset_storage->start(offset);
      } else {
        _next_offset = OFFSET_END;
      }
      _start_running = true;
    }

    inline event_queue<K, V>& queue(){
      return _incomming_msg;
    };

    inline const event_queue<K, V>& queue() const {
      return _incomming_msg;
    };

    void commit(bool flush) {
      int64_t offset = _commit_chain.last_good_offset();
      if (offset>0 && _offset_storage)
          _offset_storage->commit(offset, flush);
    }

    inline int64_t offset() const {
      return _commit_chain.last_good_offset();
    }

    inline bool good() const {
      return _good;
    }

  protected:
    void stop_thread(){
      _exit = true;

      if (_bg.joinable())
        _bg.join();
    }

    void _thread() {
      using namespace std::chrono_literals;
      while (!_start_running && !_exit)
        std::this_thread::sleep_for(100ms);

      _resolver = std::make_shared<grpc_avro_schema_resolver>(_channel, _api_key);
      _serdes = std::make_unique<kspp::grpc_avro_serdes>(_resolver);

      while(!_exit) {
        size_t msg_in_rpc=0;
        //DLOG(INFO) << "new rpc";
        _stub = bitbouncer::streaming::streamprovider::NewStub(_channel);
        grpc::ClientContext context;
        add_api_key_secret(context, _api_key, _secret_access_key);

        bitbouncer::streaming::SubscriptionRequest request;
        request.set_topic(_topic_name);
        request.set_partition(_partition);
        request.set_offset(_next_offset);
        std::shared_ptr<grpc::ClientReader<bitbouncer::streaming::SubscriptionBundle>> stream(_stub->Subscribe(&context, request));
        bitbouncer::streaming::SubscriptionBundle reply;

        while (!_exit) {
          //backpressure
          if (_incomming_msg.size() > 50000) {
            std::this_thread::sleep_for(100ms);
            continue;
          }

          if (!stream->Read(&reply))
            break;

          ++msg_in_rpc;

          size_t sz = reply.data().size();
          for (size_t i = 0; i != sz; ++i) {
            const auto &record = reply.data(i);

            // empty message - read again
            if ((record.value().size() == 0) && record.key().size() == 0) {
              _eof = record.eof();
              continue;
            }

            _next_offset = record.offset()+1;
            auto krec = decode(record);
            if (krec == nullptr)
              continue;
            auto e = std::make_shared<kevent<K, V>>(krec, _commit_chain.create(record.offset()));
            assert(e.get() != nullptr);
            ++_msg_cnt;
            _incomming_msg.push_back(e);
          }
        }

        if (_exit){
          //DLOG(INFO) << "TRY CANCEL";
          context.TryCancel();
          grpc::Status status = stream->Finish();
          //DLOG(INFO) << "STREAM FINISHED status :" << status.error_message();
          break;
        }

        if (!_exit) {
          //DLOG(INFO) << "FINISHING STREAM, nr_of_msg: " << msg_in_rpc;
          grpc::Status status = stream->Finish();
          //DLOG(INFO) << "STREAM FINISHED status :" << status.error_message();
          if (!status.ok()) {
            LOG(ERROR) << "rpc failed: " << status.error_message();
          } else {
            //LOG(INFO) << "grpc_avro_consumer rpc";
          }
        }

        if (!_exit) {
          if (msg_in_rpc==0)
            std::this_thread::sleep_for(1000ms);

        }
      } // while /!exit) -> try to connect again
      _good = false;
      LOG(INFO) << "grpc_avro_consumer exiting thread";
    }

    virtual std::shared_ptr<kspp::krecord<K, V>> decode(const bitbouncer::streaming::SubscriptionData& record)=0;

    volatile bool _exit=false;
    volatile bool _start_running=false;
    volatile bool _good=true;
    volatile bool _eof=false;
    bool _closed=false;

    std::thread _bg;
    const std::string _uri;
    const std::string _topic_name;
    const int32_t _partition;

    int64_t _next_offset = kspp::OFFSET_BEGINNING; // not same as commit
    std::shared_ptr<offset_storage> _offset_storage;
    commit_chain _commit_chain;
    event_queue<K, V> _incomming_msg;
    uint64_t _msg_cnt=0;
    std::shared_ptr<grpc::Channel> _channel;
    std::shared_ptr<grpc_avro_schema_resolver> _resolver;
    std::unique_ptr<bitbouncer::streaming::streamprovider::Stub> _stub;
    std::string _api_key;
    std::string _secret_access_key;
    std::unique_ptr<grpc_avro_serdes> _serdes;
  };

  template<class K, class V>
  class grpc_avro_consumer : public grpc_avro_consumer_base<K, V> {
  public:
    grpc_avro_consumer(int32_t partition,
                       std::string topic_name,
                       std::shared_ptr<offset_storage> offset_store,
                       std::string uri,
                       std::string api_key,
                       std::string secret_access_key)
        : grpc_avro_consumer_base<K,V>(partition, topic_name, offset_store, uri, api_key, secret_access_key) {
    }

    virtual ~grpc_avro_consumer(){
      this->stop_thread();
    }

    std::shared_ptr<kspp::krecord<K, V>> decode(const bitbouncer::streaming::SubscriptionData& record) override{
      K key;
      std::shared_ptr<V> val;
      size_t r0 = this->_serdes->decode(record.key_schema(), record.key().data(), record.key().size(), key);
      if (r0 == 0)
        return nullptr;

      if (record.value().size() > 0) {
        val = std::make_shared<V>();
        auto r1 = this->_serdes->decode(record.value_schema(), record.value().data(), record.value().size(), *val);
        if (r1 == 0)
          return nullptr;
      }
      return std::make_shared<krecord<K, V>>(key, val, record.timestamp());
    }
  };

  // this is a special case of generic stuff where the key might be null
  template<class V>
  class grpc_avro_consumer<kspp::generic_avro, V> : public grpc_avro_consumer_base<kspp::generic_avro, V> {
  public:
    grpc_avro_consumer(int32_t partition,
                       std::string topic_name,
                       std::shared_ptr<offset_storage> offset_store,
                       std::string uri,
                       std::string api_key,
                       std::string secret_access_key)
        : grpc_avro_consumer_base<kspp::generic_avro,V>(partition, topic_name, offset_store, uri, api_key, secret_access_key) {
    }

    virtual ~grpc_avro_consumer(){
      this->stop_thread();
    }

    std::shared_ptr<kspp::krecord<kspp::generic_avro, V>> decode(const bitbouncer::streaming::SubscriptionData& record) override{
      kspp::generic_avro key;
      std::shared_ptr<V> val;
      if (record.key().size()==0) {
        key.create(s_null_schema, 0);
      } else {
        size_t r0 = this->_serdes->decode(record.key_schema(), record.key().data(), record.key().size(), key);
        if (r0 == 0)
          return nullptr;
      }

      if (record.value().size() > 0) {
        val = std::make_shared<V>();
        auto r1 = this->_serdes->decode(record.value_schema(), record.value().data(), record.value().size(), *val);
        if (r1 == 0)
          return nullptr;
      }
      return std::make_shared<krecord<kspp::generic_avro, V>>(key, val, record.timestamp());
    }
  };



  template<class V>
  class grpc_avro_consumer<void, V> : public grpc_avro_consumer_base<void, V> {
  public:
    grpc_avro_consumer(int32_t partition,
                       std::string topic_name,
                       std::shared_ptr<offset_storage> offset_store,
                       std::string uri,
                       std::string api_key,
                       std::string secret_access_key)
        : grpc_avro_consumer_base<void,V>(partition, topic_name, offset_store, uri, api_key, secret_access_key) {
    }

    virtual ~grpc_avro_consumer(){
      this->stop_thread();
    }

    std::shared_ptr<kspp::krecord<void, V>> decode(const bitbouncer::streaming::SubscriptionData& record) override{
      std::shared_ptr<V> val;
      if (record.value().size() > 0) {
        val = std::make_shared<V>();
        auto r1 = this->_serdes->decode(record.value_schema(), record.value().data(), record.value().size(), *val);
        if (r1 == 0)
          return nullptr;
      }
      return std::make_shared<krecord<void, V>>(val, record.timestamp());
    }
  };

  template<class K>
  class grpc_avro_consumer<K, void> : public grpc_avro_consumer_base<K, void> {
  public:
    grpc_avro_consumer(int32_t partition,
                       std::string topic_name,
                       std::shared_ptr<offset_storage> offset_store,
                       std::string uri,
                       std::string api_key,
                       std::string secret_access_key)
        : grpc_avro_consumer_base<K,void>(partition, topic_name, offset_store, uri, api_key, secret_access_key) {
    }

    std::shared_ptr<kspp::krecord<K, void>> decode(const bitbouncer::streaming::SubscriptionData& record) override{
      K key;
      size_t r0 = this->_serdes->decode(record.key_schema(), record.key().data(), record.key().size(), key);
      if (r0 == 0)
        return nullptr;
      return std::make_shared<krecord<K, void>>(key, record.timestamp());
    }
  };
}

