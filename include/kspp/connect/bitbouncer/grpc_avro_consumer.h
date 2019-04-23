#include <chrono>
#include <memory>
#include <kspp/impl/queue.h>
#include <kspp/topology.h>
#include <kspp/avro/generic_avro.h>
#include <grpcpp/grpcpp.h>
#include <glog/logging.h>
#include <bitbouncer_streaming.grpc.pb.h>
#include "grpc_avro_schema_resolver.h"
#include "grpc_avro_serdes.h"

#pragma once

namespace kspp {
  template<class K, class V>
  class grpc_avro_consumer {
  public:
    grpc_avro_consumer(int32_t partition,
                       std::string topic_name,
                       std::string consumer_group,
                       std::string offset_storage_path,
                       std::shared_ptr<grpc::Channel> streaming_channel,
                       std::string api_key)
        :  _exit(false)
        , _start_running(false)
        , _offset_storage_path(offset_storage_path)
        , _good(true)
        , _eof(false)
        , _closed(false)
        , _topic_name(topic_name)
        , _partition(partition)
        , _consumer_group(consumer_group)
        , _commit_chain(topic_name, partition)
        , _bg([this](){_thread();})
        , _msg_cnt(0)
        , _resolver(std::make_shared<grpc_avro_schema_resolver>(streaming_channel, api_key))
        , _serdes(std::make_unique<kspp::grpc_avro_serdes>(_resolver))
        , stub_(ksppstreaming::streamprovider::NewStub(streaming_channel))
        , _api_key(api_key)
        , _log_name(consumer_group + "::" + topic_name + ":" + std::to_string(partition)){
      if (_offset_storage_path.size()){
        boost::filesystem::create_directories(boost::filesystem::path(_offset_storage_path).parent_path());
      }
    }

    ~grpc_avro_consumer() {
      _exit = true;
      if (!_closed)
        close();
      _bg.join();
      LOG(INFO) << "grpc_avro_consumer " << _log_name << " exiting";
    }

    void close(){
      _exit = true;
      _start_running = false;
      if (_closed)
        return;
      _closed = true;
      LOG(INFO) << "grpc_consumer " << _log_name << ", closed - consumed " << _msg_cnt << " messages";
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
      if (offset == kspp::OFFSET_STORED) {

        if (boost::filesystem::exists(_offset_storage_path)) {
          std::ifstream is(_offset_storage_path.generic_string(), std::ios::binary);
          int64_t tmp;
          is.read((char *) &tmp, sizeof(int64_t));
          if (is.good()) {
            // if we are rescraping we must assume that this offset were at eof
            LOG(INFO) << "grpc_consumer " << _log_name  << ", start(OFFSET_STORED) - > ts:" << tmp;
            _start_offset = tmp;
          } else {
            LOG(INFO) << "grpc_consumer " << _log_name  <<", start(OFFSET_STORED), bad file " << _offset_storage_path << ", starting from OFFSET_BEGINNING";
            _start_offset = kspp::OFFSET_BEGINNING;
          }
        } else {
          LOG(INFO) << "grpc_consumer " << _log_name  << ", start(OFFSET_STORED), missing file " << _offset_storage_path << ", starting from OFFSET_BEGINNING";
          _start_offset = kspp::OFFSET_BEGINNING;
        }
      } else if (offset == kspp::OFFSET_BEGINNING) {
        DLOG(INFO) << "grpc_consumer " << _log_name  << " starting from OFFSET_BEGINNING";
        _start_offset = offset;
      } else if (offset == kspp::OFFSET_END) {
        DLOG(INFO) << "grpc_consumer " << _log_name  << " starting from OFFSET_END";
        _start_offset = offset;
      } else {
        LOG(INFO) << "grpc_consumer " << _log_name  <<  " starting from fixed offset: " << offset;
        _start_offset = offset;
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
      if (offset>0)
        commit(offset, flush);
    }

    inline int64_t offset() const {
      return _commit_chain.last_good_offset();
    }

    inline bool good() const {
      return _good;
    }

  private:
    void commit(int64_t offset, bool flush) {
      _last_commited_ts_ticks = offset;
      if (flush || ((_last_commited_ts_ticks - _last_flushed_ticks) > 10000)) {
        if (_last_flushed_ticks != _last_commited_ts_ticks) {
          std::ofstream os(_offset_storage_path.generic_string(), std::ios::binary);
          os.write((char *) &_last_commited_ts_ticks, sizeof(int64_t));
          _last_flushed_ticks = _last_commited_ts_ticks;
          os.flush();
        }
      }
    }

    void _thread() {
      using namespace std::chrono_literals;
      while (!_start_running && !_exit)
        std::this_thread::sleep_for(100ms);

      grpc::ClientContext context;
      add_api_key(context, _api_key);
      ksppstreaming::SubscriptionRequest request;
      request.set_topic(_topic_name);
      request.set_partition(_partition);
      request.set_offset(_start_offset);

      std::shared_ptr<grpc::ClientReader<ksppstreaming::SubscriptionData> > stream(stub_->Subscribe(&context, request));
      ksppstreaming::SubscriptionData reply;
      while (!_exit && stream->Read(&reply)) {
        // empty message - read again
        if ((reply.value().size()==0) && reply.key().size()==0){
          _eof = reply.eof();
          continue;
        }

        K key;
        auto val = std::make_shared<V>();

        _serdes->decode(reply.key_schema(), reply.key().data(), reply.key().size(), key);
        _serdes->decode(reply.value_schema(), reply.value().data(), reply.value().size(), *val);

        _eof = reply.eof();

        if (reply.eof())
          LOG(INFO) << "EOF";

        auto record = std::make_shared<krecord<K, V>>(key, val, reply.timestamp());
        // do we have one...
        auto e = std::make_shared<kevent<K, V>>(record, _commit_chain.create(reply.offset()));
        assert(e.get()!=nullptr);
        ++_msg_cnt;
        // TODO - backpressure here... stopp reading if wueue gets to big...
        _incomming_msg.push_back(e);
        if (_incomming_msg.size()>50000)
          std::this_thread::sleep_for(100ms);
      }
      _good = false;

      if (!_exit) {
        grpc::Status status = stream->Finish();
        if (!status.ok()) {
          LOG(ERROR) << "ksppstreaming rpc failed: " << status.error_message();
        }
      }
      LOG(INFO) << "exiting thread";
    }

    volatile bool _exit;
    volatile bool _start_running;
    volatile bool _good;
    volatile bool _eof;
    bool _closed;

    std::thread _bg;
    const std::string _topic_name;
    const int32_t _partition;
    const std::string _consumer_group;
    const std::string _log_name;

    int64_t _start_offset;
    boost::filesystem::path _offset_storage_path;
    commit_chain _commit_chain;
    int64_t _last_commited_ts_ticks=0;
    int64_t _last_flushed_ticks=0;
    event_queue<K, V> _incomming_msg;
    uint64_t _msg_cnt;
    std::shared_ptr<grpc_avro_schema_resolver> _resolver;
    std::unique_ptr<ksppstreaming::streamprovider::Stub> stub_;
    std::string _api_key;
    std::unique_ptr<grpc_avro_serdes> _serdes;
  };
}

