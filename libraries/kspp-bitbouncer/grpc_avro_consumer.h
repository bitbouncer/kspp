#include <chrono>
#include <memory>
#include <grpcpp/grpcpp.h>
#include <glog/logging.h>
#include <kspp/internal/queue.h>
#include <kspp/topology.h>
#include <kspp/avro/generic_avro.h>
#include <kspp/utils/offset_storage_provider.h>
#include <kspp/internal/commit_chain.h>

#include <bb_streaming.grpc.pb.h>
#include "grpc_avro_schema_resolver.h"
#include "grpc_avro_serdes.h"


#pragma once

namespace kspp {
  static auto s_null_schema = std::make_shared<const avro::ValidSchema>(
      avro::compileJsonSchemaFromString("{\"type\":\"null\"}"));

  template<class K, class V>
  class grpc_avro_consumer_base {
  public:
    grpc_avro_consumer_base(int32_t partition,
                            std::string topic_name,
                            std::shared_ptr<offset_storage> offset_store,
                            std::shared_ptr<grpc::Channel> channel,
                            std::string api_key,
                            std::string secret_access_key)
        : offset_storage_(offset_store), topic_name_(topic_name), partition_(partition),
          commit_chain_(topic_name, partition), bg_([this]() { _thread(); }), channel_(channel), api_key_(api_key),
          secret_access_key_(secret_access_key) {
    }

    virtual ~grpc_avro_consumer_base() {
      if (!closed_)
        close();
      LOG(INFO) << "grpc_avro_consumer " << topic_name_ << " exiting";
    }

    void close() {
      exit_ = true;
      if (closed_)
        return;
      closed_ = true;
      LOG(INFO) << "grpc_avro_consumer " << topic_name_ << ", closed - consumed " << msg_cnt_ << " messages";
    }

    inline bool eof() const {
      return (incomming_msg_.size() == 0) && eof_;
    }

    inline std::string logical_name() const {
      return topic_name_;
    }

    inline int32_t partition() const {
      return partition_;
    }

    void start(int64_t offset) {
      if (offset_storage_) {
        next_offset_ = offset_storage_->start(offset);
      } else {
        next_offset_ = OFFSET_END;
      }
      start_running_ = true;
    }

    inline event_queue<K, V> &queue() {
      return incomming_msg_;
    };

    inline const event_queue<K, V> &queue() const {
      return incomming_msg_;
    };

    void commit(bool flush) {
      int64_t offset = commit_chain_.last_good_offset();
      if (offset > 0 && offset_storage_)
        offset_storage_->commit(offset, flush);
    }

    inline int64_t offset() const {
      return commit_chain_.last_good_offset();
    }

    inline bool good() const {
      return good_;
    }

  protected:
    void stop_thread() {
      exit_ = true;

      if (bg_.joinable())
        bg_.join();
    }

    void _thread() {
      using namespace std::chrono_literals;
      while (!start_running_ && !exit_)
        std::this_thread::sleep_for(100ms);

      resolver_ = std::make_shared<grpc_avro_schema_resolver>(channel_, api_key_);
      serdes_ = std::make_unique<kspp::grpc_avro_serdes>(resolver_);

      while (!exit_) {
        size_t msg_in_rpc = 0;
        //DLOG(INFO) << "new rpc";
        stub_ = bitbouncer::streaming::streamprovider::NewStub(channel_);
        grpc::ClientContext context;
        add_api_key_secret(context, api_key_, secret_access_key_);

        bitbouncer::streaming::SubscriptionRequest request;
        request.set_topic(topic_name_);
        request.set_partition(partition_);
        request.set_offset(next_offset_);
        std::shared_ptr<grpc::ClientReader<bitbouncer::streaming::SubscriptionBundle>> stream(
            stub_->Subscribe(&context, request));
        bitbouncer::streaming::SubscriptionBundle reply;

        while (!exit_) {
          //backpressure
          if (incomming_msg_.size() > 50000) {
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
              continue;
            }

            next_offset_ = record.offset() + 1;
            auto krec = decode(record);
            if (krec == nullptr)
              continue;
            auto e = std::make_shared<kevent<K, V>>(krec, commit_chain_.create(record.offset()));
            assert(e.get() != nullptr);
            ++msg_cnt_;
            incomming_msg_.push_back(e);
          }
          eof_ = reply.eof();
        }

        if (exit_) {
          //DLOG(INFO) << "TRY CANCEL";
          context.TryCancel();
          grpc::Status status = stream->Finish();
          //DLOG(INFO) << "STREAM FINISHED status :" << status.error_message();
          break;
        }

        if (!exit_) {
          //DLOG(INFO) << "FINISHING STREAM, nr_of_msg: " << msg_in_rpc;
          grpc::Status status = stream->Finish();
          //DLOG(INFO) << "STREAM FINISHED status :" << status.error_message();
          if (!status.ok()) {
            LOG(ERROR) << "rpc failed: " << status.error_message();
          } else {
            //LOG(INFO) << "grpc_avro_consumer rpc";
          }
        }

        if (!exit_) {
          if (msg_in_rpc == 0)
            std::this_thread::sleep_for(1000ms);

        }
      } // while /!exit) -> try to connect again
      good_ = false;
      LOG(INFO) << "grpc_avro_consumer exiting thread";
    }

    virtual std::shared_ptr<kspp::krecord<K, V>> decode(const bitbouncer::streaming::SubscriptionData &record) = 0;

    volatile bool exit_ = false;
    volatile bool start_running_ = false;
    volatile bool good_ = true;
    volatile bool eof_ = false;
    bool closed_ = false;

    std::thread bg_;
    const std::string topic_name_;
    const int32_t partition_;

    int64_t next_offset_ = kspp::OFFSET_BEGINNING; // not same as commit
    std::shared_ptr<offset_storage> offset_storage_;
    commit_chain commit_chain_;
    event_queue<K, V> incomming_msg_;
    uint64_t msg_cnt_ = 0;
    std::shared_ptr<grpc::Channel> channel_;
    std::shared_ptr<grpc_avro_schema_resolver> resolver_;
    std::unique_ptr<bitbouncer::streaming::streamprovider::Stub> stub_; // why a member??
    std::string api_key_;
    std::string secret_access_key_;
    std::unique_ptr<grpc_avro_serdes> serdes_;
  };

  template<class K, class V>
  class grpc_avro_consumer : public grpc_avro_consumer_base<K, V> {
  public:
    grpc_avro_consumer(int32_t partition,
                       std::string topic_name,
                       std::shared_ptr<offset_storage> offset_store,
                       std::shared_ptr<grpc::Channel> channel,
                       std::string api_key,
                       std::string secret_access_key)
        : grpc_avro_consumer_base<K, V>(partition, topic_name, offset_store, channel, api_key, secret_access_key) {
    }

    virtual ~grpc_avro_consumer() {
      this->stop_thread();
    }

    std::shared_ptr<kspp::krecord<K, V>> decode(const bitbouncer::streaming::SubscriptionData &record) override {
      K key;
      std::shared_ptr<V> val;

      size_t consumed0 = this->serdes_->decode(record.key_schema(), record.key().data(), record.key().size(), key);
      if (record.key().size() != consumed0) {
        LOG_FIRST_N(ERROR, 100) << "avro decode key failed, consumed: " << consumed0 << ", actual: "
                                << record.key().size();
        LOG_EVERY_N(ERROR, 1000) << "avro decode key failed, consumed: " << consumed0 << ", actual: "
                                 << record.key().size();
        return nullptr;
      }


      if (record.value().size() > 0) {
        val = std::make_shared<V>();
        auto consumed1 = this->serdes_->decode(record.value_schema(), record.value().data(), record.value().size(),
                                               *val);
        if (record.value().size() != consumed1) {
          LOG_FIRST_N(ERROR, 100) << "avro decode value failed, consumed: " << consumed1 << ", actual: "
                                  << record.value().size();
          LOG_EVERY_N(ERROR, 1000) << "avro decode value failed, consumed: " << consumed1 << ", actual: "
                                   << record.value().size();
          return nullptr;
        }
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
                       std::shared_ptr<grpc::Channel> channel,
                       std::string api_key,
                       std::string secret_access_key)
        : grpc_avro_consumer_base<kspp::generic_avro, V>(partition, topic_name, offset_store, channel, api_key,
                                                         secret_access_key) {
    }

    virtual ~grpc_avro_consumer() {
      this->stop_thread();
    }

    std::shared_ptr<kspp::krecord<kspp::generic_avro, V>>
    decode(const bitbouncer::streaming::SubscriptionData &record) override {
      kspp::generic_avro key;
      std::shared_ptr<V> val;
      if (record.key().size() == 0) {
        key.create(s_null_schema, 0);
      } else {

        size_t consumed0 = this->serdes_->decode(record.key_schema(), record.key().data(), record.key().size(), key);
        if (record.key().size() != consumed0) {
          LOG_FIRST_N(ERROR, 100) << "avro decode key failed, consumed: " << consumed0 << ", actual: "
                                  << record.key().size();
          LOG_EVERY_N(ERROR, 1000) << "avro decode key failed, consumed: " << consumed0 << ", actual: "
                                   << record.key().size();
          return nullptr;
        }
      }

      if (record.value().size() > 0) {
        val = std::make_shared<V>();
        auto consumed1 = this->serdes_->decode(record.value_schema(), record.value().data(), record.value().size(),
                                               *val);
        if (record.value().size() != consumed1) {
          LOG_FIRST_N(ERROR, 100) << "avro decode value failed, consumed: " << consumed1 << ", actual: "
                                  << record.value().size();
          LOG_EVERY_N(ERROR, 1000) << "avro decode value failed, consumed: " << consumed1 << ", actual: "
                                   << record.value().size();
          return nullptr;
        }
      }
      return std::make_shared<krecord<kspp::generic_avro, V>>(key, val, record.timestamp());
    }
  };


  template<class K>
  class grpc_avro_consumer<K, void> : public grpc_avro_consumer_base<K, void> {
  public:
    grpc_avro_consumer(int32_t partition,
                       std::string topic_name,
                       std::shared_ptr<offset_storage> offset_store,
                       std::shared_ptr<grpc::Channel> channel,
                       std::string api_key,
                       std::string secret_access_key)
        : grpc_avro_consumer_base<K, void>(partition, topic_name, offset_store, channel, api_key, secret_access_key) {
    }

    std::shared_ptr<kspp::krecord<K, void>> decode(const bitbouncer::streaming::SubscriptionData &record) override {
      K key;
      size_t consumed0 = this->serdes_->decode(record.key_schema(), record.key().data(), record.key().size(), key);
      if (record.key().size() != consumed0) {
        LOG_FIRST_N(ERROR, 100) << "avro decode key failed, consumed: " << consumed0 << ", actual: "
                                << record.key().size();
        LOG_EVERY_N(ERROR, 1000) << "avro decode key failed, consumed: " << consumed0 << ", actual: "
                                 << record.key().size();
        return nullptr;
      }
      return std::make_shared<krecord<K, void>>(key, record.timestamp());
    }
  };

  template<class V>
  class grpc_avro_consumer<void, V> : public grpc_avro_consumer_base<void, V> {
  public:
    grpc_avro_consumer(int32_t partition,
                       std::string topic_name,
                       std::shared_ptr<offset_storage> offset_store,
                       std::shared_ptr<grpc::Channel> channel,
                       std::string api_key,
                       std::string secret_access_key)
        : grpc_avro_consumer_base<void, V>(partition, topic_name, offset_store, channel, api_key, secret_access_key) {
    }

    virtual ~grpc_avro_consumer() {
      this->stop_thread();
    }

    std::shared_ptr<kspp::krecord<void, V>> decode(const bitbouncer::streaming::SubscriptionData &record) override {
      std::shared_ptr<V> val;
      if (record.value().size() > 0) {
        val = std::make_shared<V>();
        auto consumed1 = this->serdes_->decode(record.value_schema(), record.value().data(), record.value().size(),
                                               *val);
        if (record.value().size() != consumed1) {
          LOG_FIRST_N(ERROR, 100) << "avro decode value failed, consumed: " << consumed1 << ", actual: "
                                  << record.value().size();
          LOG_EVERY_N(ERROR, 1000) << "avro decode value failed, consumed: " << consumed1 << ", actual: "
                                   << record.value().size();
          return nullptr;
        }
      }
      return std::make_shared<krecord<void, V>>(val, record.timestamp());
    }
  };
}

