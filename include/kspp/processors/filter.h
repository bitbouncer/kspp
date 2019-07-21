#include <kspp/kspp.h>
#pragma once
namespace kspp {
  template<class K, class V>
  class filter : public event_consumer<K, V>, public partition_source<K, V> {
    static constexpr const char* PROCESSOR_NAME = "filter";
  public:
    //typedef std::function<bool(std::shared_ptr<const krecord <K, V>> record)> predicate; // return true to keep
    typedef std::function<bool(const krecord <K, V>& record)> predicate; // return true to keep

    filter(std::shared_ptr<cluster_config> config, std::shared_ptr<partition_source < K, V>> source, predicate f)
    : event_consumer<K, V>()
    , partition_source<K, V>(source.get(), source->partition())
    , source_(source)
    , predicate_(f)
    , predicate_false_("predicate_false", "msg") {
      source_->add_sink([this](auto r) {
        this->_queue.push_back(r);
      });
      this->add_metric(&predicate_false_);
      this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, "filter");
      this->add_metrics_label(KSPP_PARTITION_TAG, std::to_string(source->partition()));
    }

    ~filter() {
      close();
    }

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }

    void start(int64_t offset) override {
      source_->start(offset);
    }

    void close() override {
      source_->close();
    }

    size_t process(int64_t tick) override {
      source_->process(tick);
      size_t processed = 0;

      while (this->_queue.next_event_time()<=tick){
        auto trans = this->_queue.pop_and_get();
        ++processed;
       this->_lag.add_event_time(tick, trans->event_time());
        ++(this->_processed_count);
        if (trans->record()) {
          if (predicate_(*trans->record())) {
            this->send_to_sinks(trans);
          } else {
            ++predicate_false_;
          }
        }
      }
      return processed;
    }

    void commit(bool flush) override {
      source_->commit(flush);
    }

    bool eof() const override {
      return ((queue_size() == 0) && source_->eof());
    }

    size_t queue_size() const override {
      return event_consumer<K, V >::queue_size();
    }

    int64_t next_event_time() const override {
      return event_consumer<K, V>::next_event_time();
    }

  private:
    std::shared_ptr<partition_source < K, V>> source_;
    predicate predicate_;
    metric_counter predicate_false_;
  };
} // namespace