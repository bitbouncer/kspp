#include <memory>
#include <deque>
#include <functional>
#include <optional>
#include <kspp/kspp.h>
#pragma once

namespace kspp {
  template<class LEFT, class RIGHT>
  class left_join {
  public:
    typedef std::pair<LEFT, std::optional<RIGHT>> value_type;
  };

  template<class LEFT, class RIGHT>
  class inner_join {
  public:
    typedef std::pair<LEFT, RIGHT> value_type;
  };

  template<class LEFT, class RIGHT>
  class outer_join {
  public:
    typedef std::pair<std::optional<LEFT>, std::optional<RIGHT>> value_type;
  };

  template<class KEY, class LEFT, class RIGHT>
  class kstream_left_join :
      public event_consumer<KEY, LEFT>,
      public partition_source<KEY, typename left_join<LEFT, RIGHT>::value_type> {
    static constexpr const char* PROCESSOR_NAME = "kstream_left_join";
  public:
    typedef typename left_join<LEFT, RIGHT>::value_type value_type;

    kstream_left_join(
        std::shared_ptr<cluster_config> config,
        std::shared_ptr<partition_source < KEY, LEFT>> left,
    std::shared_ptr<materialized_source < KEY, RIGHT>> right)
    : event_consumer<KEY, LEFT>()
    , partition_source<KEY, value_type>(left.get(), left->partition())
    , left_stream_ (left)
    , right_table_(right) {
      this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, "kstream_left_join");
      this->add_metrics_label(KSPP_PARTITION_TAG, std::to_string(left->partition()));
      left_stream_->add_sink([this](auto r) { this->_queue.push_back(r); });
    }

    ~kstream_left_join() {
      close();
    }

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }

    void start(int64_t offset) override {
      left_stream_->start(offset);
      right_table_->start(offset);
    }

    void close() override {
      left_stream_->close();
      right_table_->close();
    }

    size_t queue_size() const override {
      return event_consumer<KEY, LEFT> ::queue_size();
    }

    int64_t next_event_time() const override {
      auto q = event_consumer<KEY, LEFT>::next_event_time();
      auto us = left_stream_->next_event_time();
      return std::min(q, us);
    }

    size_t process(int64_t tick) override {
      if (right_table_->process(tick) > 0)
        right_table_->commit(false);

      left_stream_->process(tick);

      size_t processed = 0;
      // reuse event time & commit it from event stream
      while (this->_queue.next_event_time() <= tick) {
        auto left = this->_queue.pop_front_and_get();
        this->_lag.add_event_time(tick, left->event_time());
        ++(this->_processed_count);
        ++processed;
        // null values from left should be ignored
        if (left->record() && left->record()->value()) {
          auto right_record = right_table_->get(left->record()->key());

          std::optional<RIGHT> right_val;
          if (right_record && right_record->value())
            right_val = *right_record->value();

          auto value = std::make_shared<value_type>(*left->record()->value(), right_val);
          auto record = std::make_shared<krecord<KEY, value_type>>(left->record()->key(), value, left->event_time());
          this->send_to_sinks(std::make_shared<kspp::kevent<KEY, value_type>>(record, left->id()));
        } else {
          // no output on left null
        }
      }
      return processed;
    }

    void commit(bool flush) override {
      right_table_->commit(flush);
      left_stream_->commit(flush);
    }

    bool eof() const override {
      return right_table_->eof() && left_stream_->eof();
    }

  private:
    std::shared_ptr<partition_source < KEY, LEFT>>   left_stream_;
    std::shared_ptr<materialized_source < KEY, RIGHT>> right_table_;
  };

  template<class KEY, class LEFT, class RIGHT>
  class kstream_inner_join :
      public event_consumer<KEY, LEFT>,
      public partition_source<KEY, typename inner_join<LEFT, RIGHT>::value_type> {
    static constexpr const char* PROCESSOR_NAME = "kstream_inner_join";
  public:
    typedef typename inner_join<LEFT, RIGHT>::value_type value_type;

    kstream_inner_join(
        std::shared_ptr<cluster_config> config,
        std::shared_ptr<partition_source < KEY, LEFT>> left,
    std::shared_ptr<materialized_source < KEY, RIGHT>> right)
    : event_consumer<KEY, LEFT>(), partition_source<KEY, value_type>(left.get(), left->partition())
    , left_stream_ (left)
    , right_table_(right) {
      this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, "kstream_inner_join");
      this->add_metrics_label(KSPP_PARTITION_TAG, std::to_string(left->partition()));
      left_stream_->add_sink([this](auto r) { this->_queue.push_back(r); });
    }

    ~kstream_inner_join() {
      close();
    }

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }

    void start(int64_t offset) override {
      left_stream_->start(offset);
      right_table_->start(offset);
    }

    void close() override {
      right_table_->close();
      left_stream_->close();
    }

    size_t queue_size() const override {
      return event_consumer<KEY, LEFT> ::queue_size();
    }

    int64_t next_event_time() const override {
      auto q = event_consumer<KEY, LEFT>::next_event_time();
      auto us = left_stream_->next_event_time();
      return std::min(q, us);
    }

    size_t process(int64_t tick) override {
      if (right_table_->process(tick) > 0)
        right_table_->commit(false);

      left_stream_->process(tick);

      size_t processed = 0;
      // reuse event time & commit it from event stream
      while (this->_queue.next_event_time() <= tick) {
        auto left = this->_queue.pop_front_and_get();
        this->_lag.add_event_time(tick, left->event_time());
        ++(this->_processed_count);
        ++processed;
        // null values from left should be ignored
        if (left->record() && left->record()->value()) {
          auto right_record = right_table_->get(left->record()->key());
          // null values from right should be ignored
          if (right_record && right_record->value()) {
            //auto right_val = std::make_shared<tableV>(*right_record->value());
            auto value = std::make_shared<value_type>(*left->record()->value(), *right_record->value());
            auto record = std::make_shared<krecord<KEY, value_type>>(left->record()->key(), value, left->event_time());
            this->send_to_sinks(std::make_shared<kspp::kevent<KEY, value_type>>(record, left->id()));
          }
        } else {
// no output on left null
        }
      }
      return processed;
    }

    void commit(bool flush) override {
      right_table_->commit(flush);
      left_stream_->commit(flush);
    }

    bool eof() const override {
      return right_table_->eof() && left_stream_->eof();
    }

  private:
    std::shared_ptr<partition_source < KEY, LEFT>>   left_stream_;
    std::shared_ptr<materialized_source < KEY, RIGHT>> right_table_;
  };


  template<class KEY, class LEFT, class RIGHT>
  class ktable_left_join
      : public event_consumer<KEY, LEFT>,
        public partition_source<KEY, typename left_join<LEFT, RIGHT>::value_type> {
    static constexpr const char* PROCESSOR_NAME = "ktable_left_join";
  public:
    typedef typename left_join<LEFT, RIGHT>::value_type value_type;

    ktable_left_join(
        std::shared_ptr<cluster_config> config,
        std::shared_ptr<materialized_source < KEY, LEFT>> left,
        std::shared_ptr<materialized_source < KEY, RIGHT>> right)
    : event_consumer<KEY, LEFT>()
    , partition_source<KEY, value_type>(left.get(), left->partition())
    , left_table_ (left)
    , right_table_(right) {
      this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, "ktable_left_join");
      this->add_metrics_label(KSPP_PARTITION_TAG, std::to_string(left->partition()));
      left_table_->add_sink([this](auto r) { this->_queue.push_back(r); });
      right_table_->add_sink([this](auto r) { this->_queue.push_back(r); });
    }

    ~ktable_left_join() {
      close();
    }

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }

    void start(int64_t offset) override {
      // if we request begin - should we restart table here???
      //it seems that we should retain whatever's in the cache in as many cases as possible
      left_table_->start(offset);
      right_table_->start(offset);
    }

    void close() override {
      left_table_->close();
      right_table_->close();
    }

    size_t queue_size() const override {
      return event_consumer < KEY, LEFT > ::queue_size();
    }

    int64_t next_event_time() const override {
      auto q = event_consumer < KEY, LEFT>::next_event_time();
      auto ls = right_table_->next_event_time();
      auto rs = left_table_->next_event_time();
      return std::min(q, std::min(ls, rs));
    }

    size_t process(int64_t tick) override {
      right_table_->process(tick);
      left_table_->process(tick);

      size_t processed = 0;
      // reuse event time & commit it from event stream
      //
      while (this->_queue.next_event_time() <= tick) {
        auto ev = this->_queue.pop_front_and_get();
        this->_lag.add_event_time(tick, ev->event_time());
        ++(this->_processed_count);
        ++processed;
        // null values from left should be ignored

        auto left_record = left_table_->get(ev->record()->key());

        if (left_record && left_record->value()) {
          //std::shared_ptr<leftV> left_val = std::make_shared<leftV>(*left_record->value());

          auto right_record = right_table_->get(ev->record()->key());
          std::optional<RIGHT> right_val;
          if (right_record && right_record->value())
            right_val = *right_record->value();

          auto value = std::make_shared<value_type>(*left_record->value(), right_val);
          auto record = std::make_shared<krecord<KEY, value_type>>(ev->record()->key(), value, ev->event_time());
          this->send_to_sinks(std::make_shared<kspp::kevent<KEY, value_type>>(record, ev->id()));
        } else {
          // null output if left is not found
          auto record = std::make_shared<krecord<KEY, value_type>>(ev->record()->key(), nullptr, ev->event_time());
          this->send_to_sinks(std::make_shared<kspp::kevent<KEY, value_type>>(record, ev->id()));
        }
      }
      return processed;
    }

    void commit(bool flush) override {
      right_table_->commit(flush);
      left_table_->commit(flush);
    }

    bool eof() const override {
      return right_table_->eof() && left_table_->eof();
    }

  private:
    std::shared_ptr<materialized_source < KEY, LEFT>>  left_table_;
    std::shared_ptr<materialized_source < KEY, RIGHT>> right_table_;
  };


  template<class KEY, class LEFT, class RIGHT>
  class ktable_inner_join
      : public event_consumer<KEY, LEFT>, public partition_source<KEY, typename inner_join<LEFT, RIGHT>::value_type> {
    static constexpr const char* PROCESSOR_NAME = "ktable_inner_join";
  public:
    typedef typename inner_join<LEFT, RIGHT>::value_type value_type;

    ktable_inner_join(
        std::shared_ptr<cluster_config> config,
        std::shared_ptr<materialized_source < KEY, LEFT>> left,
    std::shared_ptr<materialized_source < KEY, RIGHT>> right)
    : event_consumer<KEY, LEFT>()
    , partition_source<KEY, value_type>(left.get(), left->partition())
    , left_table_ (left)
    , right_table_(right) {
      this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, "ktable_inner_join");
      this->add_metrics_label(KSPP_PARTITION_TAG, std::to_string(left->partition()));
      left_table_->add_sink([this](auto r) { this->_queue.push_back(r); });
      right_table_->add_sink([this](auto r) { this->_queue.push_back(r); });
    }

    ~ktable_inner_join() {
      close();
    }

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }

    void start(int64_t offset) override {
      // if we request begin - should we restart table here???
      //it seems that we should retain whatever's in the cache in as many cases as possible
      right_table_->start(offset);
      left_table_->start(offset);
    }

    void close() override {
      right_table_->close();
      left_table_->close();
    }

    size_t queue_size() const override {
      return event_consumer < KEY, LEFT > ::queue_size();
    }

    int64_t next_event_time() const override {
      auto q = event_consumer < KEY, LEFT>::next_event_time();
      auto ls = right_table_->next_event_time();
      auto rs = left_table_->next_event_time();
      return std::min(q, std::min(ls, rs));
    }

    size_t process(int64_t tick) override {
      right_table_->process(tick);
      left_table_->process(tick);

      size_t processed = 0;
      // reuse event time & commit it from event stream
      //
      while (this->_queue.next_event_time() <= tick) {
        auto ev = this->_queue.pop_front_and_get();
        this->_lag.add_event_time(tick, ev->event_time());
        ++(this->_processed_count);
        ++processed;
        // null values from left should be ignored

        auto left_record = left_table_->get(ev->record()->key());
        auto right_record = right_table_->get(ev->record()->key());

        if (left_record && left_record->value() && right_record && right_record->value()) {
          auto value = std::make_shared<value_type>(*left_record->value(), *right_record->value());
          auto record = std::make_shared<krecord<KEY, value_type>>(ev->record()->key(), value, ev->event_time());
          this->send_to_sinks(std::make_shared<kspp::kevent<KEY, value_type>>(record, ev->id()));
        } else {
          // null output if left is not found
          auto record = std::make_shared<krecord<KEY, value_type>>(ev->record()->key(), nullptr, ev->event_time());
          this->send_to_sinks(std::make_shared<kspp::kevent<KEY, value_type>>(record, ev->id()));
        }
      }
      return processed;
    }

    void commit(bool flush) override {
      right_table_->commit(flush);
      left_table_->commit(flush);
    }

    bool eof() const override {
      return right_table_->eof() && left_table_->eof();
    }

  private:
    std::shared_ptr<materialized_source < KEY, LEFT>>  left_table_;
    std::shared_ptr<materialized_source < KEY, RIGHT>> right_table_;
  };

  template<class KEY, class LEFT, class RIGHT>
  class ktable_outer_join
      : public event_consumer<KEY, LEFT>, public partition_source<KEY, typename outer_join<LEFT, RIGHT>::value_type> {
    static constexpr const char* PROCESSOR_NAME = "ktable_outer_join";
  public:
    typedef typename outer_join<LEFT, RIGHT>::value_type value_type;

    ktable_outer_join(std::shared_ptr<cluster_config> config,
                      std::shared_ptr<materialized_source<KEY, LEFT>> left,
                      std::shared_ptr<materialized_source < KEY, RIGHT>> right)
    : event_consumer<KEY, LEFT>()
    , partition_source<KEY, value_type>(left.get(), left->partition())
    , left_table_ (left)
    , right_table_(right) {
      this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, "ktable_outer_join");
      this->add_metrics_label(KSPP_PARTITION_TAG, std::to_string(left->partition()));
      left_table_->add_sink([this](auto r) { this->_queue.push_back(r); });
      right_table_->add_sink([this](auto r) { this->_queue.push_back(r); });
    }

    ~ktable_outer_join() {
      close();
    }

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }

    void start(int64_t offset) override {
      // if we request begin - should we restart table here???
      //it seems that we should retain whatever's in the cache in as many cases as possible
      left_table_->start(offset);
      right_table_->start(offset);
    }

    void close() override {
      left_table_->close();
      right_table_->close();
    }

    size_t queue_size() const override {
      return event_consumer < KEY, LEFT > ::queue_size();
    }

    int64_t next_event_time() const override {
      auto q = event_consumer < KEY, LEFT>::next_event_time();
      auto ls = right_table_->next_event_time();
      auto rs = left_table_->next_event_time();
      return std::min(q, std::min(ls, rs));
    }

    size_t process(int64_t tick) override {
      left_table_->process(tick);
      right_table_->process(tick);

      size_t processed = 0;
      // reuse event time & commit it from event stream
      //
      while (this->_queue.next_event_time() <= tick) {
        auto ev = this->_queue.pop_front_and_get();
        this->_lag.add_event_time(tick, ev->event_time());
        ++(this->_processed_count);
        ++processed;
        // null values from left should be ignored

        auto left_record = left_table_->get(ev->record()->key());
        auto right_record = right_table_->get(ev->record()->key());

        if (right_record || left_record) {
          std::optional<RIGHT> right_val;
          std::optional<LEFT> left_val;

          if (right_record && right_record->value())
            right_val = *right_record->value();

          if (left_record && left_record->value())
            left_val = *left_record->value();

          auto value = std::make_shared<value_type>(left_val, right_val);
          auto record = std::make_shared<krecord<KEY, value_type>>(ev->record()->key(), value, ev->event_time());
          this->send_to_sinks(std::make_shared<kspp::kevent<KEY, value_type>>(record, ev->id()));
        } else {
          // null output if left or right is not found
          auto record = std::make_shared<krecord<KEY, value_type>>(ev->record()->key(), nullptr, ev->event_time());
          this->send_to_sinks(std::make_shared<kspp::kevent<KEY, value_type>>(record, ev->id()));
        }
      }
      return processed;
    }

    void commit(bool flush) override {
      right_table_->commit(flush);
      left_table_->commit(flush);
    }

    bool eof() const override {
      return right_table_->eof() && left_table_->eof();
    }

  private:
    std::shared_ptr<materialized_source < KEY, LEFT>>  left_table_;
    std::shared_ptr<materialized_source < KEY, RIGHT>> right_table_;
  };
}
