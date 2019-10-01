#include <memory>
#include <avro/Generic.hh>
#include <avro/DataFile.hh>
#include <kspp/avro/avro_utils.h>
#include <kspp/kspp.h>
#pragma once

namespace kspp {

  template<class V>
  class avro_file_sink : public topic_sink<void, V> {
    static constexpr const char* PROCESSOR_NAME = "avro_file_sink";
  public:
    avro_file_sink(std::shared_ptr<cluster_config> config, std::string parent_path, std::string base_name, std::chrono::seconds window_size)
        : topic_sink<void, V>()
        , _parent_path(parent_path)
        , _base_name(base_name)
        , _window_size(window_size) {
      this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, PROCESSOR_NAME);
    }

    ~avro_file_sink() override {
      this->flush();
      this->close();
    }

    void close() override {
      close_file();
      LOG(INFO) << PROCESSOR_NAME << " processor closed - consumed " << this->_processed_count.value() << " messages";
    }

    void close_file() {
      if (_file_writer) {
        _file_writer->flush();
        _file_writer->close();
        _file_writer.reset();
        LOG(INFO) << PROCESSOR_NAME << ", file: "  << _current_file_name << " closed - written " << _messages_in_file << " messages";
      }
    }

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }

    size_t queue_size() const override {
      return event_consumer<void, V>::queue_size();
    }

    void flush() override {
      while (process(kspp::milliseconds_since_epoch())>0)
      {
        ; // noop
      }
    }

    bool eof() const override {
      return this->_queue.size();
    }

    size_t process(int64_t tick) override {
      size_t processed =0;

      //forward up this timestamp
      while (this->_queue.next_event_time()<=tick){
        auto r = this->_queue.pop_front_and_get();
        this->_lag.add_event_time(tick, r->event_time());

        // check if it's time to rotate
        if (_file_writer && r->event_time()>= _end_of_windows_ts){
          close_file();
        }

        // time to create a new file?
        if (!_file_writer){
          //auto schema = r->record()->value()->valid_schema();
          auto schema = avro_utils::avro_utils<V>::valid_schema(*r->record()->value());
          _current_file_name = _parent_path + "/" + _base_name + "-" + std::to_string(r->event_time()) + ".avro";
          _end_of_windows_ts =  r->event_time() + (_window_size.count() * 1000); // should we make another kind of window that plays nice with 24h?
          _messages_in_file = 0;
#ifdef SNAPPY_CODEC_AVAILABLE
          _file_writer = std::make_shared<avro::DataFileWriter<V>>(_current_file_name.c_str(), *schema, 16 * 1024, avro::SNAPPY_CODEC);
#else
          _file_writer = std::make_shared<avro::DataFileWriter<V>>(_current_file_name.c_str(), *schema, 16 * 1024, avro::DEFLATE_CODEC);
#endif
        }

        // null value protection
        if (r->record()->value()) {
          ++_messages_in_file;
          //_file_writer->write(*r->record()->value()->generic_datum());
          _file_writer->write(*r->record()->value());
        }
        ++(this->_processed_count);
        ++processed;
      }
      return processed;
    }

  protected:
    std::shared_ptr<avro::DataFileWriter<V>> _file_writer;
    std::string _parent_path;
    std::string _base_name;
    std::string _current_file_name;
    const std::chrono::seconds _window_size;
    int64_t _end_of_windows_ts=0;
    int64_t _messages_in_file=0;
  };
}