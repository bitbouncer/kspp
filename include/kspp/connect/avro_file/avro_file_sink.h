#include <memory>
#include <avro/Generic.hh>
#include <avro/DataFile.hh>
#include <kspp/kspp.h>
#pragma once

namespace kspp {
  class avro_file_sink : public topic_sink<void, kspp::GenericAvro> {
    static constexpr const char* PROCESSOR_NAME = "avro_file_sink";
  public:
    avro_file_sink(topology &t, std::string path)
        : topic_sink<void, kspp::GenericAvro>()
        , _path(path) {
      this->add_metrics_tag(KSPP_PROCESSOR_TYPE_TAG, PROCESSOR_NAME);
    }

    ~avro_file_sink() override {
      this->flush();
      this->close();
    }

    void close() override {
      if (_file_writer){
        _file_writer->flush();
        _file_writer->close();
        _file_writer.reset();
        LOG(INFO) << PROCESSOR_NAME << _path <<  " closed - consumed " << this->_processed_count.value() << " messages";
      }
    }

    std::string log_name() const override {
      return PROCESSOR_NAME;
    }

    size_t queue_size() const override {
      return event_consumer<void, kspp::GenericAvro>::queue_size();
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
        auto r = this->_queue.pop_and_get();
        this->_lag.add_event_time(tick, r->event_time());

        // first time create the file - we have to wait for first data to know the internal type..
        if (!_file_writer){
          auto schema = r->record()->value()->valid_schema();
          _file_writer = std::make_shared<avro::DataFileWriter<avro::GenericDatum>>(
              _path.c_str(), *schema, 16 * 1024, avro::DEFLATE_CODEC); //AVRO_DEFLATE_CODEC, NULL_CODEC
        }
        // null value protection
        if (r->record()->value()) {
          _file_writer->write(*r->record()->value()->generic_datum());
          //_file_writer->flush();
        }
        ++(this->_processed_count);
        ++processed;
      }
      return processed;
    }

  protected:
    std::shared_ptr<avro::DataFileWriter<avro::GenericDatum>> _file_writer;
    std::string _path;
  };
}