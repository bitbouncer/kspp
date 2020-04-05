#include <kspp/sources/avro_file_source.h>
#include <memory>
#include <avro/Generic.hh>
#include <avro/DataFile.hh>
#include <kspp/avro/avro_utils.h>
#include <kspp/avro/generic_avro.h>
#include <kspp/kspp.h>

namespace kspp {
  generic_avro_file_source::generic_avro_file_source(std::shared_ptr<cluster_config> config, int32_t partition, std::string filename)
    : partition_source<void, kspp::generic_avro>(nullptr, partition)
      ,thread_(&generic_avro_file_source::thread_f, this)
      , filename_(filename) {
    this->add_metrics_label(KSPP_PROCESSOR_TYPE_TAG, PROCESSOR_NAME);
    // if a given  a directory scan directory and add all avro files??
  }

  generic_avro_file_source::~generic_avro_file_source() {
    this->close();
  }

  void generic_avro_file_source::start(int64_t offset){
    started_ = true;
  }

  void generic_avro_file_source::commit(bool flush) {
  // do nothing - since we only support start from beginning
  }

  void generic_avro_file_source::close() {
    if (!exit_) {
      exit_ = true;
      thread_.join();
    }
    LOG(INFO) << PROCESSOR_NAME << " processor closed - produced " << this->_processed_count.value() << " messages";
  }

  bool generic_avro_file_source::eof() const {
    return incomming_msg_.size() == 0 && eof_;
  }

  size_t generic_avro_file_source::queue_size() const {
    return incomming_msg_.size();
  }

  int64_t generic_avro_file_source::next_event_time() const {
    return incomming_msg_.next_event_time();
  }

  size_t generic_avro_file_source::process(int64_t tick) {
    if (incomming_msg_.size() == 0)
      return 0;
    size_t processed = 0;
    while (!incomming_msg_.empty()) {
      auto p = incomming_msg_.front();
      if (p == nullptr || p->event_time() > tick)
        return processed;
      incomming_msg_.pop_front();
      this->send_to_sinks(p);
      ++(this->_processed_count);
      ++processed;
      this->_lag.add_event_time(tick, p->event_time());
    }
    return processed;
  }

  std::string generic_avro_file_source::topic() const {
    return filename_;
  }

  void generic_avro_file_source::thread_f() {
    while (!started_)
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    DLOG(INFO) << "starting thread";

    avro::DataFileReader<generic_avro> reader(filename_.c_str());
    auto dataSchema = reader.dataSchema();
    auto valid_schema = std::make_shared<const avro::ValidSchema>(dataSchema);

    std::strstream output;
    dataSchema.toJson(output);
    LOG(INFO) << output.str();
    //avro::GenericDatum datum(dataSchema);

    auto datum = std::make_shared<generic_avro>(valid_schema, -1);
    // Write out data schema in JSON for grins
    while (!exit_ && reader.read(*datum)) {
      auto record = std::make_shared<krecord<void, generic_avro>>(datum, kspp::milliseconds_since_epoch());
      auto ev = std::make_shared<kevent<void, generic_avro>>(record);
      incomming_msg_.push_back(ev);
      datum = std::make_shared<generic_avro>(valid_schema, -1); // create a new item to send
      // to much work in queue - back off and let the consumers work
      size_t sz = incomming_msg_.size();
      while (incomming_msg_.size() > max_incomming_queue_size_ && !exit_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
    }
    eof_ = true;
    DLOG(INFO) << "exiting thread";
  }
}