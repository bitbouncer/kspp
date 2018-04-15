#include <iostream>
#include <chrono>
#include <kspp/topology_builder.h>
#include <kspp/sinks/kafka_sink.h>
#include <kspp/sources/kafka_source.h>
#include <kspp/processors/flat_map.h>
#include <kspp/utils/kafka_utils.h>
#include <kspp/avro/avro_serdes.h>
#include <kspp/impl/serdes/text_serdes.h>
#include <kspp/metrics/influx_metrics_reporter.h>
#include <kspp/utils/env.h>
#include <boost/lexical_cast.hpp>


#define SERVICE_NAME "telegraf-ingest"

using namespace std::chrono_literals;


std::string default_hostname() {
  char s[256];
  sprintf(s, "unknown");
  gethostname(s, 256);
  return std::string(s);
}

std::string default_metrics_tags(std::string app_name) {
  std::string s = "env=dev,serviceName=" + app_name + ",instanceId=dev,machineName=" + default_hostname();
  return s;
}

std::string to_string(const metrics20::avro::metrics20_key_t& key)
{
  std::string s;
  for (std::vector<metrics20::avro::metrics20_key_tags_t>::const_iterator i = key.tags.begin(); i!=key.tags.end(); ++i)
  {
    s += i->key + "=" + i->value;
    if (i != key.tags.end()-1){
      s += ", ";
    }
  }
  return s;
}

inline void add_tag(metrics20::avro::metrics20_key_t& key, const char* tag_key, size_t tag_key_sz, const char* tag_val, size_t tag_val_sz)
{
  metrics20::avro::metrics20_key_tags_t tag;
  tag.key = std::string(tag_key, tag_key_sz);
  tag.value = std::string(tag_val, tag_val_sz);
  key.tags.push_back(tag);
}

static bool tag_order_fn (const metrics20::avro::metrics20_key_tags_t& i,const metrics20::avro::metrics20_key_tags_t& j) { return (i.key<j.key); }

inline void sort_tags(metrics20::avro::metrics20_key_t& key) {
  std::sort(key.tags.begin(), key.tags.end(), tag_order_fn);
}


struct metrics_item {
  metrics_item(const metrics20::avro::metrics20_key_t& k) : key(k), val(0), ts(0) {}
  metrics20::avro::metrics20_key_t key;
  double val;
  int64_t ts;
};

//weather,location=us-midwest temperature=82,bug_concentration=98 1465839830100400200

//key
//tags
//what=weather
//location=us-midwest
//mtype=temperature
//unit=""

//value
//82

//ts 1465839830100


//what=weather
//location=us-midwest
//mtype=bug_concentration
//unit=""

//value
//98

//ts 1465839830100

std::vector<metrics_item> parse_line(const char* buf, size_t len)
{
  const char* cursor=buf;
  const char* end=buf+len;

  metrics20::avro::metrics20_key_t generic_key;
  std::vector<metrics_item> line_metrics;

  while (end - cursor > 0) {
    const char *p = (const char *) memchr(cursor, ',', end - cursor);
    if (!p)
      break;
    //std::cerr << "what:" << std::string(cursor, p - cursor) << std::endl;
    add_tag(generic_key, "key", 3, cursor, p - cursor) ;
    cursor = p + 1;
    if (end - cursor <= 0)
      break;
    const char *end_of_tags = (const char *) memchr(cursor, ' ', end - cursor);

    while(end_of_tags - cursor>0) {
      const char *end_of_tag = (const char *) memchr(cursor, ',', end_of_tags - cursor);
      if (!end_of_tag)
        end_of_tag = end_of_tags;

      const char* kv_split = (const char *) memchr(cursor, '=', end_of_tag - cursor);
      if (kv_split){
        add_tag(generic_key, cursor, kv_split - cursor, kv_split+1, end_of_tag - (kv_split+1));
      }
      cursor = end_of_tag + 1;
    }

    while(end - cursor>0) {

      const char *end_of_measurement = (const char *) memchr(cursor, ',', end - cursor);
      if (!end_of_measurement) {
        // last ',' so lets find LAST space of line instead
        size_t pos = std::string(cursor, end-cursor).find_last_of(' ');
        if (pos==std::string::npos){
          // this means we're at the timestamp
          break;
        } else {
          end_of_measurement = cursor + pos;
        }
        //end_of_measurement = end_of_measurements;
      }

      const char* kv_split = (const char *) memchr(cursor, '=', end_of_measurement - cursor);
      if (kv_split) {
        metrics_item item(generic_key);
        // first part of this is mtype  second part goes to value
        std::string value(kv_split + 1, end_of_measurement - (kv_split + 1));
        add_tag(item.key, "what", 4, cursor, kv_split - cursor);
        add_tag(item.key, "mtype", 5, "", 0);        // [rate,count,gauge,counter,timestamp]
        add_tag(item.key, "unit", 4, "undefined", 9);  //
        try {
          //influx adds 'i to integer values which lexical_cast frowns upon
          if (value.size() && value.back() =='i')
            value.pop_back();
          item.val = boost::lexical_cast<double>(value);
          sort_tags(item.key);
          line_metrics.push_back(item);
        } catch (std::exception& e)
        {
          //std::cerr << "bad val: " << e.what() << " " << value << std::endl;
          //return std::vector<metrics_item>();
        }

      }
      cursor = end_of_measurement + 1;
    }

    int64_t line_ts=0;
    if (cursor<end) {
      //this seems to be in nanoseconds so kill the last 6 digits
      size_t digits = end - cursor;
      if (digits > 6)
        digits -= 6;

      try {
        line_ts = std::stoll(std::string(cursor, digits));
      }
      catch(std::exception& e)
      {
        std::cerr << "bad ts: " << digits << std::endl;
        return std::vector<metrics_item>();
      }
    }

    // we did not get any ts??
    if (line_ts==0)
      line_ts = kspp::milliseconds_since_epoch();

    for (std::vector<metrics_item>::iterator i=line_metrics.begin(); i!=line_metrics.end(); ++i)
      i->ts = line_ts;
  }
  return line_metrics;
}

static bool run = true;
static void sigterm(int sig) {
  run = false;
}

int main(int argc, char **argv) {
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);

  std::string metrics_tags = default_metrics_tags(SERVICE_NAME);


  auto config = std::make_shared<kspp::cluster_config>();
  config->load_config_from_env();
  config->set_consumer_buffering_time(10ms);
  config->set_producer_buffering_time(10ms);
  config->validate();// optional
  config->log(); // optional

  auto partitions = kspp::kafka::get_number_partitions(config, "telegraf");
  auto partition_list = kspp::get_partition_list(partitions);

  auto builder = kspp::topology_builder("metrics2", SERVICE_NAME, config);

  auto topology = builder.create_topology();

  auto sources = topology->create_processors<kspp::kafka_source<std::string, std::string, kspp::text_serdes, kspp::text_serdes>>(
      partition_list,
      "telegraf");

  auto transforms = topology->create_processors<kspp::flat_map<std::string, std::string, metrics20::avro::metrics20_key_t, double>>(
      sources, [](const auto record, auto flat_map) {
        if (record->value()) {
          auto v = parse_line(record->value()->data(), record->value()->size());
          for (auto m : v) {
            flat_map->push_back(kspp::make_krecord(m.key, m.val, m.ts));
          }
        }
      });

  auto sink = topology->create_sink<kspp::kafka_sink<metrics20::avro::metrics20_key_t, double, kspp::avro_serdes, kspp::avro_serdes>>(
      "metrics2_telegraf_raw",
      config->avro_serdes(),
      config->avro_serdes());

  for (auto i : transforms)
    i->add_sink(sink);

  topology->init_metrics();
  topology->start(kspp::OFFSET_STORED);
  //topology->start(kspp::OFFSET_BEGINNING);

  std::signal(SIGINT, sigterm);
  std::signal(SIGTERM, sigterm);
  std::signal(SIGPIPE, SIG_IGN);

  LOG(INFO) << "status is up";


  /*
   * topology->for_each_metrics([](kspp::metric &m) {
    std::cerr << "metrics: " << m.name() << " : " << m.value() << std::endl;
  });
   */


  // output metrics and run
  {
    auto metrics_reporter = std::make_shared<kspp::influx_metrics_reporter>(builder, "kspp_metrics", "kspp", metrics_tags) << topology;
    while (run) {
      if (topology->process(kspp::milliseconds_since_epoch()) == 0) {
        std::this_thread::sleep_for(10ms);
        topology->commit(false);
      }
    }
  }

  topology->for_each_metrics([](kspp::metric &m) {
    std::cerr << "metrics: " << m.name() << " " << m.tags() << " : " << m.value() << std::endl;
  });

  topology->commit(true);
  topology->close();
  return 0;
}

