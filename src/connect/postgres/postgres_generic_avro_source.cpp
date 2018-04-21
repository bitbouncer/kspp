#include <kspp/connect/postgres/postgres_generic_avro_source.h>
#include <kspp/connect/postgres/postgres_avro_utils.h>
#include <glog/logging.h>

using namespace std::chrono_literals;

namespace kspp
{
  postgres_generic_avro_source::postgres_generic_avro_source(topology &t,
                                                             int32_t partition,
                                                             std::string table,
                                                             std::string connect_string,
                                                             std::string id_column,
                                                             std::string ts_column)
      : partition_source<void, kspp::GenericAvro>(nullptr, partition)
      , _started(false)
      , _exit(false)
      , _thread(&postgres_generic_avro_source::thread_f, this)
      , _impl(partition, table, t.consumer_group(), connect_string, id_column, ts_column)
      , _commit_chain(table, partition)
      , _parse_errors("parse_errors", "err")
      , _commit_chain_size("commit_chain_size", metric::GAUGE, "msg", [this]() { return _commit_chain.size(); })
  {
    this->add_metric(&_commit_chain_size);
    this->add_metrics_tag(KSPP_PROCESSOR_TYPE_TAG, PROCESSOR_NAME);
    this->add_metrics_tag(KSPP_TOPIC_TAG, table);
    this->add_metrics_tag(KSPP_PARTITION_TAG, std::to_string(partition));
  }


// this is done by mapping schema field names to result columns...
  void postgres_generic_avro_source::parse(const PGresult* result) {
    if (!result)
      return;

    // first time?
    if (!this->_schema)
      this->_schema = std::make_shared<avro::ValidSchema>(*schema_for_table_row(this->topic(), result));

    int nRows = PQntuples(result);

    for (int i = 0; i < nRows; i++)
    {
      auto gd = std::make_shared<kspp::GenericAvro>(this->_schema, -1); // here we should really have the option to connect to schema registry and add the correct id...
      assert(gd->type() == avro::AVRO_RECORD);
      avro::GenericRecord& record(gd->generic_datum()->value<avro::GenericRecord>());
      size_t nFields = record.fieldCount();
      for (int j = 0; j < nFields; j++)
      {
        if (record.fieldAt(j).type() != avro::AVRO_UNION)
        {
          std::cerr << "unexpected schema - bailing out, type:" << record.fieldAt(j).type() << std::endl;
          assert(false);
          break;
        }
        avro::GenericUnion& au(record.fieldAt(j).value<avro::GenericUnion>());

        const std::string& column_name = record.schema()->nameAt(j);

//which pg column has this value?
        int column_index = PQfnumber(result, column_name.c_str());
        if (column_index < 0)
        {
          std::cerr << "unknown column - bailing out: " << column_name << std::endl;
          assert(false);
          break;
        }

        if (PQgetisnull(result, i, column_index) == 1)
        {
          au.selectBranch(0); // NULL branch - we hope..
          assert(au.datum().type() == avro::AVRO_NULL);
        }
        else
        {
          au.selectBranch(1);
          avro::GenericDatum& avro_item(au.datum());
          const char* val = PQgetvalue(result, i, j);

          switch (avro_item.type())
          {
            case avro::AVRO_STRING:
              avro_item.value<std::string>() = val;
              break;
            case avro::AVRO_BYTES:
              avro_item.value<std::string>() = val;
              break;
            case avro::AVRO_INT:
              avro_item.value<int32_t>() = atoi(val);
              break;
            case avro::AVRO_LONG:
              avro_item.value<int64_t>() = std::stoull(val);
              break;
            case avro::AVRO_FLOAT:
              avro_item.value<float>() = (float)atof(val);
              break;
            case avro::AVRO_DOUBLE:
              avro_item.value<double>() = atof(val);
              break;
            case avro::AVRO_BOOL:
              avro_item.value<bool>() = (strcmp(val, "True") == 0);
              break;
            case avro::AVRO_RECORD:
            case avro::AVRO_ENUM:
            case avro::AVRO_ARRAY:
            case avro::AVRO_MAP:
            case avro::AVRO_UNION:
            case avro::AVRO_FIXED:
            case avro::AVRO_NULL:
            default:
              std::cerr << "unexpectd / non supported type e:" << avro_item.type() << std::endl;;
              assert(false);
          }
        }
      }
      int64_t ts = 0; // TODO get the timestamp column
      auto r = std::make_shared<krecord<void, kspp::GenericAvro>>(gd, ts);
      auto e = std::make_shared<kevent<void, kspp::GenericAvro>>(r);
      assert(e.get()!=nullptr);
      _incomming_msg.push_back(e);
    }
  }

  void postgres_generic_avro_source::thread_f()
  {
    while(!_started)
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    DLOG(INFO) << "starting thread";

    while (!_impl.is_connected())
      std::this_thread::sleep_for(std::chrono::milliseconds(100));

    DLOG(INFO) << "consumption phase";

    while(!_exit) {
      auto tick = kspp::milliseconds_since_epoch();
      while (auto p = _impl.consume()) {
        parse(p.get());
      }

      // to much work in queue - back off and let the consumers work
      while(_incomming_msg.size()>10000 && !_exit) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        DLOG(INFO) << "c_incomming_msg.size() " << _incomming_msg.size();
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    DLOG(INFO) << "exiting thread";
  }
}