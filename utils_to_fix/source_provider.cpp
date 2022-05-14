#include <kspp/sources/source_provider.h>
#include <boost/algorithm/string.hpp>

namespace kspp {
  source_parts split_url_parts(std::string uri) {
    //if (uri.size() >5 && boost::iequals(uri.substr(0, 5), ("s3://")))
    //  return S3;
    source_parts parts;
    parts.protocol = source_parts::NONE;


    if (uri.size() > 8 && boost::iequals(uri.substr(0, 8), ("kafka://"))) {
      parts.protocol = source_parts::KAFKA;
      const size_t topic_start = 8;
      //parts.host = uri.substr(host_start, uri.find("/", host_start) - host_start);
      parts.topic = uri.substr(topic_start, uri.find(":", topic_start) - topic_start);
      parts.partition = 0; // fixme
    }

    if (uri.size() > 7 && boost::iequals(uri.substr(0, 7), ("avro://"))) {
      parts.protocol = source_parts::AVRO;
    }

    if (uri.size() > 6 && boost::iequals(uri.substr(0, 6), ("tds://"))) {
      parts.protocol = source_parts::TDS;
    }

    if (uri.size() > 11 && boost::iequals(uri.substr(0, 11), ("postgres://"))) {
      parts.protocol = source_parts::POSTGRES;
    }

    if (uri.size() > 5 && boost::iequals(uri.substr(0, 5), ("bb://"))) {
      const size_t host_start = 5;
      parts.protocol = source_parts::BB_GRPC;
      parts.host = uri.substr(host_start, uri.find("/", host_start) - host_start);
      const size_t topic_start = host_start + parts.host.size() + 1;
      parts.topic = uri.substr(topic_start, uri.find(":", topic_start) - topic_start);

      parts.partition = 0; // fixme

      if (parts.topic.empty()) {
        LOG(ERROR) << "bad topic";
        //return nullptr;
      }

      if (parts.host.empty()) {
        LOG(ERROR) << "bad host";
        //throw
      }
    }

    return parts;
  }
}