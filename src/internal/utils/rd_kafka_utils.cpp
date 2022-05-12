#include <kspp/internal/rd_kafka_utils.h>
#include <thread>
#include <chrono>
#include <glog/logging.h>
#include <kspp/utils/url_parser.h>

using namespace std::chrono_literals;

void set_config(RdKafka::Conf *conf, std::string key, std::string value) {
  std::string errstr;
  if (conf->set(key, value, errstr) != RdKafka::Conf::CONF_OK) {
    throw std::invalid_argument("\"" + key + "\" -> " + value + ", error: " + errstr);
  }
  DLOG(INFO) << "rd_kafka set_config: " << key << "->" << value;
}

void set_config(RdKafka::Conf *conf, std::string key, RdKafka::Conf *topic_conf) {
  std::string errstr;
  if (conf->set(key, topic_conf, errstr) != RdKafka::Conf::CONF_OK) {
    throw std::invalid_argument("\"" + key + ", error: " + errstr);
  }
}

void set_config(RdKafka::Conf *conf, std::string key, RdKafka::DeliveryReportCb *callback) {
  std::string errstr;
  if (conf->set(key, callback, errstr) != RdKafka::Conf::CONF_OK) {
    throw std::invalid_argument("\"" + key + "\", error: " + errstr);
  }
}

void set_config(RdKafka::Conf *conf, std::string key, RdKafka::PartitionerCb *partitioner_cb) {
  std::string errstr;
  if (conf->set(key, partitioner_cb, errstr) != RdKafka::Conf::CONF_OK) {
    throw std::invalid_argument("\"" + key + "\", error: " + errstr);
  }
}

void set_config(RdKafka::Conf *conf, std::string key, RdKafka::EventCb *event_cb) {
  std::string errstr;
  if (conf->set(key, event_cb, errstr) != RdKafka::Conf::CONF_OK) {
    throw std::invalid_argument("\"" + key + "\", error: " + errstr);
  }
}

void set_broker_config(RdKafka::Conf *rd_conf, const kspp::cluster_config *config) {
  auto v = kspp::split_url_list(config->get_brokers());

  set_config(rd_conf, "metadata.broker.list", config->get_brokers());

  if ((v.size() > 0) && v[0].scheme() == "ssl") {
    // SSL no auth - always
    set_config(rd_conf, "security.protocol", "ssl");
    set_config(rd_conf, "ssl.ca.location", config->get_ca_cert_path());

    //do we have client certs
    if (config->get_client_cert_path().size() > 0 && config->get_private_key_path().size() > 0) {
      set_config(rd_conf, "ssl.certificate.location", config->get_client_cert_path());
      set_config(rd_conf, "ssl.key.location", config->get_private_key_path());
      // optional password
      if (config->get_private_key_passphrase().size())
        set_config(rd_conf, "ssl.key.password", config->get_private_key_passphrase());
    }
  }
}


/*-----------------------------------------------------------------------------
// Based on MurmurHashNeutral2, by Austin Appleby
// COPY of private function in librdkafka
// Same as MurmurHash2, but endian- and alignment-neutral.
// Half the speed though, alas.
//
*/
#define MM_MIX(h, k, m)                                                        \
        {                                                                      \
                k *= m;                                                        \
                k ^= k >> r;                                                   \
                k *= m;                                                        \
                h *= m;                                                        \
                h ^= k;                                                        \
        }
namespace kspp {
  uint32_t rd_murmur2(const void *key, size_t len) {
    const uint32_t seed = 0x9747b28c;
    const uint32_t m = 0x5bd1e995;
    const int r = 24;
    uint32_t h = seed ^ (uint32_t) len;
    const unsigned char *tail;

    if (((intptr_t) key & 0x3) == 0) [[likely]] {
      /* Input is 32-bit word aligned. */
      const uint32_t *data = (const uint32_t *) key;

      while (len >= 4) {
        uint32_t k = htole32(*(uint32_t *) data);

        MM_MIX(h, k, m);

        data++;
        len -= 4;
      }

      tail = (const unsigned char *) data;

    } else [[unlikely]] {
      /* Unaligned slower variant */
      const unsigned char *data = (const unsigned char *) key;

      while (len >= 4) {
        uint32_t k;

        k = data[0];
        k |= data[1] << 8;
        k |= data[2] << 16;
        k |= data[3] << 24;

        MM_MIX(h, k, m);

        data += 4;
        len -= 4;
      }

      tail = data;
    }

    /* Read remaining sub-word */
    switch (len) {
      case 3:
        h ^= tail[2] << 16;
      case 2:
        h ^= tail[1] << 8;
      case 1:
        h ^= tail[0];
        h *= m;
    };

    h ^= h >> 13;
    h *= m;
    h ^= h >> 15;

    /* Last bit is set to 0 because the java implementation uses int_32
     * and then sets to positive number flipping last bit to 1. */
    return h;
  }
}



