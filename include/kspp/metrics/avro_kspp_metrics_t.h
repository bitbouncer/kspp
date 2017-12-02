/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#ifndef INCLUDE_KSPP_METRICS_AVRO_KSPP_METRICS_T_H_3834434456__H_
#define INCLUDE_KSPP_METRICS_AVRO_KSPP_METRICS_T_H_3834434456__H_


#include <sstream>
#include "boost/any.hpp"
#include "avro/Specific.hh"
#include "avro/Encoder.hh"
#include "avro/Decoder.hh"
#include "avro/Compiler.hh"

namespace avro {
struct kspp_metrics_tags_t {
    std::string name;
    std::string value;
    kspp_metrics_tags_t() :
        name(std::string()),
        value(std::string())
        { }
//avro extension
  static inline const char*                       schema_as_string() { return "{\"type\":\"record\",\"name\":\"kspp_metrics_t\",\"fields\":[{\"name\":\"measurement\",\"type\":\"string\"},{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"kspp_metrics_tags_t\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"value\",\"type\":\"string\"}]}}},{\"name\":\"fields\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"kspp_metrics_fields_t\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"value\",\"type\":\"string\"}]}}}]}"; } 
  static std::shared_ptr<const avro::ValidSchema> valid_schema()     { static const std::shared_ptr<const avro::ValidSchema> _validSchema(std::make_shared<const avro::ValidSchema>(avro::compileJsonSchemaFromString(schema_as_string()))); return _validSchema; }
  static std::string                              name()             { return "avro::kspp_metrics_tags_t"; } 
};

struct kspp_metrics_fields_t {
    std::string name;
    std::string value;
    kspp_metrics_fields_t() :
        name(std::string()),
        value(std::string())
        { }
//avro extension
  static inline const char*                       schema_as_string() { return "{\"type\":\"record\",\"name\":\"kspp_metrics_t\",\"fields\":[{\"name\":\"measurement\",\"type\":\"string\"},{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"kspp_metrics_tags_t\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"value\",\"type\":\"string\"}]}}},{\"name\":\"fields\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"kspp_metrics_fields_t\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"value\",\"type\":\"string\"}]}}}]}"; } 
  static std::shared_ptr<const avro::ValidSchema> valid_schema()     { static const std::shared_ptr<const avro::ValidSchema> _validSchema(std::make_shared<const avro::ValidSchema>(avro::compileJsonSchemaFromString(schema_as_string()))); return _validSchema; }
  static std::string                              name()             { return "avro::kspp_metrics_fields_t"; } 
};

struct kspp_metrics_t {
    std::string measurement;
    std::vector<kspp_metrics_tags_t > tags;
    std::vector<kspp_metrics_fields_t > fields;
    kspp_metrics_t() :
        measurement(std::string()),
        tags(std::vector<kspp_metrics_tags_t >()),
        fields(std::vector<kspp_metrics_fields_t >())
        { }
//avro extension
  static inline const char*                       schema_as_string() { return "{\"type\":\"record\",\"name\":\"kspp_metrics_t\",\"fields\":[{\"name\":\"measurement\",\"type\":\"string\"},{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"kspp_metrics_tags_t\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"value\",\"type\":\"string\"}]}}},{\"name\":\"fields\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"kspp_metrics_fields_t\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"value\",\"type\":\"string\"}]}}}]}"; } 
  static std::shared_ptr<const avro::ValidSchema> valid_schema()     { static const std::shared_ptr<const avro::ValidSchema> _validSchema(std::make_shared<const avro::ValidSchema>(avro::compileJsonSchemaFromString(schema_as_string()))); return _validSchema; }
  static std::string                              name()             { return "avro::kspp_metrics_t"; } 
};

}
namespace avro {
template<> struct codec_traits<avro::kspp_metrics_tags_t> {
    static void encode(Encoder& e, const avro::kspp_metrics_tags_t& v) {
        avro::encode(e, v.name);
        avro::encode(e, v.value);
    }
    static void decode(Decoder& d, avro::kspp_metrics_tags_t& v) {
        if (avro::ResolvingDecoder *rd =
            dynamic_cast<avro::ResolvingDecoder *>(&d)) {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (std::vector<size_t>::const_iterator it = fo.begin();
                it != fo.end(); ++it) {
                switch (*it) {
                case 0:
                    avro::decode(d, v.name);
                    break;
                case 1:
                    avro::decode(d, v.value);
                    break;
                default:
                    break;
                }
            }
        } else {
            avro::decode(d, v.name);
            avro::decode(d, v.value);
        }
    }
};

template<> struct codec_traits<avro::kspp_metrics_fields_t> {
    static void encode(Encoder& e, const avro::kspp_metrics_fields_t& v) {
        avro::encode(e, v.name);
        avro::encode(e, v.value);
    }
    static void decode(Decoder& d, avro::kspp_metrics_fields_t& v) {
        if (avro::ResolvingDecoder *rd =
            dynamic_cast<avro::ResolvingDecoder *>(&d)) {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (std::vector<size_t>::const_iterator it = fo.begin();
                it != fo.end(); ++it) {
                switch (*it) {
                case 0:
                    avro::decode(d, v.name);
                    break;
                case 1:
                    avro::decode(d, v.value);
                    break;
                default:
                    break;
                }
            }
        } else {
            avro::decode(d, v.name);
            avro::decode(d, v.value);
        }
    }
};

template<> struct codec_traits<avro::kspp_metrics_t> {
    static void encode(Encoder& e, const avro::kspp_metrics_t& v) {
        avro::encode(e, v.measurement);
        avro::encode(e, v.tags);
        avro::encode(e, v.fields);
    }
    static void decode(Decoder& d, avro::kspp_metrics_t& v) {
        if (avro::ResolvingDecoder *rd =
            dynamic_cast<avro::ResolvingDecoder *>(&d)) {
            const std::vector<size_t> fo = rd->fieldOrder();
            for (std::vector<size_t>::const_iterator it = fo.begin();
                it != fo.end(); ++it) {
                switch (*it) {
                case 0:
                    avro::decode(d, v.measurement);
                    break;
                case 1:
                    avro::decode(d, v.tags);
                    break;
                case 2:
                    avro::decode(d, v.fields);
                    break;
                default:
                    break;
                }
            }
        } else {
            avro::decode(d, v.measurement);
            avro::decode(d, v.tags);
            avro::decode(d, v.fields);
        }
    }
};

}
#endif
