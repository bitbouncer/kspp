#include <set>
#include <avro/Generic.hh>
#include <avro/DataFile.hh>
#include <avro/ValidSchema.hh>
#include <avro/Compiler.hh>
#include <avro/Specific.hh>

#include <istream>
#include <fstream>
#include <string>
#include <vector>
#include <experimental/filesystem>
#include <nlohmann/json.hpp>
#include <glog/logging.h>
#include <kspp/utils/kspp_utils.h>
#include <boost/program_options.hpp>

const std::string WHITESPACE = " \n\r\t\f\v";

std::string ltrim(const std::string& s)
{
  size_t start = s.find_first_not_of(WHITESPACE);
  return (start == std::string::npos) ? "" : s.substr(start);
}

std::string rtrim(const std::string& s)
{
  size_t end = s.find_last_not_of(WHITESPACE);
  return (end == std::string::npos) ? "" : s.substr(0, end + 1);
}

std::string trim(const std::string& s)
{
  return rtrim(ltrim(s));
}

/*
 * credits:
 * https://stackoverflow.com/users/25450/sastanin
 * https://stackoverflow.com/questions/1120140/how-can-i-read-and-parse-csv-files-in-c
*/

namespace fs = std::experimental::filesystem;
using json = nlohmann::json;

static std::string to_lower(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) {
    return std::tolower(c);
  });
  return s;
}

static std::string remove_non_alpha_num(std::string s) {
  s.erase(std::remove_if(s.begin(), s.end(), [](char ch) {
    return !(::isalnum(ch) || ch == '_');
  }), s.end());
  return s;
}

static std::string replace_space(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(), [](char ch) {
    return ch == ' ' ? '_' : ch;
  });
  return s;
}

static std::map<std::string, std::string> s_translation = {
  { "table", "_table_" }
};

static std::string translate_forbidden_keywords(std::string s) {
  auto item = s_translation.find(s);
  if (item != s_translation.end())
    return item->second;
  return s;
}

static std::string sanitize_column_name(std::string s){
  s = trim(s);
  s = to_lower(s);
  s = replace_space(s);
  s = remove_non_alpha_num(s);
  s = translate_forbidden_keywords(s);
  return s;
}

enum class CSVState {
  UnquotedField,
  QuotedField,
  QuotedQuote
};

static std::vector<std::string> parseCSVRow(const std::string &row) {
  CSVState state = CSVState::UnquotedField;
  std::vector<std::string> fields {""};
  size_t i = 0; // index of the current field
  for (char c : row) {
    switch (state) {
      case CSVState::UnquotedField:
        switch (c) {
          case ',': // end of field
            fields.push_back(""); i++;
            break;
          case '"': state = CSVState::QuotedField;
            break;
          default:  fields[i].push_back(c);
            break; }
        break;
      case CSVState::QuotedField:
        switch (c) {
          case '"': state = CSVState::QuotedQuote;
            break;
          default:  fields[i].push_back(c);
            break; }
        break;
      case CSVState::QuotedQuote:
        switch (c) {
          case ',': // , after closing quote
            fields.push_back(""); i++;
            state = CSVState::UnquotedField;
            break;
          case '"': // "" -> "
            fields[i].push_back('"');
            state = CSVState::QuotedField;
            break;
          default:  // end of quote
            state = CSVState::UnquotedField;
            break; }
        break;
    }
  }
  //cleanup leading or trailing whitespaces
  if (fields.size()){
    fields[0] = trim(fields[0]);
    fields[fields.size()-1] = trim(fields[fields.size()-1]);
  }

  return fields;
}

void set_member(avro::GenericRecord& record, std::string member, std::string value){
  /*if (!record.hasField(member))
    // trow
  */
  avro::GenericDatum &column = record.field(member);
  if (column.isUnion()){
    column.selectBranch(1);
    assert(column.type() == avro::AVRO_STRING); // here we can make a switch on column type and probably assign right from beginning
    column.value<std::string>() = value;
  } else {
    column.value<std::string>() = value;
  }
  //throw std::invalid_argument(name() + "." + member + ": wrong type, expected:" + avro_utils::to_string( avro_utils::cpp_to_avro_type<T>()) +  ", actual: " +  avro_utils::to_string(datum.type()));
}

int main(int argc, char** argv) {
  FLAGS_logtostderr = 1;
  google::InitGoogleLogging(argv[0]);
  boost::program_options::options_description desc("options");
  desc.add_options()
    ("help", "produce help message")
    ("src", boost::program_options::value<std::string>(), "src")
    ("dst", boost::program_options::value<std::string>(), "dst")
    ("column_names", boost::program_options::value<std::string>(), "column_names")
    ("static_column", boost::program_options::value<std::string>(), "static_column")
    ("keys", boost::program_options::value<std::string>(), "keys");

  boost::program_options::variables_map vm;
  boost::program_options::store(boost::program_options::parse_command_line(argc, argv, desc), vm);
  boost::program_options::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << std::endl;
    return 0;
  }

  std::string src;
  if (vm.count("src")) {
    src = vm["src"].as<std::string>();
  } else {
    std::cout << "--src must be specified" << std::endl;
    return 1;
  }

  std::string dst;
  if (vm.count("dst")) {
    dst = vm["dst"].as<std::string>();
  } else {
    std::cout << "--dst must be specified" << std::endl;
    return 1;
  }

  std::set<std::string> keys;
  if (vm.count("keys")) {
    std::string s = vm["keys"].as<std::string>();
    auto v = kspp::parse_string_array(s);
    for (auto i : v)
      keys.insert(sanitize_column_name(i));
  }

  std::vector<std::string> column_names_in_csv;
  if (vm.count("column_names")) {
    std::string s = vm["column_names"].as<std::string>();
    column_names_in_csv = kspp::parse_string_array(s);
    for (auto &i : column_names_in_csv)
      i = sanitize_column_name(i);
  }

  std::string static_column;
  std::string static_column_value;
  if (vm.count("static_column")) {
    auto s = vm["static_column"].as<std::string>();
    size_t delim = s.find("=");
    if (delim!=std::string::npos){
      static_column = sanitize_column_name(s.substr(0, delim));
      static_column_value = s.substr(delim+1);
    }
  }

  LOG(INFO) << "src:           " << src;
  LOG(INFO) << "dst:           " << dst;
  if (column_names_in_csv.size())
    LOG(INFO) << "column_names:  " << kspp::to_string(column_names_in_csv);
  LOG(INFO) << "keys:          " << kspp::to_string(keys);
  if (static_column.size())
    LOG(INFO) << "static_column: " << static_column << " -> " << static_column_value;

  std::vector<std::vector<std::string>> table;
  std::string row;

  if (!fs::exists(src)) {
    std::cerr << "cannot find src:" << src;
    return -1;
  }

  std::fstream in(src, std::ios::binary | std::ios::in);

  // read first line and make an avro schema
  //if we were given column names then skip reading of them in file
  if (column_names_in_csv.size() == 0){
    LOG(INFO) << "scanning column names from file";
    std::getline(in, row);
    column_names_in_csv = parseCSVRow(row);
    for (auto &i :column_names_in_csv)
      i = sanitize_column_name(i);
    LOG(INFO) << "column_names:  " << kspp::to_string(column_names_in_csv);
  }

  std::vector<std::string> column_names_in_schema = column_names_in_csv;

  if (static_column.size()){
    column_names_in_schema.push_back(static_column);
  }

  json nullable_string = json::array();
  nullable_string.push_back("null");
  nullable_string.push_back("string");

  json j;
  j["type"] = "record";
  j["name"] = "csv_import";
  j["fields"] = json::array();

  for (auto i : column_names_in_schema) {
    std::cout << i << ", ";
    json column;
    column["name"] = i;
    if (keys.find(i) != keys.end())
      column["type"] = "string";
    else
      column["type"] = nullable_string;
    j["fields"].push_back(column);
  }
  std::cout << std::endl;
  std::cout << j.dump(2) << std::endl;

  std::stringstream s;
  s << j.dump(4) << std::endl;

  avro::ValidSchema valid_schema_;

  try {
    //std::ifstream in(avro_schema);
    avro::compileJsonSchema(s, valid_schema_);
  } catch (std::exception &e) {
    std::cerr << "exception parsing schema " << e.what();
    return -1;
  }

  auto file_writer = std::make_shared<avro::DataFileWriter<avro::GenericDatum>>(dst.c_str(), valid_schema_, 10 * 1024 * 1024, avro::SNAPPY_CODEC);

  size_t messages_in_file = 0;
  LOG(INFO) << "starting...";

  while (!in.eof()) {
    std::getline(in, row);
    if (in.bad() || in.fail()) {
      break;
    }
    auto fields = parseCSVRow(row);

    if (fields.size() != column_names_in_csv.size()) {
      std::cerr << "skipping row with to different nr of columns" << std::endl;
      continue;
    }


    auto gd = std::make_shared<avro::GenericDatum>(valid_schema_);
    avro::GenericRecord &record = gd->value<avro::GenericRecord>();
    int sz = column_names_in_csv.size();
    for (int i = 0; i != sz; ++i) {
      // skip assigning columns with empty strings - keys will be empty and the rest NULL
      if (fields[i].size() == 0)
        continue;
      set_member(record, column_names_in_csv[i], fields[i]);
    }

    if (static_column.size()){
      if (static_column_value.size())
        set_member(record, static_column, static_column_value);
    }

    ++messages_in_file;
    file_writer->write(*gd);

    //std::cerr << "+";
    //table.push_back(fields);
  }
  file_writer->flush();
  file_writer->close();
  file_writer.reset();
  LOG(INFO) << "file: " << dst << " closed - written " << messages_in_file << " messages";
}


