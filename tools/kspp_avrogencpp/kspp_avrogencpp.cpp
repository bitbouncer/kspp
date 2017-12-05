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

#include <ctype.h>

#ifndef _WIN32

#include <sys/time.h>

#endif

#include <iostream>
#include <fstream>
#include <map>
#include <set>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/program_options.hpp>
#include <boost/filesystem.hpp>
#include <avro/Compiler.hh>
#include <avro/ValidSchema.hh>
#include <avro/NodeImpl.hh>

using std::ostream;
using std::ifstream;
using std::ofstream;
using std::map;
using std::set;
using std::string;
using std::vector;
using avro::NodePtr;
using avro::resolveSymbol;

using boost::lexical_cast;

using avro::ValidSchema;
using avro::compileJsonSchema;

struct PendingSetterGetter {
  string structName;
  string type;
  string name;
  size_t idx;

  PendingSetterGetter(const string &sn,
                      const string &t,
                      const string &n,
                      size_t i)
      : structName(sn)
      , type(t)
      , name(n)
      , idx(i) {
  }
};

struct PendingConstructor {
  string structName;
  string memberName;
  bool initMember;

  PendingConstructor(const string &sn,
                     const string &n,
                     bool im)
      : structName(sn)
      , memberName(n)
      , initMember(im) {
  }
};

std::string to_string(const avro::ValidSchema &vs) {
  std::stringstream ss;
  vs.toJson(ss);
  std::string s = ss.str();

  // TBD we should strip type : string to string 

  // strip whitespace
  s.erase(remove_if(s.begin(), s.end(), ::isspace), s.end());  // c version does not use locale... 
  return s;
}


static set<string> s_protected_words = {"explicit", "public"};

static string decorate_reserved_words(const string &name) {
  if (s_protected_words.find(name) != s_protected_words.end())
    return "_rsvd_" + name;
  return name;
}

static string namespace_cpp(const avro::Name &name){
  auto ns = name.ns();
  boost::replace_all(ns, ".", "::");
  return ns;
}

static std::vector<std::string> split_namespaces(const avro::Name &name){
  std::vector<std::string> words;
  std::string s = name.ns();
  if (s.size()>0){
    boost::split(words, s, boost::is_any_of("."), boost::token_compress_on);
  }
  return words;
}

static string fullname_cpp(const avro::Name &name) {
  auto ns = namespace_cpp(name);
  if (ns.size() > 0)
    return ns + "::" + name.simpleName();
  else
    return name;
}

static string simplename_cpp(const avro::Name &name) {
  return name.simpleName(); // guard about reserved words???
}

class CodeGen {
public:
  CodeGen(std::ostream &os,
          const std::string &schemaFile,
          const std::string &headerFile,
          const std::string &includePrefix,
          bool noUnion)
      : unionNumber_(0)
      , os_(os)
      , schemaFile_(schemaFile)
      , headerFile_(headerFile)
      , includePrefix_(includePrefix)
      , noUnion_(noUnion) {
  }

  void generate(const ValidSchema &schema);


  std::string generateEnumType(const NodePtr &n);

  std::string cppTypeOf(const NodePtr &n);

  std::string generateRecordType(const NodePtr &n);

  std::string unionName();

  std::string generateUnionType(const NodePtr &n);

  std::string generateType(const NodePtr &n);

  std::string generateDeclaration(const NodePtr &n);

  std::string doGenerateType(const NodePtr &n);

  void generateEnumTraits(const NodePtr &n);

  void generateTraits(const NodePtr &n);

  void generateRecordTraits(const NodePtr &n);

  void generateUnionTraits(const NodePtr &n);

  void generateExtensions(const ValidSchema &schema);

  void generateNsDecraration(const NodePtr &n);
  void generateNsEnd(const NodePtr &n);


private:
  size_t unionNumber_;
  std::ostream &os_;
  const std::string schemaFile_;
  const std::string headerFile_;
  const std::string includePrefix_;
  const bool noUnion_;
  std::string escaped_schema_string_;
  std::string root_name_;

  vector<PendingSetterGetter> pendingGettersAndSetters;
  vector<PendingConstructor> pendingConstructors;

  map<NodePtr, string> done;
  set<NodePtr> doing;
};

string CodeGen::generateEnumType(const NodePtr &n) {
  string s = simplename_cpp(n->name());
  os_ << "enum " << s << " {\n";
  size_t c = n->names();
  for (size_t i = 0; i < c; ++i) {
    os_ << "  " << decorate_reserved_words(n->nameAt(i)) << ",\n";
  }
  os_ << "};\n\n";
  return s;
}

string CodeGen::cppTypeOf(const NodePtr &n) {
  switch (n->type()) {
    case avro::AVRO_STRING:
      return "std::string";
    case avro::AVRO_BYTES:
      return "std::vector<uint8_t>";
    case avro::AVRO_INT:
      return "int32_t";
    case avro::AVRO_LONG:
      return "int64_t";
    case avro::AVRO_FLOAT:
      return "float";
    case avro::AVRO_DOUBLE:
      return "double";
    case avro::AVRO_BOOL:
      return "bool";
    case avro::AVRO_RECORD:
    case avro::AVRO_ENUM: {
      return fullname_cpp(n->name());
    }
    case avro::AVRO_ARRAY:
      return "std::vector<" + cppTypeOf(n->leafAt(0)) + " >";
    case avro::AVRO_MAP:
      return "std::map<std::string, " + cppTypeOf(n->leafAt(1)) + " >";
    case avro::AVRO_FIXED:
      return "boost::array<uint8_t, " +
             lexical_cast<string>(n->fixedSize()) + ">";
    case avro::AVRO_SYMBOLIC:
      return cppTypeOf(resolveSymbol(n));
    case avro::AVRO_UNION:
      return fullname_cpp(done[n]);
    default:
      return "$Undefined$";
  }
}

static string cppNameOf(const NodePtr &n) {
  switch (n->type()) {
    case avro::AVRO_NULL:
      return "null";
    case avro::AVRO_STRING:
      return "string";
    case avro::AVRO_BYTES:
      return "bytes";
    case avro::AVRO_INT:
      return "int";
    case avro::AVRO_LONG:
      return "long";
    case avro::AVRO_FLOAT:
      return "float";
    case avro::AVRO_DOUBLE:
      return "double";
    case avro::AVRO_BOOL:
      return "bool";
    case avro::AVRO_RECORD:
    case avro::AVRO_ENUM:
    case avro::AVRO_FIXED:
      return simplename_cpp(n->name());
    case avro::AVRO_ARRAY:
      return "array";
    case avro::AVRO_MAP:
      return "map";
    case avro::AVRO_SYMBOLIC:
      return cppNameOf(resolveSymbol(n));
    default:
      return "$Undefined$";
  }
}

void CodeGen::generateNsDecraration(const NodePtr &n) {
  auto v = split_namespaces(n->name());
  for (auto ns : v) {
    os_ << "namespace " << ns << " {\n";
  }
}

void CodeGen::generateNsEnd(const NodePtr &n){
  auto v = split_namespaces(n->name());
  for (auto ns : v) {
    os_ << "} // namespace " << ns << "\n";
  }
}

string CodeGen::generateRecordType(const NodePtr &n) {
  size_t c = n->leaves();
  vector<string> types;
  for (size_t i = 0; i < c; ++i) {
    types.push_back(generateType(n->leafAt(i)));
  }

  map<NodePtr, string>::const_iterator it = done.find(n);
  if (it != done.end()) {
    return it->second;
  }

  string decoratedName = simplename_cpp(n->name());
  os_ << "struct " << decoratedName << " {\n";
  if (!noUnion_) {
    for (size_t i = 0; i < c; ++i) {
      if (n->leafAt(i)->type() == avro::AVRO_UNION) {
        os_ << "  typedef " << types[i]
            << ' ' << decorate_reserved_words(n->nameAt(i)) << "_t;\n";
      }
    }
  }
  for (size_t i = 0; i < c; ++i) {
    if (!noUnion_ && n->leafAt(i)->type() == avro::AVRO_UNION) {
      os_ << "  " << decorate_reserved_words(n->nameAt(i)) << "_t";
    } else {
      os_ << "  " << types[i];
    }
    os_ << ' ' << n->nameAt(i) << ";\n";
  }

  os_ << "  " << decoratedName << "()";
  if (c > 0) {
    os_ << " :";
  }
  os_ << "\n";
  for (size_t i = 0; i < c; ++i) {
    os_ << "    " << decorate_reserved_words(n->nameAt(i)) << "(";
    if (!noUnion_ && n->leafAt(i)->type() == avro::AVRO_UNION) {
      os_ << decorate_reserved_words(n->nameAt(i)) << "_t";
    } else {
      os_ << types[i];
    }
    os_ << "())";
    if (i != (c - 1)) {
      os_ << ",\n";
    } else {
      os_ << "{\n";
    }
  }
  os_ << "  }\n\n";

  //extensions
  //should only be here for root level
  if (n->name().fullname() == root_name_)
  {
    os_ << "  //returns the string representation of the schema of self (avro extension for kspp avro serdes)\n";
    os_ << "  static inline const char* schema_as_string() {\n";
    os_ << "    return \"" << escaped_schema_string_ << "\";\n";
    os_ << "  } \n\n";
    os_ << "  //returns a valid schema of self (avro extension for kspp avro serdes)\n";
    os_ << "  static std::shared_ptr<const ::avro::ValidSchema> valid_schema() {\n";
    os_ << "    static const std::shared_ptr<const ::avro::ValidSchema> _validSchema(std::make_shared<const ::avro::ValidSchema>(::avro::compileJsonSchemaFromString(schema_as_string())));\n";
    os_ << "    return _validSchema;\n";
    os_ << "  }\n\n";
    os_ << "  //returns the (type)name of self (avro extension for kspp avro serdes)\n";
    os_ << "  static std::string name(){\n";
    os_ << "    return \"" << n->name().fullname() << "\";\n";
    os_ << "  }\n";
  }

  os_ << "};\n\n";

  return simplename_cpp(n->name());
}

void makeCanonical(string &s, bool foldCase) {
  for (string::iterator it = s.begin(); it != s.end(); ++it) {
    if (isalpha(*it)) {
      if (foldCase) {
        *it = toupper(*it);
      }
    } else if (!isdigit(*it)) {
      *it = '_';
    }
  }
}

string CodeGen::unionName() {
  string s = schemaFile_;
  string::size_type n = s.find_last_of("/\\");
  if (n != string::npos) {
    s = s.substr(n);
  }
  makeCanonical(s, false);

  return s + "_Union__" + boost::lexical_cast<string>(unionNumber_++) + "__";
}

static void generateGetterAndSetter(ostream &os,
                                    const string &structName,
                                    const string &type,
                                    const string &name,
                                    size_t idx) {
  string sn = " " + structName + "::";

  os << "inline\n";

  os << type << sn << "get_" << name << "() const {\n"
     << "  if (idx_ != " << idx << ") {\n"
     << "    throw avro::Exception(\"Invalid type for "
     << "union\");\n"
     << "  }\n"
     << "  return boost::any_cast<" << type << " >(value_);\n"
     << "}\n\n";

  os << "inline\n"
     << "void" << sn << "set_" << name
     << "(const " << type << "& v) {\n"
     << "  idx_ = " << idx << ";\n"
     << "  value_ = v;\n"
     << "}\n\n";
}

static void generateConstructor(ostream &os,
                                const string &structName,
                                bool initMember,
                                const string &type) {
  os << "inline " << structName << "::" << structName << "() : idx_(0)";
  if (initMember) {
    os << ", value_(" << type << "())";
  }
  os << " { }\n";
}

/**
 * Generates a type for union and emits the code.
 * Since unions can encounter names that are not fully defined yet,
 * such names must be declared and the inline functions deferred until all
 * types are fully defined.
 */
string CodeGen::generateUnionType(const NodePtr &n) {
  size_t c = n->leaves();
  vector<string> types;
  vector<string> names;

  set<NodePtr>::const_iterator it = doing.find(n);
  if (it != doing.end()) {
    for (size_t i = 0; i < c; ++i) {
      const NodePtr &nn = n->leafAt(i);
      types.push_back(generateDeclaration(nn));
      names.push_back(cppNameOf(nn));
    }
  } else {
    doing.insert(n);
    for (size_t i = 0; i < c; ++i) {
      const NodePtr &nn = n->leafAt(i);
      types.push_back(generateType(nn));
      names.push_back(cppNameOf(nn));
    }
    doing.erase(n);
  }
  if (done.find(n) != done.end()) {
    return done[n];
  }

  const string result = unionName();

  os_ << "struct " << result << " {\n"
      << "private:\n"
      << "  size_t idx_;\n"
      << "  boost::any value_;\n"
      << "public:\n"
      << "  size_t idx() const { return idx_; }\n";

  for (size_t i = 0; i < c; ++i) {
    const NodePtr &nn = n->leafAt(i);
    if (nn->type() == avro::AVRO_NULL) {
      os_ << "  bool is_null() const {\n"
          << "    return (idx_ == " << i << ");\n"
          << "  }\n"
          << "  void set_null() {\n"
          << "    idx_ = " << i << ";\n"
          << "    value_ = boost::any();\n"
          << "  }\n";
    } else {
      const string &type = types[i];
      const string &name = names[i];
      os_ << "  " << type << " get_" << name << "() const;\n"
              "  void set_" << name << "(const " << type << "& v);\n";
      pendingGettersAndSetters.push_back(
              PendingSetterGetter(result, type, name, i));
    }
  }

  os_ << "  " << result << "();\n";
  pendingConstructors.push_back(PendingConstructor(result, types[0],
                                                   n->leafAt(0)->type() != avro::AVRO_NULL));
  os_ << "};\n\n";

  return result;
}

/**
 * Returns the type for the given schema node and emits code to os.
 */
string CodeGen::generateType(const NodePtr &n) {
  NodePtr nn = (n->type() == avro::AVRO_SYMBOLIC) ? resolveSymbol(n) : n;

  map<NodePtr, string>::const_iterator it = done.find(nn);
  if (it != done.end()) {
    return it->second;
  }
  string result = doGenerateType(nn);
  done[nn] = result;
  return result;
}

string CodeGen::doGenerateType(const NodePtr &n) {
  switch (n->type()) {
    case avro::AVRO_STRING:
    case avro::AVRO_BYTES:
    case avro::AVRO_INT:
    case avro::AVRO_LONG:
    case avro::AVRO_FLOAT:
    case avro::AVRO_DOUBLE:
    case avro::AVRO_BOOL:
    case avro::AVRO_NULL:
    case avro::AVRO_FIXED:
      return cppTypeOf(n);
    case avro::AVRO_ARRAY:
      return "std::vector<" + generateType(n->leafAt(0)) + ">";
    case avro::AVRO_MAP:
      return "std::map<std::string, " + generateType(n->leafAt(1)) + ">";
    case avro::AVRO_RECORD:
      return generateRecordType(n);
    case avro::AVRO_ENUM:
      return generateEnumType(n);
    case avro::AVRO_UNION:
      return generateUnionType(n);
    default:
      break;
  }
  return "$Undefuned$";
}

string CodeGen::generateDeclaration(const NodePtr &n) {
  NodePtr nn = (n->type() == avro::AVRO_SYMBOLIC) ? resolveSymbol(n) : n;
  switch (nn->type()) {
    case avro::AVRO_STRING:
    case avro::AVRO_BYTES:
    case avro::AVRO_INT:
    case avro::AVRO_LONG:
    case avro::AVRO_FLOAT:
    case avro::AVRO_DOUBLE:
    case avro::AVRO_BOOL:
    case avro::AVRO_NULL:
    case avro::AVRO_FIXED:
      return cppTypeOf(nn);
    case avro::AVRO_ARRAY:
      return "std::vector<" + generateDeclaration(nn->leafAt(0)) + ">";
    case avro::AVRO_MAP:
      return "std::map<std::string, " +
             generateDeclaration(nn->leafAt(1)) + ">";
    case avro::AVRO_RECORD:
      os_ << "struct " << cppTypeOf(nn) << ";\n";
      return cppTypeOf(nn);
    case avro::AVRO_ENUM:
      return generateEnumType(nn);
    case avro::AVRO_UNION:
      // FIXME: When can this happen?
      return generateUnionType(nn);
    default:
      break;
  }
  return "$Undefined$";
}

void CodeGen::generateEnumTraits(const NodePtr &n) {
  //string dname = decorate(n->name());
  string fn = fullname_cpp(n->name());
  size_t c = n->names();
  string first = fullname_cpp(n->nameAt(0));
  string last = fullname_cpp(n->nameAt(c - 1));
  os_ << "template<> struct codec_traits<" << fn << "> {\n"
      << "  static void encode(Encoder& e, " << fn << " v) {\n"
      << "  if (v < " << first << " || v > " << last << ")\n"
      << "	{\n"
      << "    std::ostringstream error;\n"
      << "		error << \"enum value \" << v << \" is out of bound for " << fn << " and cannot be encoded\";\n"
      << "		throw avro::Exception(error.str());\n"
      << "  }\n"
      << "  e.encodeEnum(v);\n"
      << "  }\n"
      << "  static void decode(Decoder& d, " << fn << "& v) {\n"
      << "	size_t index = d.decodeEnum();\n"
      << "	if (index < " << first << " || index > " << last << ")\n"
      << "	{\n"
      << "	  std::ostringstream error;\n"
      << "		error << \"enum value \" << index << \" is out of bound for " << fn << " and cannot be decoded\";\n"
      << "		throw avro::Exception(error.str());\n"
      << "	}\n"
      << "  v = static_cast<" << fn << ">(index);\n"
      << "  }\n"
      << "};\n\n";
}

void CodeGen::generateRecordTraits(const NodePtr &n) {
  size_t c = n->leaves();
  for (size_t i = 0; i < c; ++i) {
    generateTraits(n->leafAt(i));
  }

  string fn = fullname_cpp(n->name());
  os_ << "template<> struct codec_traits<" << fn << "> {\n"
      << "  static void encode(Encoder& e, const " << fn << "& v) {\n";

  for (size_t i = 0; i < c; ++i) {
    os_ << "    avro::encode(e, v." << n->nameAt(i) << ");\n";
  }

  os_ << "  }\n"
      << "  static void decode(Decoder& d, " << fn << "& v) {\n";
  os_ << "    if (avro::ResolvingDecoder *rd =\n";
  os_ << "      dynamic_cast<avro::ResolvingDecoder *>(&d)) {\n";
  os_ << "        const std::vector<size_t> fo = rd->fieldOrder();\n";
  os_ << "        for (std::vector<size_t>::const_iterator it = fo.begin(); it != fo.end(); ++it) {\n";
  os_ << "          switch (*it) {\n";
  for (size_t i = 0; i < c; ++i) {
    os_ << "          case " << i << ":\n";
    os_ << "          avro::decode(d, v." << n->nameAt(i) << ");\n";
    os_ << "          break;\n";
  }
  os_ << "            default:\n";
  os_ << "            break;\n";
  os_ << "          }\n";
  os_ << "        }\n";
  os_ << "    } else {\n";

  for (size_t i = 0; i < c; ++i) {
    os_ << "      avro::decode(d, v." << n->nameAt(i) << ");\n";
  }
  os_ << "    }\n";

  os_ << "  }\n"
      << "};\n\n";
}

void CodeGen::generateUnionTraits(const NodePtr &n) {
  size_t c = n->leaves();

  for (size_t i = 0; i < c; ++i) {
    const NodePtr &nn = n->leafAt(i);
    generateTraits(nn);
  }

  string name = done[n];
  string fn = fullname_cpp(name);

  os_ << "template<> struct codec_traits<" << fn << "> {\n"
      << "  static void encode(Encoder& e, " << fn << " v) {\n"
      << "    e.encodeUnionIndex(v.idx());\n"
      << "    switch (v.idx()) {\n";

  for (size_t i = 0; i < c; ++i) {
    const NodePtr &nn = n->leafAt(i);
    os_ << "    case " << i << ":\n";
    if (nn->type() == avro::AVRO_NULL) {
      os_ << "      e.encodeNull();\n";
    } else {
      os_ << "      avro::encode(e, v.get_" << cppNameOf(nn)
          << "());\n";
    }
    os_ << "     break;\n";
  }

  os_ << "    }\n"
      << "  }\n"
      << "  static void decode(Decoder& d, " << fn << "& v) {\n"
      << "    size_t n = d.decodeUnionIndex();\n"
      << "    if (n >= " << c << ") { throw avro::Exception(\""
              "Union index too big\"); }\n"
      << "    switch (n) {\n";

  for (size_t i = 0; i < c; ++i) {
    const NodePtr &nn = n->leafAt(i);
    os_ << "    case " << i << ":\n";
    if (nn->type() == avro::AVRO_NULL) {
      os_ << "      d.decodeNull();\n"
          << "      v.set_null();\n";
    } else {
      os_ << "      {\n"
          << "          " << cppTypeOf(nn) << " vv;\n"
          << "          avro::decode(d, vv);\n"
          << "          v.set_" << cppNameOf(nn) << "(vv);\n"
          << "      }\n";
    }
    os_ << "      break;\n";
  }
  os_ << "    }\n"
      << "  }\n"
      << "};\n\n";
}

void CodeGen::generateTraits(const NodePtr &n) {
  switch (n->type()) {
    case avro::AVRO_STRING:
    case avro::AVRO_BYTES:
    case avro::AVRO_INT:
    case avro::AVRO_LONG:
    case avro::AVRO_FLOAT:
    case avro::AVRO_DOUBLE:
    case avro::AVRO_BOOL:
    case avro::AVRO_NULL:
      break;
    case avro::AVRO_RECORD:
      generateRecordTraits(n);
      break;
    case avro::AVRO_ENUM:
      generateEnumTraits(n);
      break;
    case avro::AVRO_ARRAY:
    case avro::AVRO_MAP:
      generateTraits(n->leafAt(n->type() == avro::AVRO_ARRAY ? 0 : 1));
      break;
    case avro::AVRO_UNION:
      generateUnionTraits(n);
      break;
    case avro::AVRO_FIXED:
      break;
    default:
      break;
  }
}

template<class OutIter>
static OutIter escape_string(std::string const &s, OutIter out) {
  //*out++ = '"';
  for (std::string::const_iterator i = s.begin(), end = s.end(); i != end; ++i) {
    unsigned char c = *i;
    if (' ' <= c && c <= '~' && c != '\\' && c != '"') {
      *out++ = c;
    } else {
      *out++ = '\\';
      switch (c) {
        case '"':
          *out++ = '"';
          break;
        case '\\':
          *out++ = '\\';
          break;
        case '\t':
          *out++ = 't';
          break;
        case '\r':
          *out++ = 'r';
          break;
        case '\n':
          *out++ = 'n';
          break;
        default:
          char const *const hexdig = "0123456789ABCDEF";
          *out++ = 'x';
          *out++ = hexdig[c >> 4];
          *out++ = hexdig[c & 0xF];
      }
    }
  }
  //*out++ = '"';
  return out;
}

void CodeGen::generateExtensions(const ValidSchema &schema) {
  const NodePtr &root = schema.root();

  //hash_ = generate_hash(schema);

  std::string str = to_string(schema);
  escape_string(str, std::back_inserter(escaped_schema_string_));

  root_name_ = root->name().fullname(); // to only emit has etc once... might exist a better way of doing this...
}

void CodeGen::generate(const ValidSchema &schema) {
  generateExtensions(schema);
  os_ << "#include <sstream>\n"
      << "#include \"boost/any.hpp\"\n"
      << "#include \"" << includePrefix_ << "Specific.hh\"\n"
      << "#include \"" << includePrefix_ << "Encoder.hh\"\n"
      << "#include \"" << includePrefix_ << "Decoder.hh\"\n"
      << "#include \"" << includePrefix_ << "Compiler.hh\"\n"
      << "#pragma once\n"
      << "\n";

  const NodePtr &root = schema.root();

  generateNsDecraration(root);

  generateType(root);

  for (vector<PendingSetterGetter>::const_iterator it =
          pendingGettersAndSetters.begin();
       it != pendingGettersAndSetters.end(); ++it) {
    generateGetterAndSetter(os_, it->structName, it->type, it->name,
                            it->idx);
  }

  for (vector<PendingConstructor>::const_iterator it =
          pendingConstructors.begin();
       it != pendingConstructors.end(); ++it) {
    generateConstructor(os_, it->structName,
                        it->initMember, it->memberName);
  }

  generateNsEnd(root);

  os_ << "\n";
  os_ << "namespace avro {\n";

  unionNumber_ = 0;

  generateTraits(root);

  os_ << "}\n";
  os_.flush();
}

namespace po = boost::program_options;

//static const string NS("namespace");
static const string OUT("output");
static const string IN("input");
static const string INCLUDE_PREFIX("include-prefix");
static const string NO_UNION_TYPEDEF("no-union-typedef");

static string readGuard(const string &filename) {
  std::ifstream ifs(filename.c_str());
  string buf;
  string candidate;
  while (std::getline(ifs, buf)) {
    boost::algorithm::trim(buf);
    if (candidate.empty()) {
      if (boost::algorithm::starts_with(buf, "#ifndef ")) {
        candidate = buf.substr(8);
      }
    } else if (boost::algorithm::starts_with(buf, "#define ")) {
      if (candidate == buf.substr(8)) {
        break;
      }
    } else {
      candidate.erase();
    }
  }
  return candidate;
}

int main(int argc, char **argv) {
  po::options_description desc("Allowed options");
  desc.add_options()
          ("help,h", "produce help message")
          ("include-prefix,p", po::value<string>()->default_value("avro"),
           "prefix for include headers, - for none, default: avro")
          ("no-union-typedef,U", "do not generate typedefs for unions in records")
          ("input,i", po::value<string>(), "input file")
          ("output,o", po::value<string>(), "output file to generate");

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);


  if (vm.count("help") || vm.count(IN) == 0) {
    std::cout << desc << std::endl;
    return 1;
  }
  string outf = vm.count(OUT) > 0 ? vm[OUT].as<string>() : string();
  string inf = vm.count(IN) > 0 ? vm[IN].as<string>() : string();
  string incPrefix = vm[INCLUDE_PREFIX].as<string>();
  bool noUnion = vm.count(NO_UNION_TYPEDEF) != 0;
  if (incPrefix == "-") {
    incPrefix.clear();
  } else if (*incPrefix.rbegin() != '/') {
    incPrefix += "/";
  }

  try {
    ValidSchema schema;

    if (!inf.empty()) {
      ifstream in(inf.c_str());
      compileJsonSchema(in, schema);
    } else {
      compileJsonSchema(std::cin, schema);
    }

    if (!outf.empty()) {
      //create out file directory
      boost::filesystem::path p(outf);
      boost::filesystem::path dir = p.parent_path();
      if (!dir.empty()) {
        boost::filesystem::create_directories(dir);
        if (!boost::filesystem::is_directory(dir)) {
          std::cerr << "Failed to create directories: " << dir << std::endl;
        }
      }
      ofstream out(outf.c_str());
      CodeGen(out, inf, outf, incPrefix, noUnion).generate(schema);
    } else {
      CodeGen(std::cout, inf, outf,  incPrefix, noUnion).generate(schema);
    }
    return 0;
  } catch (std::exception &e) {
    std::cerr << "Failed to parse or compile schema: " << e.what() << std::endl;
    return 1;
  }

}
