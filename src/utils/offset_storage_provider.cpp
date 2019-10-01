#include <kspp/utils/offset_storage_provider.h>
#include <fstream>
#include <glog/logging.h>
#include <kspp/kspp.h>
#include <kspp/utils/url.h>

#ifdef KSPP_S3
#include <kspp/features/aws/s3_offset_storage_provider.h>
#endif

namespace kspp {

  //TODO should we set commited offset and flushed offset to this??? probably?!
  int64_t offset_storage::start(int64_t offset){
    if (offset == kspp::OFFSET_STORED) {
      return load_offset(flush_offset_timeout_ms_);
    } else if (offset == kspp::OFFSET_BEGINNING) {
      LOG(INFO) << "starting from OFFSET_BEGINNING";
      return kspp::OFFSET_BEGINNING;
    } else if (offset == kspp::OFFSET_END) {
      LOG(INFO) << "starting from OFFSET_END";
      return kspp::OFFSET_END;
    }
    LOG(INFO) << "starting from fixed offset: " << offset << ", overriding storage";
    return offset;
  }

  fs_offset_storage::fs_offset_storage(std::string path)
      : offset_storage()
      , offset_storage_path_(path){
    if (!offset_storage_path_.empty()){
      std::experimental::filesystem::create_directories(offset_storage_path_.parent_path());
    }
  }

  fs_offset_storage::~fs_offset_storage(){
    persist_offset(last_commited_offset_, 1000);
  }

  int64_t fs_offset_storage::load_offset(int timeout_ms_not_used){
    if (!std::experimental::filesystem::exists(offset_storage_path_)){
      LOG(INFO) << "start(OFFSET_STORED), missing file " << offset_storage_path_ << ", starting from OFFSET_BEGINNING";
      return kspp::OFFSET_BEGINNING;
    }

    std::ifstream is(offset_storage_path_.generic_string(), std::ios::binary);
    int64_t tmp;
    is.read((char *) &tmp, sizeof(int64_t));
    if (is.good()) {
      LOG(INFO) << "start(OFFSET_STORED) - > offset:" << tmp;
      return tmp;
    }

    LOG(INFO) << "start(OFFSET_STORED), bad file " << offset_storage_path_ << ", starting from OFFSET_BEGINNING";
    return kspp::OFFSET_BEGINNING;
  }

  void fs_offset_storage::persist_offset(int64_t offset, int timeout_ms_not_used) {
    if (last_flushed_offset_ != last_commited_offset_) {
      std::ofstream os(offset_storage_path_.generic_string(), std::ios::binary);
      os.write((char *) &last_commited_offset_, sizeof(int64_t));
      last_flushed_offset_ = last_commited_offset_;
      os.flush();
    }
  }

  std::shared_ptr<offset_storage> get_offset_provider(std::string uri) {
    if (uri.size()==0)
      return std::make_shared<null_offset_storage>();

    kspp::url u(uri, "file");

    if (!u.good()){
      LOG(ERROR) << "bad uri: " << uri;
      return nullptr;
    }

    if (u.scheme()=="null"){
      return std::make_shared<null_offset_storage>();
    }

    if (u.scheme()=="s3"){
#ifdef KSPP_S3
      return s3_offset_storage::create(uri);
#else
      LOG(ERROR) << "feature S3 not enabled";
      return nullptr
#endif
    }

    if (u.scheme()=="file"){
      return std::make_shared<fs_offset_storage>(uri);
    }

    LOG(ERROR) << "unknown scheme: " << u.scheme() << " in uri:" << uri;
    return nullptr;
  }

}
