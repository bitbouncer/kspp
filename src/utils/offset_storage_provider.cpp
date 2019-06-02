#include <kspp/utils/offset_storage_provider.h>
#include <kspp/kspp.h>

namespace kspp {
  fs_offset_storage::fs_offset_storage(std::string path, size_t max_pendning)
      : offset_storage(max_pendning)
      , _offset_storage_path(path){
    if (!_offset_storage_path.empty()){
      boost::filesystem::create_directories(boost::filesystem::path(_offset_storage_path).parent_path());
    }
  }

  fs_offset_storage::~fs_offset_storage(){
    persist_offset(_last_commited_offset);
  }

  int64_t fs_offset_storage::start(int64_t offset){
    if (offset == kspp::OFFSET_STORED) {
      if (boost::filesystem::exists(_offset_storage_path)) {
        std::ifstream is(_offset_storage_path.generic_string(), std::ios::binary);
        int64_t tmp;
        is.read((char *) &tmp, sizeof(int64_t));
        if (is.good()) {
          LOG(INFO) << "start(OFFSET_STORED) - > offset:" << tmp;
          return tmp;
        } else {
          LOG(INFO) << "start(OFFSET_STORED), bad file " << _offset_storage_path << ", starting from OFFSET_BEGINNING";
          return kspp::OFFSET_BEGINNING;
        }
      } else {
        LOG(INFO) << "start(OFFSET_STORED), missing file " << _offset_storage_path << ", starting from OFFSET_BEGINNING";
        return kspp::OFFSET_BEGINNING;
      }
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


  void fs_offset_storage::persist_offset(int64_t offset) {
    if (_last_flushed_offset != _last_commited_offset) {
      std::ofstream os(_offset_storage_path.generic_string(), std::ios::binary);
      os.write((char *) &_last_commited_offset, sizeof(int64_t));
      _last_flushed_offset = _last_commited_offset;
      os.flush();
    }
  }
}
