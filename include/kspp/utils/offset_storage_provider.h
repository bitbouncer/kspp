#include <string>
#include <boost/filesystem.hpp>
#pragma once

namespace kspp {
  class offset_storage {
  public:
    offset_storage(size_t max_pending=10000):
        _max_pending(max_pending){
    };

    virtual ~offset_storage() {}

    virtual int64_t start(int64_t offset)=0;

    virtual void commit(int64_t offset, bool flush) {
      _last_commited_offset = offset;
      if (flush || (_last_commited_offset - _last_flushed_offset) > _max_pending){
        persist_offset(_last_commited_offset);
      }
    }

  protected:
    virtual void persist_offset(int64_t offset) = 0;

    int64_t _last_commited_offset=0;
    int64_t _last_flushed_offset=0;
    size_t _max_pending=10000;
  };


  class fs_offset_storage : public offset_storage {
  public:
    fs_offset_storage(std::string path, size_t max_pendning=10000);
    ~fs_offset_storage() override;
    int64_t start(int64_t offset) override;
  private:
    void persist_offset(int64_t offset) override;
    boost::filesystem::path _offset_storage_path;
  };

}







