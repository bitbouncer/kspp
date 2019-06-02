#include <string>
#include <boost/filesystem.hpp>
#include <libs3.h>

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
    virtual int64_t load_offset()=0;
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
    int64_t load_offset() override;
    void persist_offset(int64_t offset) override;
    boost::filesystem::path _offset_storage_path;
  };


  class s3_offset_storage : public offset_storage {
  public:
    s3_offset_storage(std::string host, std::string bucket, std::string key, std::string _access_key, std::string _secret_key);
    ~s3_offset_storage() override;
    int64_t start(int64_t offset) override;
  private:
    int64_t load_offset() override;
    void persist_offset(int64_t offset) override;
    const std::string _host;
    const std::string _bucket;
    const std::string _access_key;
    const std::string _secret_key;
    const std::string _key;
    S3BucketContext _bucketContext;
  };

  std::shared_ptr<offset_storage> get_offset_provider(std::string uri);

}







