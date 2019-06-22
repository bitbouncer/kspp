#include <climits>
#include <string>
#include <experimental/filesystem>
#include <libs3.h>

#pragma once

namespace kspp {
  class offset_storage {
  public:
    offset_storage(){
    };

    virtual ~offset_storage() {}

    int64_t start(int64_t offset);

    virtual void commit(int64_t offset, bool flush) {
      _last_commited_offset = offset;
      if (flush || (_last_commited_offset - _last_flushed_offset) > _max_pending_offsets){
        if (_last_commited_offset > _last_flushed_offset)
          persist_offset(_last_commited_offset, _flush_offset_timeout_ms);
      }
    }

    void set_max_pending_offsets(int64_t max_pending){
      _max_pending_offsets = max_pending;
    }

    void set_flush_offset_timeout(int flush_offset_timeout_ms){
      _flush_offset_timeout_ms = flush_offset_timeout_ms;
    }

  protected:
    virtual int64_t load_offset(int timeout_ms)=0;
    virtual void persist_offset(int64_t offset, int timeout_ms) = 0;

    int64_t _last_commited_offset=0;
    int64_t _last_flushed_offset=0;
    size_t _max_pending_offsets=3600000;
    int _flush_offset_timeout_ms=1000;
  };

  class fs_offset_storage : public offset_storage {
  public:
    fs_offset_storage(std::string path);
    ~fs_offset_storage() override;
  private:
    int64_t load_offset(int timeout_ms) override;
    void persist_offset(int64_t offset, int timeout_ms) override;
    std::experimental::filesystem::path _offset_storage_path;
  };


  class s3_offset_storage : public offset_storage {
  public:
    s3_offset_storage(std::string host, std::string bucket, std::string key, std::string _access_key, std::string _secret_key, bool use_ssl);
    ~s3_offset_storage() override;
  private:
    int64_t load_offset(int timeout_ms) override;
    void persist_offset(int64_t offset, int timeout_ms) override;
    const std::string _host;
    const std::string _bucket;
    const std::string _access_key;
    const std::string _secret_key;
    const std::string _key;
    S3BucketContext _bucketContext;
  };

  std::shared_ptr<offset_storage> get_offset_provider(std::string uri);

}







