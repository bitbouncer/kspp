#include <climits>
#include <string>
#include <experimental/filesystem>
#include <kspp/typedefs.h>

#pragma once

namespace kspp {
  class offset_storage {
  public:
    enum { UNKNOWN_OFFSET = -4242 };

    offset_storage(){};

    virtual ~offset_storage() {}

    int64_t start(int64_t offset);

    virtual void commit(int64_t offset, bool flush) {
      last_commited_offset_ = offset;
      if (flush || (last_commited_offset_ - last_flushed_offset_) > max_pending_offsets_){
        if (last_commited_offset_ > last_flushed_offset_)
          persist_offset(last_commited_offset_, flush_offset_timeout_ms_);
      }
    }

    void set_max_pending_offsets(int64_t max_pending){
      max_pending_offsets_ = max_pending;
    }

    void set_flush_offset_timeout(int flush_offset_timeout_ms){
      flush_offset_timeout_ms_ = flush_offset_timeout_ms;
    }

  protected:
    virtual int64_t load_offset(int timeout_ms)=0;
    virtual void persist_offset(int64_t offset, int timeout_ms) = 0;

    int64_t last_commited_offset_=UNKNOWN_OFFSET;
    int64_t last_flushed_offset_=UNKNOWN_OFFSET-1;
    size_t max_pending_offsets_=3600000;
    int flush_offset_timeout_ms_=1000;
  };

  class fs_offset_storage : public offset_storage {
  public:
    fs_offset_storage(std::string path);
    ~fs_offset_storage() override;
  private:
    int64_t load_offset(int timeout_ms) override;
    void persist_offset(int64_t offset, int timeout_ms) override;
    std::experimental::filesystem::path offset_storage_path_;
  };

  class null_offset_storage : public offset_storage {
  public:
    null_offset_storage() {};
    ~null_offset_storage() override {}
  private:
    int64_t load_offset(int timeout_ms) override { return OFFSET_END; }
    void persist_offset(int64_t offset, int timeout_ms) override { } // noop
  };


  /*
   * uri is one of [file: s3: null:]
   */
  std::shared_ptr<offset_storage> get_offset_provider(std::string uri);
}







