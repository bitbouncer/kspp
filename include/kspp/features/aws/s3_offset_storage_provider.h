#include <kspp/utils/offset_storage_provider.h>
#include <kspp/utils/url.h>
#include <aws/s3/S3Client.h>

namespace kspp {
  class s3_offset_storage : public offset_storage {
  public:
    static std::shared_ptr<s3_offset_storage> create(kspp::url);

    s3_offset_storage(std::string host, std::string s3_bucket, std::string key, std::string access_key, std::string secret_key);

    ~s3_offset_storage() override;

  private:
    int64_t load_offset(int timeout_ms) override;

    void persist_offset(int64_t offset, int timeout_ms) override;

    const std::string s3_bucket_;
    const std::string s3_object_name_;
    std::shared_ptr<Aws::S3::S3Client> s3_client_;
  };
}