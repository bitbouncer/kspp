#include <kspp/features/aws/aws.h>
#include <aws/core/Aws.h>

namespace kspp {
  void init_aws() {
    bool is_init = false;
    if (!is_init) {
      is_init = true;
      Aws::SDKOptions options;
      options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Info;
      Aws::InitAPI(options);
    }
  }
}

