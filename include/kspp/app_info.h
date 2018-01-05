#include <string>
#pragma once

namespace kspp {
  struct app_info {
    /**
    * multi tenant apps - state stores will be prefixed with tenant_id
    * metrics will have tennant_id and app_id tags
    */
    app_info(std::string my_namespace,
             std::string my_app_id,
             std::string tenant_id)
            : app_namespace(my_namespace)
        , app_id(my_app_id)
        , app_tenant_id(tenant_id){
    }

    /**
    * single instance apps - state stores will not be prefixed with instance_id
    * metrics will not have instance_id or app_instance_name tag ??
    * maybe they should be marked as single instance?
    */
    app_info(std::string my_namespace,
             std::string my_app_id)
            : app_namespace(my_namespace), app_id(my_app_id) {
    }

    std::string identity() const {
      if (app_tenant_id.size() == 0)
        return app_namespace + "::" + app_id;
      else
        return app_namespace + "::" + app_id + "#" + app_tenant_id;
    }

    std::string consumer_group() const {
      if (app_tenant_id.size() == 0)
        return app_namespace + "_" + app_id;
      else
        return app_namespace + "_" + app_id + "_" + app_tenant_id;
    }

    const std::string app_namespace;
    const std::string app_id;
    const std::string app_tenant_id;
  };

  inline std::string to_string(const app_info &obj) {
    return obj.identity();
  }
}