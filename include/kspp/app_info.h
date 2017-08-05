#include <string>
#pragma once

namespace kspp {
  struct app_info {
    /**
    * multi instance apps - state stores will be prefixed with instance_id
    * metrics will have instance_id and app_instance_name tags
    */
    app_info(std::string _app_namespace,
             std::string _app_id,
             std::string _app_instance_id,
             std::string _app_instance_name)
            : app_namespace(_app_namespace), app_id(_app_id), app_instance_id(_app_instance_id),
              app_instance_name(_app_instance_name) {}

    /**
    * single instance apps - state stores will not be prefixed with instance_id
    * metrics will not have instance_id or app_instance_name tag ??
    * maybe they should be marked as single instance?
    */
    app_info(std::string _app_namespace,
             std::string _app_id)
            : app_namespace(_app_namespace), app_id(_app_id) {}

    std::string identity() const {
      if (app_instance_id.size() == 0)
        return app_namespace + "::" + app_id;
      else
        return app_namespace + "::" + app_id + "#" + app_instance_id;
    }

    std::string group_id() const {
      if (app_instance_id.size() == 0)
        return app_namespace + "_" + app_id;
      else
        return app_namespace + "_" + app_id + "_" + app_instance_id;
    }

    const std::string app_namespace;
    const std::string app_id;
    const std::string app_instance_id;
    const std::string app_instance_name;
  };

  inline std::string to_string(const app_info &obj) {
    return obj.identity();
  }
};