#include <chrono>
#include <utility>
/*
 * https://stackoverflow.com/questions/2808398/easily-measure-elapsed-time
 * Nikos Athanasiou
 */
#pragma once
namespace kspp {
  template<typename TimeT = std::chrono::milliseconds>
  struct measure {
    template<typename F, typename ...Args>
    static typename TimeT::rep execution(F &&func, Args &&... args) {
      auto start = std::chrono::steady_clock::now();
      std::forward<decltype(func)>(func)(std::forward<Args>(args)...);
      auto duration = std::chrono::duration_cast<TimeT>
          (std::chrono::steady_clock::now() - start);
      return duration.count();
    }

    template<typename F, typename ...Args>
    static auto duration(F &&func, Args &&... args) {
      auto start = std::chrono::steady_clock::now();
      std::forward<decltype(func)>(func)(std::forward<Args>(args)...);
      return std::chrono::duration_cast<TimeT>(std::chrono::steady_clock::now() - start);
    }
  };
}

// usage:
//    std::cout << measure<>::execution(functor(dummy)) << std::endl;
//
// call .count() manually later when needed (eg IO)
// auto avg = (measure<>::duration(func) + measure<>::duration(func)) / 2.0;
