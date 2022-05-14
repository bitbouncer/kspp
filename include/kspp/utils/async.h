#include <vector>
#include <chrono>
#include <functional>
#include <cstdint>
#include <future>
#include <cassert>

#pragma once

namespace kspp {
  namespace async {

    enum end_condition_t {
      FIRST_FAIL, FIRST_SUCCESS, ALL
    }; // could this be a user defined function instead??
    enum scheduling_t {
      PARALLEL, SEQUENTIAL
    }; // node async calls this parallel & waterfall

    template<typename result_type>
    class work {
    public:
      using callback = std::function<void(result_type)>;
      using async_function = std::function<void(callback)>;
      using async_vcallback1 = std::function<void(int64_t duration_ms, result_type ec)>;

      static_assert(!std::is_reference<result_type>::value, "Error: lvalue was passed!");

    private:
      class result {
      public:
        result(size_t nr_of_tasks, end_condition_t end, async_vcallback1 callback) :
            cb_(callback),
            end_condition_(end),
            result_reported_(false),
            result_(nr_of_tasks, result_type()),
            done_(nr_of_tasks, false),
            nr_done_(0),
            start_ts_(std::chrono::steady_clock::now()) {}

        ~result() {}

        inline size_t size() const { return result_.size(); }

        bool set_result(size_t index, result_type ec) {
          result_[index] = ec;
          done_[index] = true;
          nr_done_++;

          switch (end_condition_) {
            case FIRST_FAIL:
              if (!result_reported_) {
                if (ec) {
                  std::chrono::milliseconds duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - start_ts_);
                  // logging?
                  cb_(duration.count(), ec);
                  result_reported_ = true;
                } else {
                  if (nr_done_ == result_.size()) {
                    std::chrono::milliseconds duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - start_ts_);
                    // logging?
                    cb_(duration.count(), ec);
                    result_reported_ = true;
                  }
                }
              }
              break;
            case FIRST_SUCCESS:
              if (!result_reported_) {
                if (!ec) {
                  std::chrono::milliseconds duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - start_ts_);
                  // logging?
                  cb_(duration.count(), ec);
                  result_reported_ = true;
                } else {
                  if (nr_done_ == result_.size()) {
                    std::chrono::milliseconds duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - start_ts_);
                    // logging?
                    cb_(duration.count(), ec);
                    result_reported_ = true;
                  }
                }
                break;
                case ALL:
                  if (nr_done_ == result_.size()) {
                    std::chrono::milliseconds duration = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - start_ts_);
                    result_type res_ec = result_type();
                    for (auto const &item_ec: result_) {
                      if (item_ec) // set res_ec to last ec that is non zero
                        res_ec = item_ec;
                    }
                    // logging?
                    cb_(duration.count(), res_ec);
                    result_reported_ = true;
                  }
                break;
                default:
                  assert(false);
                break;
              }
          } // switch
          return (!result_reported_);
        }

        result_type operator[](size_t index) const { return result_[index]; }

        result_type ec(size_t index) const { return result_[index]; }

        bool done(size_t index) const { return done_[index]; }

        bool reported() const { return result_reported_; }

      private:
        async_vcallback1 cb_;
        end_condition_t end_condition_;
        bool result_reported_;
        std::vector<result_type> result_;
        std::vector<bool> done_;
        size_t nr_done_;
        std::chrono::steady_clock::time_point start_ts_;
      };

    public:
      work(scheduling_t s = PARALLEL, end_condition_t ec = FIRST_FAIL) :
          scheduling_(s),
          end_condition_(ec) {}

      work(std::vector<async_function> f, scheduling_t s = PARALLEL, end_condition_t ec = FIRST_FAIL) :
          scheduling_(s),
          end_condition_(ec),
          work_(f) {}

      void push_back(async_function f) {
        work_.push_back(f);
      }

      async_function get_function(size_t index) {
        return work_[index];
      }

      result_type get_result(size_t index) {
        return (*result_)[index];
      }

      result_type operator()() {
        std::promise<result_type> p;
        std::future<result_type> f = p.get_future();
        async_call([&p](int64_t duration_ms, result_type ec) {
          p.set_value(ec);
        });
        f.wait();
        return f.get();
      }

      void operator()(callback cb) {
        async_call([cb](int64_t duration_ms, result_type ec) {
          // add logging?
          cb(ec);
        });
      }

      template<typename RAIter>
      static void async_parallel(RAIter begin, RAIter end, std::shared_ptr<result> result) {
        size_t index = 0;
        for (RAIter i = begin; i != end; ++i, ++index) {
          (*i)([i, result, index](result_type ec) {
            (*result).set_result(index, ec);
          });
        }
      }

      template<typename RAIter>
      static void async_sequential(RAIter begin, RAIter end, std::shared_ptr<result> result) {
        if (begin == end) {
          return;
        }
        (*begin)([result, begin, end](result_type ec) {
          size_t index = result->size() - (end - begin);
          result->set_result(index, ec);
          if (!result->reported())
            async_sequential(begin + 1, end, result);
        });
      }

      void async_call(async_vcallback1 cb) {
        result_ = std::make_shared<result>(work_.size(), end_condition_, [cb](int64_t duration_ms, result_type ec) {
          cb(duration_ms, ec);
        });
        if (scheduling_ == PARALLEL)
          async_parallel(work_.begin(), work_.end(), result_);
        else
          async_sequential(work_.begin(), work_.end(), result_);
      };

      void async_call(callback cb) {
        async_call([cb](int64_t duration_ms, result_type ec) {
          // add logging?
          cb(ec);
        });
      };

      inline result_type call() {
        std::promise<result_type> p;
        std::future<result_type> f = p.get_future();
        async_call([&p](int64_t duration_ms, result_type ec) {
          p.set_value(ec);
        });
        f.wait();
        return f.get();
      }

    private:
      std::vector<async_function> work_;
      std::shared_ptr<result> result_;
      scheduling_t scheduling_;
      end_condition_t end_condition_;
    };
  }
}
