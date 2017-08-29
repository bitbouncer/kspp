#include <vector>
#include <chrono>
#include <functional>
#include <cstdint>
#include <future>
#include <cassert>
#pragma once

namespace kspp {
  namespace async {

    enum end_condition_t { FIRST_FAIL, FIRST_SUCCESS, ALL }; // could this be a user defined function instead??
    enum scheduling_t { PARALLEL, SEQUENTIAL }; // node async calls this parallel & waterfall

    template <typename result_type>
    class work
    {
    public:
      using callback = std::function <void(result_type)>;
      using async_function = std::function <void(callback)>;
      using async_vcallback1 = std::function <void(int64_t duration_ms, result_type ec)>;

      static_assert(!std::is_reference<result_type>::value, "Error: lvalue was passed!");

    private:
      class result
      {
      public:
        result(size_t nr_of_tasks, end_condition_t end, async_vcallback1 callback) :
          _cb(callback),
          _result_reported(false),
          _end_condition(end),
          _result(nr_of_tasks, result_type()),
          _done(nr_of_tasks, false),
          _nr_done(0),
          _start_ts(std::chrono::steady_clock::now()) {}
        ~result() {}

        inline size_t size() const { return _result.size(); }

        bool set_result(size_t index, result_type ec) {
          _result[index] = ec;
          _done[index] = true;
          _nr_done++;

          switch (_end_condition)
          {
          case FIRST_FAIL:
            if (!_result_reported)
            {
              if (ec)
              {
                std::chrono::milliseconds duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - _start_ts);
                // logging?
                _cb(duration.count(), ec);
                _result_reported = true;
              } else
              {
                if (_nr_done == _result.size())
                {
                  std::chrono::milliseconds duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - _start_ts);
                  // logging?
                  _cb(duration.count(), ec);
                  _result_reported = true;
                }
              }
            }
            break;
          case FIRST_SUCCESS:
            if (!_result_reported)
            {
              if (!ec)
              {
                std::chrono::milliseconds duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - _start_ts);
                // logging?
                _cb(duration.count(), ec);
                _result_reported = true;
              } else
              {
                if (_nr_done == _result.size())
                {
                  std::chrono::milliseconds duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - _start_ts);
                  // logging?
                  _cb(duration.count(), ec);
                  _result_reported = true;
                }
              }
              break;
          case ALL:
            if (_nr_done == _result.size())
            {
              std::chrono::milliseconds duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - _start_ts);
              result_type res_ec = result_type();
              for (auto const& item_ec : _result)
              {
                if (item_ec) // set res_ec to last ec that is non zero
                  res_ec = item_ec;
              }
              // logging?
              _cb(duration.count(), res_ec);
              _result_reported = true;
            }
            break;
          default:
            assert(false);
            break;
            }
          } // switch
          return (!_result_reported);
        }

        result_type operator[](size_t index) const { return _result[index]; }

        result_type ec(size_t index) const { return _result[index]; }
        bool        done(size_t index) const { return _done[index]; }
        bool        reported() const { return _result_reported; }

      private:
        async_vcallback1 _cb;
        end_condition_t _end_condition;
        bool _result_reported;
        std::vector<result_type> _result;
        std::vector<bool> _done;
        size_t _nr_done;
        std::chrono::steady_clock::time_point _start_ts;
      };

    public:
      work(scheduling_t s = PARALLEL, end_condition_t ec = FIRST_FAIL) :
        _scheduling(s),
        _end_condition(ec) {}

      work(std::vector<async_function> f, scheduling_t s = PARALLEL, end_condition_t ec = FIRST_FAIL) :
        _scheduling(s),
        _end_condition(ec),
        _work(f) {}

      void push_back(async_function f) {
        _work.push_back(f); 
      }

      async_function get_function(size_t index) { 
        return _work[index]; 
      }

      result_type get_result(size_t index) {
        return (*_result)[index];
      }

      result_type operator()() {
        std::promise<result_type> p;
        std::future<result_type>  f = p.get_future();
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
        for (RAIter i = begin; i != end; ++i, ++index)
        {
          (*i)([i, result, index](result_type ec) {
            (*result).set_result(index, ec);
          });
        }
      }

      template<typename RAIter>
      static void async_sequential(RAIter begin, RAIter end, std::shared_ptr<result> result) {
        if (begin == end)
        {
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
        _result = std::make_shared<result>(_work.size(), _end_condition, [cb](int64_t duration_ms, result_type ec) {
          cb(duration_ms, ec);
        });
        if (_scheduling == PARALLEL)
          async_parallel(_work.begin(), _work.end(), _result);
        else
          async_sequential(_work.begin(), _work.end(), _result);
      };

      void async_call(callback cb) {
        async_call([cb](int64_t duration_ms, result_type ec) {
          // add logging?
          cb(ec);
        });
      };

      inline result_type call() {
        return operator();
      }

    private:
      std::vector<async_function> _work;
      std::shared_ptr<result> _result;
      scheduling_t _scheduling;
      end_condition_t _end_condition;
    };
  }
}
