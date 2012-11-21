#ifndef JS_CNT_EXECUTOR_H_
#define JS_CNT_EXECUTOR_H_

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/thread.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>
#include <boost/bind/apply.hpp>
#include <boost/bind/protect.hpp>

namespace jetstream {
using boost::bind;
using boost::thread;
using boost::thread_group;
using boost::asio::io_service;
using boost::shared_ptr;
using boost::make_shared;

class CountingExecutor {
  public:

    CountingExecutor(size_t n): service(new io_service(n)), work(*service), outstanding(0) {
      start_threads(n);
    }

    CountingExecutor(shared_ptr<io_service> serv): service(serv), work(*service), outstanding(0) {
    }

    ~CountingExecutor() {
      service->stop();
      pool.join_all();
    }

    void start_threads(size_t n) {
      for (size_t i = 0; i < n; i++) {
        pool.create_thread(bind(&io_service::run, service));
      }
    }

    template<typename Handler> void submit(Handler task) {
      ++outstanding;
      //can't bind to an abstract function
      //don't wan't handler executed before calling run, so protect!
      service->post(boost::bind(&CountingExecutor::run<boost::_bi::protected_bind_t<Handler> >, this, boost::protect(task)));
    }
 
    /*template<typename Handler> 
      inline void dispatch(Handler task) {
      ++outstanding;
      //can't bind to an abstract function
      //don't wan't handler executed before calling run, so protect!
      service->dispatch(boost::bind(&CountingExecutor::run<boost::_bi::protected_bind_t<Handler> >, this, boost::protect(task)));
    }

    template<typename Handler>
    boost::asio::detail::wrapped_handler<CountingExecutor&, Handler>
    wrap(Handler task)
    {
      //wrapped handler will call the dispatch method
      return boost::asio::detail::wrapped_handler<CountingExecutor&, Handler>(*this, task);
    }
*/
    size_t outstanding_tasks()
    {
      return outstanding;
    }

    shared_ptr<boost::asio::strand> make_strand()
    {
       shared_ptr<boost::asio::strand> pStrand(new boost::asio::strand(*service));
       return pStrand;
    }
    
    shared_ptr<io_service> get_io_service() {
      return service;
    }
  
  protected:
    template<typename Handler> void run(Handler task)
    {
      task();
      --outstanding;
    }
    
    thread_group pool;
    shared_ptr<io_service> service;
    io_service::work work;
    boost::detail::atomic_count outstanding;
};
}

#endif // JS_CNT_EXECUTOR_H_

