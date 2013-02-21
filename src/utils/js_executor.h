#ifndef JS_EXECUTOR_H_
#define JS_EXECUTOR_H_

#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/thread.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>


namespace jetstream {
using boost::bind;
using boost::thread;
using boost::thread_group;
using boost::asio::io_service;
using boost::shared_ptr;
using boost::make_shared;

class Executor {
  public:
    std::string name;

    Executor(size_t n, std::string name = "Executor"): name(name), service(new io_service(n)), work(*service) {
      start_threads(n);
    }

    Executor(shared_ptr<io_service> serv): service(serv), work(*service) {
    }

    ~Executor() {
      service->stop();
      pool.join_all();
    }

    void start_threads(size_t n) {
      for (size_t i = 0; i < n; i++) {
        pool.create_thread(bind(&Executor::run, this));
      }
    }

    void run() {
      jetstream::set_thread_name(name);
      service->run();
    }

    template<typename F> void submit(F task) {
      service->post(task);
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
    thread_group pool;
    shared_ptr<io_service> service;
    io_service::work work;
};
}

#endif // JS_EXECUTOR_H_

