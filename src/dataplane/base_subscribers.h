#ifndef Q_SUBSCRIBER_2FAEJ0UJ
#define Q_SUBSCRIBER_2FAEJ0UJ

#include <boost/shared_ptr.hpp>

#include <boost/thread.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>


#include "subscriber.h"
#include "dataplaneoperator.h"
#include "jetstream_types.pb.h"

namespace jetstream {
namespace cube {

class QueueSubscriber: public Subscriber {
  public:
    QueueSubscriber(): Subscriber (), returnAction(SEND) {};
    virtual ~QueueSubscriber() {};

    Action returnAction;
    std::vector<boost::shared_ptr<jetstream::Tuple> > insert_q;
    std::vector<boost::shared_ptr<jetstream::Tuple> > update_q;

    virtual Action action_on_tuple(boost::shared_ptr<const jetstream::Tuple> const update);

    virtual void insert_callback(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value);

    virtual void update_callback(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value,
                                 boost::shared_ptr<jetstream::Tuple> const &old_value);

};

class UnionSubscriber: public Subscriber {
  public:
    UnionSubscriber(): Subscriber (){};
    virtual ~UnionSubscriber() {};

    virtual Action action_on_tuple(boost::shared_ptr<const jetstream::Tuple> const update);

    virtual void insert_callback(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value);

    virtual void update_callback(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value,
                                 boost::shared_ptr<jetstream::Tuple> const &old_value);

};

} /* cube */



class TimeBasedSubscriber: public jetstream::cube::Subscriber {
  private:
    bool running;
    boost::shared_ptr<boost::thread> loopThread;
    int windowSizeMs;
  
  public:

    TimeBasedSubscriber(): running(true) {};



    virtual ~TimeBasedSubscriber() {};


    virtual Action action_on_tuple(boost::shared_ptr<const jetstream::Tuple> const update) ;

    virtual void insert_callback(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value);

    virtual void update_callback(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value, 
                                 boost::shared_ptr<jetstream::Tuple> const &old_value);
  
    virtual operator_err_t configure(std::map<std::string,std::string> &config);
    virtual void start();
    virtual void stop() {running = false; }

    void operator()();  // A thread that will loop while reading the file


  private:
  
    const static std::string my_type_name;
  public:
    virtual const std::string& typename_as_str() {
      return my_type_name;
    }
};

} /* jetsream */

#endif /* end of include guard: Q_SUBSCRIBER_2FAEJ0UJ */
