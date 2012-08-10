#ifndef _workerclient_H_
#define _workerclient_H_

#include <sys/types.h>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <deque>

#include "js_utils.h"
#include "jetstream_types.pb.h"
#include "jetstream_dataplane.pb.h"

using namespace ::google;
using namespace boost::asio::ip;


namespace jetstream {

typedef edu::princeton::jetstream::Heartbeat ProtobufMsg;

class WorkerConnHandler {
  public:
 WorkerConnHandler(boost::asio::io_service& io_service,
      tcp::resolver::iterator endpoint_iterator);

 void write(const ProtobufMsg &msg);
 void close();

 virtual void processMessage(ProtobufMsg &msg) = 0;


private:

  void expandReadBuf(size_t size);
  void expandWriteBuf(size_t size);
  void handle_connect(const boost::system::error_code& error);
  void handle_read_header(const boost::system::error_code& error);

  void handle_read_body(const boost::system::error_code& error);

  void do_write(ProtobufMsg &msg);

  void handle_write(const boost::system::error_code& error);

  void do_close();
  void sendOneOffWriteQueue();

private:
  void * readBuf;
  size_t readBufSize;
  uint32_t readSize;

  std::deque<ProtobufMsg> writeQueue;
  void * writeBuf;
  size_t writeBufSize;


  boost::asio::io_service& io_service_;
  tcp::socket socket_;
};

}

#endif
