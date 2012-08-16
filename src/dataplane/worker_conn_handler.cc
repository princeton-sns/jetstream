#include "worker_conn_handler.h"

using namespace jetstream;

WorkerConnHandler::WriteQueueElement::WriteQueueElement (const ProtobufMsg *msg)
{
  // XXX We should avoid such memcpy's whenever possible
  unsigned int tmp = msg->ByteSize();
  assert (tmp <= MAX_UINT32);
  sz = (u_int32_t) tmp;

  buf = (char *) malloc(sz + sizeof(u_int32_t));
  memcpy(&sz, buf, sizeof(u_int32_t));
  msg->SerializeToArray(((char *) buf) + sizeof (int32_t), sz);
}



WorkerConnHandler::WorkerConnHandler (boost::asio::io_service &io_service,
				      tcp::resolver::iterator endpoint_iterator)
  : readBuf(NULL),
    readBufSize (0),
    readSize (0),
    iosrv (io_service),
    sock (io_service)
{
  boost::asio::async_connect(sock, endpoint_iterator,
			     boost::bind(&WorkerConnHandler::handle_connect, this,
					 boost::asio::placeholders::error));
}


void 
WorkerConnHandler::expand_read_buf (size_t size)
{
  if (size <= readBufSize) 
    return;
  
  if (size <= readBufSize * 2) 
    size = readBufSize * 2;
  
  if (readBuf == NULL) 
    readBuf = malloc(size);
  else 
    readBuf = realloc(readBuf, size);
}


void 
WorkerConnHandler::write  (const ProtobufMsg *msg)
{
  WriteQueueElement *we = new WriteQueueElement(msg);
  iosrv.post(boost::bind(&WorkerConnHandler::do_write, this, we));
}


void 
WorkerConnHandler::close ()
{
  iosrv.post(boost::bind(&WorkerConnHandler::do_close, this));
}


void 
WorkerConnHandler::handle_connect  (const boost::system::error_code &error)
{
  if (error)
    return;

  boost::asio::async_read(sock,
			  boost::asio::buffer(&readSize, sizeof(uint32_t)),
			  boost::bind(&WorkerConnHandler::handle_read_header, this,
				      boost::asio::placeholders::error));
}


void 
WorkerConnHandler::handle_read_header (const boost::system::error_code &error)
{
  if (error)
    do_close();
  else {
    expand_read_buf((size_t)readSize);
    boost::asio::async_read(sock,
			    boost::asio::buffer(readBuf, readSize),
			    boost::bind(&WorkerConnHandler::handle_read_body, this,
					boost::asio::placeholders::error));
  }
}


void 
WorkerConnHandler::handle_read_body (const boost::system::error_code &error)
{
  if (error)
    do_close();
  else {
      process_message((char *)readBuf, readSize);
       boost::asio::async_read(sock,
          boost::asio::buffer(&readSize, sizeof(uint32_t)),
          boost::bind(&WorkerConnHandler::handle_read_header, this,
            boost::asio::placeholders::error));
  }
}


void 
WorkerConnHandler::do_write(WriteQueueElement *we)
{
  bool write_in_progress = !writeQueue.empty();
  writeQueue.push_back(we);
  if (!write_in_progress)
    send_one_off_write_queue();
}


void 
WorkerConnHandler::send_one_off_write_queue ()
{
  WriteQueueElement *wqe = writeQueue.front();
  
  boost::asio::async_write(sock,
			   boost::asio::buffer(wqe->buf, wqe->sz+4),
			   boost::bind(&WorkerConnHandler::handle_write, this,
				       boost::asio::placeholders::error));
  
}


void 
WorkerConnHandler::handle_write (const boost::system::error_code &error)
{
  if (error)
    do_close();
  else {
    WriteQueueElement *wqe = writeQueue.front();
    writeQueue.pop_front();
    delete wqe;
    
    if (!writeQueue.empty())
      send_one_off_write_queue();
  }
}


void 
WorkerConnHandler::do_close ()
{
  if (!sock.is_open())
    return;

  boost::system::error_code error;
  sock.shutdown(tcp::socket::shutdown_both, error);
  
  if (error) {
    std::cerr << "WorkerConnHandler::do_close: " << error << std::endl;
  }
  
  sock.close (error);
  
  if (error) {
    std::cerr << "WorkerConnHandler::do_close: " << error << std::endl;
  }
}
