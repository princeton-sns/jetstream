#include "worker_conn_handler.h"

jetstream::WriteQueueElement::WriteQueueElement(const ProtobufMsg *msg)
{
  sz= msg->ByteSize();
  buf = (char *) malloc(sz+4);
  memcpy(&sz, buf, 4);
  msg->SerializeToArray(((char *)buf)+4, sz);
}

jetstream::WriteQueueElement::~WriteQueueElement()
{
  free(buf);
}



jetstream::WorkerConnHandler::WorkerConnHandler(boost::asio::io_service& io_service,
      tcp::resolver::iterator endpoint_iterator)
    : io_service_(io_service),
      socket_(io_service)
  {
    readBufSize = 0;
    readBuf = NULL;
    boost::asio::async_connect(socket_, endpoint_iterator,
        boost::bind(&WorkerConnHandler::handle_connect, this,
          boost::asio::placeholders::error));
  }

  void jetstream::WorkerConnHandler::expand_read_buf(size_t size)
  {
    if(size <= readBufSize) 
      return;
    
    if(size <= readBufSize * 2) 
      size = readBufSize * 2;

    if(readBuf == NULL) 
      readBuf = malloc(size);
    else 
      readBuf = realloc(readBuf, size);
  }


  void jetstream::WorkerConnHandler::write(const ProtobufMsg *msg)
  {
    WriteQueueElement *we = new WriteQueueElement(msg);
    io_service_.post(boost::bind(&WorkerConnHandler::do_write, this, we));
  }

  void jetstream::WorkerConnHandler::close()
  {
    io_service_.post(boost::bind(&WorkerConnHandler::do_close, this));
  }


  void jetstream::WorkerConnHandler::handle_connect(const boost::system::error_code& error)
  {
    if (!error)
    {
      boost::asio::async_read(socket_,
          boost::asio::buffer(&readSize, sizeof(uint32_t)),
          boost::bind(&WorkerConnHandler::handle_read_header, this,
            boost::asio::placeholders::error));
    }
  }

  void jetstream::WorkerConnHandler::handle_read_header(const boost::system::error_code& error)
  {
    if (!error)
    {
      expand_read_buf((size_t)readSize);
      boost::asio::async_read(socket_,
          boost::asio::buffer(readBuf, readSize),
          boost::bind(&WorkerConnHandler::handle_read_body, this,
            boost::asio::placeholders::error));
    }
    else
    {
      do_close();
    }
  }

  void jetstream::WorkerConnHandler::handle_read_body(const boost::system::error_code& error)
  {
    if (!error)
    {
      process_message((char *)readBuf, readSize);
       boost::asio::async_read(socket_,
          boost::asio::buffer(&readSize, sizeof(uint32_t)),
          boost::bind(&WorkerConnHandler::handle_read_header, this,
            boost::asio::placeholders::error));
    }
    else
    {
      do_close();
    }
  }

  void jetstream::WorkerConnHandler::do_write(WriteQueueElement *we)
  {
    bool write_in_progress = !writeQueue.empty();
    writeQueue.push_back(we);
    if (!write_in_progress)
      send_one_off_write_queue();
  }

  void jetstream::WorkerConnHandler::send_one_off_write_queue()
  {
      WriteQueueElement * wqe = writeQueue.front();

      boost::asio::async_write(socket_,
          boost::asio::buffer(wqe->buf, wqe->sz+4),
          boost::bind(&WorkerConnHandler::handle_write, this,
            boost::asio::placeholders::error));

  }

  void jetstream::WorkerConnHandler::handle_write(const boost::system::error_code& error)
  {
    if (!error)
    {
      WriteQueueElement * wqe = writeQueue.front();
      writeQueue.pop_front();
      delete wqe;

      if (!writeQueue.empty())
        send_one_off_write_queue();
    }
    else
    {
      do_close();
    }
  }

  void jetstream::WorkerConnHandler::do_close()
  {
    socket_.close();
  }


