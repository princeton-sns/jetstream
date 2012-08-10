#include "worker_conn_handler.h"

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

  void jetstream::WorkerConnHandler::expandReadBuf(size_t size)
  {
    if(size <= readBufSize) 
    {
      return;
    }

    if(size <= readBufSize * 2)
    {
      size = readBufSize * 2;
    }

    if(readBuf == NULL)
    {
      readBuf = malloc(size);
    }
    else
    {
      readBuf = realloc(readBuf, size);
    }
  }

  void jetstream::WorkerConnHandler::expandWriteBuf(size_t size)
  {
    if(size <= writeBufSize) 
    {
      return;
    }

    if(size <= writeBufSize * 2)
    {
      size = writeBufSize * 2;
    }

    if(writeBuf == NULL)
    {
      readBuf = malloc(size);
    }
    else
    {
      writeBuf = realloc(writeBuf, size);
    }
  }



  void jetstream::WorkerConnHandler::write(const ProtobufMsg &msg)
  {
    io_service_.post(boost::bind(&WorkerConnHandler::do_write, this, msg));
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
      expandReadBuf((size_t)readSize);
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
      ProtobufMsg *wr = new ProtobufMsg;
      wr->ParseFromArray(readBuf, readSize);
      processMessage(*wr);
      delete wr;
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

  void jetstream::WorkerConnHandler::do_write(ProtobufMsg& msg)
  {
    bool write_in_progress = !writeQueue.empty();
    writeQueue.push_back(msg);
    if (!write_in_progress)
    {
      sendOneOffWriteQueue();
    }
  }

  void jetstream::WorkerConnHandler::sendOneOffWriteQueue()
  {
      ProtobufMsg msg_send = writeQueue.front();
      uint32_t size = msg_send.ByteSize();
      expandWriteBuf(size+4);
      memcpy(&size, writeBuf, 4);
      msg_send.SerializeToArray(((char *)writeBuf)+4, size);

      boost::asio::async_write(socket_,
          boost::asio::buffer(writeBuf, size+4),
          boost::bind(&WorkerConnHandler::handle_write, this,
            boost::asio::placeholders::error));

  }

  void jetstream::WorkerConnHandler::handle_write(const boost::system::error_code& error)
  {
    if (!error)
    {
      writeQueue.pop_front();
      if (!writeQueue.empty())
      {
        sendOneOffWriteQueue();
      }
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


