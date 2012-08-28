
#include <glog/logging.h>

#include "dataplane_comm.h"

using namespace jetstream;
using namespace ::std;
using namespace boost;


void
DataplaneConnManager::enable_connection (shared_ptr<ClientConnection> c,
                                         operator_id_t dest_op_id,
                                         shared_ptr<DataPlaneOperator> dest)
{
  boost::system::error_code error;
  
  live_conns[dest_op_id] = c;

  c->recv_data_msg(bind(&DataplaneConnManager::got_data_cb,
                                  this, dest_op_id, dest,  _1, _2), error);
  //TODO: what if there's an error? Can there be?
   LOG(INFO) << "dataplane connection enabled for data";
}
                     
void
DataplaneConnManager::pending_connection (shared_ptr<ClientConnection> c,
                                          operator_id_t future_op)
{
  pending_conns[future_op] = c;
}


void
DataplaneConnManager::created_operator (operator_id_t op_id,
                                        shared_ptr<DataPlaneOperator> dest)
{
  shared_ptr<ClientConnection> c = pending_conns[op_id];
  pending_conns.erase(op_id);

  enable_connection(c, op_id, dest);

  boost::system::error_code error;  
  DataplaneMessage response;
  response.set_type(DataplaneMessage::CHAIN_READY);
  c->send_msg(response, error);
  
}

void DataplaneConnManager::got_data_cb (operator_id_t dest_id,
                                        shared_ptr<DataPlaneOperator> dest,
                                        const DataplaneMessage &msg,
                                        const boost::system::error_code &error)
{

  if (error) {
    LOG(WARNING) << "error trying to read data: " << error.message();
    return;
  }
  
  LOG(INFO) << "dataplane got data from remote host";
  
  cout << msg.Utf8DebugString();

  
  if (!dest)
    LOG(FATAL) << "got data but no operator to receive it";
  
  
  switch (msg.type ()) {
    case DataplaneMessage::DATA:
    {
      shared_ptr<Tuple> data (new Tuple);
      for(int i=0; i < msg.data_size(); ++i) {
        data->MergeFrom (msg.data(i));
        dest->process(data);
      }
      break;
    }
    default:
     shared_ptr<ClientConnection> c = live_conns[dest_id];
     assert(c);
     LOG(WARNING) << "unexpected dataplane message: "<<msg.type() << 
        " from " << c->get_remote_endpoint() << " for existing dataplane connection";
        
  }
}
  