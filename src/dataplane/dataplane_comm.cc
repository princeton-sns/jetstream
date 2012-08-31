
#include <glog/logging.h>

#include "dataplane_comm.h"

using namespace jetstream;
using namespace ::std;
using namespace boost;


void
DataplaneConnManager::enable_connection (shared_ptr<ClientConnection> c,
                                         operator_id_t dest_op_id,
                                         shared_ptr<DataPlaneOperator> dest) {
  boost::system::error_code error;
  
  live_conns[dest_op_id] = c;

  
  c->recv_data_msg(bind(&DataplaneConnManager::got_data_cb,
                                  this, dest_op_id, dest,  _1, _2), error);
  //TODO: what if there's an error? Can there be?
   LOG(INFO) << "dataplane connection enabled for data";
   
  DataplaneMessage response;
  response.set_type(DataplaneMessage::CHAIN_READY);
  c->send_msg(response, error);
}
                     
void
DataplaneConnManager::pending_connection (shared_ptr<ClientConnection> c,
                                          operator_id_t future_op) {
  pending_conns[future_op] = c;
}


void
DataplaneConnManager::created_operator (operator_id_t op_id,
                                        shared_ptr<DataPlaneOperator> dest) {
  shared_ptr<ClientConnection> c = pending_conns[op_id];
  pending_conns.erase(op_id);

  enable_connection(c, op_id, dest);
  
}

void DataplaneConnManager::got_data_cb (operator_id_t dest_id,
                                        shared_ptr<DataPlaneOperator> dest,
                                        const DataplaneMessage &msg,
                                        const boost::system::error_code &error) {

  if (error) {
    LOG(WARNING) << "error trying to read data: " << error.message();
    return;
  }
  
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
  
  

OutgoingConnAdaptor::OutgoingConnAdaptor (ConnectionManager& cm,
                                          const string& addr,
                                          int32_t portno) {
  cm.create_connection(addr, portno, boost::bind(
                 &OutgoingConnAdaptor::conn_created_cb, this, _1, _2));

}

void
OutgoingConnAdaptor::conn_created_cb(shared_ptr<ClientConnection> c,
                                     boost::system::error_code error) {
  conn = c;
  conn_ready.notify_all();
}
  
void
OutgoingConnAdaptor::process (boost::shared_ptr<Tuple> t) {

  boost::unique_lock<boost::mutex> lock(mutex);//wraps mutex in an RIAA pattern
  while (!conn) {
   //SHOULD BLOCK HERE
   LOG(WARNING) << "trying to send data through closed conn. Should block";
   
   system_time wait_until = get_system_time()+ posix_time::milliseconds(wait_for_conn);
   bool conn_established = conn_ready.timed_wait(lock, wait_until);
   
   if (!conn_established) {
    LOG(WARNING) << "timeout on dataplane connection. Retrying. Should tear down instead?";
   }
 }

 DataplaneMessage d;
 d.set_type(DataplaneMessage::DATA);
 d.add_data()->MergeFrom(*t);
 //TODO: could we merge multiple tuples here?

 boost::system::error_code err;
 conn->send_msg(d, err);
}
  