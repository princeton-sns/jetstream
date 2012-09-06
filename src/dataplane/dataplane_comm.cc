#include <glog/logging.h>
#include "dataplane_comm.h"

using namespace jetstream;
using namespace std;
using namespace boost;


void
DataplaneConnManager::enable_connection (shared_ptr<ClientConnection> c,
                                         operator_id_t dest_op_id,
                                         shared_ptr<DataPlaneOperator> dest) 
{
  liveConns[dest_op_id] = c;

  boost::system::error_code error;
  c->recv_data_msg(bind(&DataplaneConnManager::got_data_cb,
			this, dest_op_id, dest,  _1, _2), error);

  DataplaneMessage response;

  if (!error) {
    LOG(INFO) << "dataplane connection enabled for data";
    response.set_type(DataplaneMessage::CHAIN_READY);
    // XXX This should include an Edge
  }

  c->send_msg(response, error);
}
                
     
void
DataplaneConnManager::pending_connection (shared_ptr<ClientConnection> c,
                                          operator_id_t future_op) 
{
  pendingConns[future_op] = c;
}


void
DataplaneConnManager::created_operator (operator_id_t op_id,
                                        shared_ptr<DataPlaneOperator> dest) 
{
  shared_ptr<ClientConnection> c = pendingConns[op_id];
  pendingConns.erase(op_id);

  enable_connection(c, op_id, dest);
}

void
DataplaneConnManager::got_data_cb (operator_id_t dest_id,
                                   shared_ptr<DataPlaneOperator> dest,
                                   const DataplaneMessage &msg,
                                   const boost::system::error_code &error) 
{

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
     shared_ptr<ClientConnection> c = liveConns[dest_id];
     assert(c);
     LOG(WARNING) << "unexpected dataplane message: "<<msg.type() << 
        " from " << c->get_remote_endpoint() << " for existing dataplane connection";
  }
}
  
  
void
DataplaneConnManager::close() {
  //TODO: gracefully stop connections
}
  

OutgoingConnAdaptor::OutgoingConnAdaptor (ConnectionManager& cm,
                                          const Edge & e) {
                                          
  const std::string& addr = e.dest_addr().address();
  int32_t portno = e.dest_addr().portno();      
  dest_op_id.computation_id = e.computation();
  dest_op_id.task_id = e.dest();
                                          
  cm.create_connection(addr, portno, boost::bind(
                 &OutgoingConnAdaptor::conn_created_cb, this, _1, _2));
}

void
OutgoingConnAdaptor::conn_created_cb(shared_ptr<ClientConnection> c,
                                     boost::system::error_code error) 
{
  conn = c;
  
  DataplaneMessage data_msg;
  data_msg.set_type(DataplaneMessage::CHAIN_CONNECT);
  
  Edge * edge = data_msg.mutable_chain_link();
  edge->set_computation(dest_op_id.computation_id);
  edge->set_dest(dest_op_id.task_id);
  edge->set_src(0);
  
  boost::system::error_code err;
  conn->recv_data_msg(boost::bind( &OutgoingConnAdaptor::conn_ready_cb, 
           this, _1, _2), err);
  conn->send_msg(data_msg, err);

  //send chain ready, setup cb for ready
}

void
OutgoingConnAdaptor::conn_ready_cb(const DataplaneMessage &msg,
                                        const boost::system::error_code &error) {

  if (msg.type() == DataplaneMessage::CHAIN_READY) {
    LOG(INFO) << "got ready back";
    conn_ready.notify_all();  
  } 
  else {
    LOG(WARNING) << "unexpected response to Chain connect: " << msg.type() << 
       " error code is " << error;  
  }
  
}

  
void
OutgoingConnAdaptor::process (boost::shared_ptr<Tuple> t) 
{
  unique_lock<boost::mutex> lock(mutex);//wraps mutex in an RIAA pattern
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
  
