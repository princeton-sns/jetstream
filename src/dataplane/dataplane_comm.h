//
//  dataplane_comm.h
//  JetStream
//
//  Created by Ariel Rabkin on 8/28/12.
//  Copyright (c) 2012 Ariel Rabkin. All rights reserved.
//

#ifndef JetStream_dataplane_comm_h
#define JetStream_dataplane_comm_h

#include <map>
#include <set>
#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>

#include "connection.h"
#include "dataplaneoperator.h"
#include <glog/logging.h>

namespace  jetstream {
  
  
/**
 *  Instances of this class is responsible for managing incoming data on the
 * dataplane. Each IncomingConnAdaptor is the start of an operator chain
 */
class DataplaneConnManager {

 private:
  std::map<operator_id_t, boost::shared_ptr<ClientConnection> > pending_conns;
  
  std::map<operator_id_t, boost::shared_ptr<ClientConnection> > live_conns;
  
  void got_data_cb (operator_id_t dest_id,
                    boost::shared_ptr<DataPlaneOperator> dest,
                    const DataplaneMessage &msg,
                    const boost::system::error_code &error);
  
  
  void operator= (const DataplaneConnManager &) 
    { LOG(FATAL) << "cannot copy a DataplaneConnManager"; }
  DataplaneConnManager (const DataplaneConnManager &) 
    { LOG(FATAL) << "cannot copy a DataplaneConnManager"; }
  
 public:
  DataplaneConnManager():pending_conns(),live_conns()  {}
 
  void enable_connection (boost::shared_ptr<ClientConnection> c,
                          operator_id_t dest_op_id,
                          boost::shared_ptr<DataPlaneOperator> dest);
                     
  void pending_connection (boost::shared_ptr<ClientConnection> c,
                          operator_id_t future_op);

  void created_operator (operator_id_t op_id,
                         boost::shared_ptr<DataPlaneOperator> dest);
  
};


const int timeout_for_conn = 2000; //ms

class OutgoingConnAdaptor : public Receiver {


 private:
  boost::shared_ptr<ClientConnection> conn;
  boost::condition_variable conn_ready;
  boost::mutex mutex;
  operator_id_t dest_op_id;
  
  void conn_created_cb (boost::shared_ptr<ClientConnection> conn,
                        boost::system::error_code error);
<<<<<<< HEAD

void conn_ready_cb (const DataplaneMessage &msg,
                    const boost::system::error_code &error);
=======
  static const int wait_for_conn = 2000; //ms
>>>>>>> 507e1a3e90b669109e81316a6de1b4752edc2cb7
  
 public:
  OutgoingConnAdaptor (boost::shared_ptr<ClientConnection> c):conn(c) {}
  OutgoingConnAdaptor (ConnectionManager& cm, const Edge& e);

  virtual void process (boost::shared_ptr<Tuple> t);


/*
  void received_data_msg (boost::shared_ptr<ClientConnection> c,
                          const jetstream::DataplaneMessage &msg,
                          const boost::system::error_code &error);
*/

};


}

#endif
