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
 * Instances of this class is responsible for managing incoming data on the
 * dataplane. Each IncomingConnAdaptor is the start of an operator chain
 */
class DataplaneConnManager {

 private:
  std::map<operator_id_t, boost::shared_ptr<ClientConnection> > pendingConns;
  std::map<operator_id_t, boost::shared_ptr<ClientConnection> > liveConns;
  
  void got_data_cb (operator_id_t dest_id,
                    boost::shared_ptr<DataPlaneOperator> dest,
                    const DataplaneMessage &msg,
                    const boost::system::error_code &error);
  
  
  void operator= (const DataplaneConnManager &) 
    { LOG(FATAL) << "cannot copy a DataplaneConnManager"; }
  DataplaneConnManager (const DataplaneConnManager &) 
    { LOG(FATAL) << "cannot copy a DataplaneConnManager"; }
  
 public:
  DataplaneConnManager () {}
 
  void enable_connection (boost::shared_ptr<ClientConnection> c,
                          operator_id_t dest_op_id,
                          boost::shared_ptr<DataPlaneOperator> dest);
                     
  void pending_connection (boost::shared_ptr<ClientConnection> c,
                          operator_id_t future_op);

  void created_operator (operator_id_t opid,
                         boost::shared_ptr<DataPlaneOperator> dest);
                         
  void close();
};



class RemoteDestAdaptor : public TupleReceiver {

 private:
  boost::shared_ptr<ClientConnection> conn;
  boost::condition_variable chainReadyCond;
  boost::mutex mutex;
  bool chainIsReady;
//  bool stopping;
  operator_id_t destOpId;
  std::string remoteAddr;
  
  void conn_created_cb (boost::shared_ptr<ClientConnection> conn,
                        boost::system::error_code error);
  
  void conn_ready_cb (const DataplaneMessage &msg,
                      const boost::system::error_code &error);
   
  static const msec_t wait_for_conn = 5000; // Note this is a wide area wait.
  
 public:
  //  The below ctor might be useful at some future point, but for now we aren't
  //    using it and therefore won't have it enabled
//  RemoteDestAdaptor (boost::shared_ptr<ClientConnection> c) :
//      conn (c), chainIsReady(false), stopping(false) {}

  RemoteDestAdaptor (ConnectionManager &cm, const jetstream::Edge&);
  virtual ~RemoteDestAdaptor() {}

  virtual void process (boost::shared_ptr<Tuple> t);
  
  std::string as_string();
};


}

#endif
