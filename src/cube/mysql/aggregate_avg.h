#ifndef AGGREGATE_AVG_Q8TYGR7Q
#define AGGREGATE_AVG_Q8TYGR7Q

#include "aggregate.h"
#include <glog/logging.h>

namespace jetstream {
namespace cube {
  
class MysqlAggregateAvg: public MysqlAggregate{
  public:
    MysqlAggregateAvg(jetstream::CubeSchema_Aggregate _schema) : MysqlAggregate(_schema){};

    vector<string> get_column_types()
    {
      vector<string> decl;
      decl.push_back("INT");
      decl.push_back("INT");
      return decl;
    }
    
    vector<string> get_column_names()
    {
      vector<string> decl;
      decl.push_back(get_base_column_name()+"_sum");
      decl.push_back(get_base_column_name()+"_count");
      return decl;
    }

    string get_update_with_new_entry_sql()
    {
      //VALUES() allow you to incorporate the value of the new entry as it would be if the entry was inserted as a new row;  
      string sql = "`"+get_base_column_name()+"_sum` = `"+get_base_column_name()+"_sum` + VALUES(`"+get_base_column_name()+"_sum`), ";
      sql += "`"+get_base_column_name()+"_count` = `"+get_base_column_name()+"_count` + 1";
      return sql;
    }

    void set_value_for_insert_entry(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple t, int &tuple_index, int &field_index)
    {
      jetstream::Element e = t.e(tuple_index);
      if(e.has_i_val())
      {
        pstmt->setInt(field_index, e.i_val());
        pstmt->setInt(field_index+1, 1);
        tuple_index += 1;
        field_index += 2;
        return;
      }
      if(e.has_d_val())
      {
        pstmt->setDouble(field_index, e.d_val());
        pstmt->setInt(field_index+1, 1);
        tuple_index += 1;
        field_index += 2;
        return;
      }

      LOG(FATAL) << "Something went wrong when processing tuple for field "<< name;
    }

  void populate_tuple_final(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) {
    int sum = resultset->getInt(column_index);
    int count = resultset->getInt(column_index+1);
    column_index += 2;
    jetstream::Element * elem = t->add_e();
    elem->set_i_val(sum/count);
    elem->set_d_val((float)sum/(float)count);
  }

   void populate_tuple_partial(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) {
    int sum = resultset->getInt(column_index);
    int count = resultset->getInt(column_index+1);
    column_index += 2;
    jetstream::Element * elem = t->add_e();
    elem->set_i_val(sum);
    elem = t->add_e();
    elem->set_i_val(count);
  }

};

} /* cube */
} /* jetstream */


#endif /* end of include guard: AGGREGATE_AVG_Q8TYGR7Q */
