#include "aggregate_count.h"

#include <glog/logging.h>

using namespace std;
using namespace jetstream::cube;

 vector<string>  MysqlAggregateCount::get_column_types() const
    {
      vector<string> decl;
      decl.push_back("INT");
      return decl;
    }
    
    string MysqlAggregateCount::get_update_with_new_entry_sql() const
    {
      string sql = "`"+get_base_column_name()+"` = `"+get_base_column_name()+"` + 1";
      return sql;
    }

    string MysqlAggregateCount::get_update_with_partial_aggregate_sql() const
    {
      string sql = "`"+get_base_column_name()+"` = `"+get_base_column_name()+"` + VALUES(`"+get_base_column_name()+"`)";
      return sql;
    }


    void MysqlAggregateCount::set_value_for_insert_entry(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple const &t, int &tuple_index, int &field_index) const
    {
      //should have no tuple element for this aggregate for a single entry.
      pstmt->setInt(field_index, 1);
      field_index += 1;
    }

    void MysqlAggregateCount::set_value_for_insert_partial_aggregate(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple const &t, int &tuple_index, int &field_index) const
    {
      jetstream::Element * const e = const_cast<jetstream::Tuple &>(t).mutable_e(tuple_index);

      if(e->has_i_val())
      {
        pstmt->setInt(field_index, e->i_val());
        tuple_index += 1;
        field_index += 1;
        return;
      }
      LOG(FATAL) << "Something went wrong when processing tuple for field "<< name;
    }





  void MysqlAggregateCount::populate_tuple_final(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const {
    int count = resultset->getInt(column_index);
    ++column_index;
    jetstream::Element * elem = t->add_e();
    elem->set_i_val(count);
  }
  
  void MysqlAggregateCount::populate_tuple_partial(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const {
    populate_tuple_final(t, resultset, column_index);
  }

