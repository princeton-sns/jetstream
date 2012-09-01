#ifndef AGGREGATE_COUNT_Q8TYGR7Q
#define AGGREGATE_COUNT_Q8TYGR7Q

#include "aggregate.h"

namespace jetstream {
namespace cube {
  
class MysqlAggregateCount: public MysqlAggregate{
  public:
    MysqlAggregateCount(jetstream::CubeSchema_Aggregate _schema) : MysqlAggregate(_schema){};

    vector<string> get_column_types()
    {
      vector<string> decl;
      decl.push_back("INT");
      return decl;
    }
    
    string get_update_with_new_entry_sql()
    {
      string sql = "`"+get_base_column_name()+"` = `"+get_base_column_name()+"` + 1";
      return sql;
    }

    void set_value_for_insert_entry(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple t, int &tuple_index, int &field_index)
    {
      //should have no tuple element for this aggregate for a single entry.
      pstmt->setInt(field_index, 1);
      field_index += 1;
    }
};


} /* cube */
} /* jetstream */


#endif /* end of include guard: AGGREGATE_COUNT_Q8TYGR7Q */
