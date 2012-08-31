#ifndef AGGREGATE_COUNT_Q8TYGR7Q
#define AGGREGATE_COUNT_Q8TYGR7Q

#include "aggregate.h"

namespace jetstream {
namespace cube {
  
class MysqlAggregateCount: public MysqlAggregate{
  public:
    MysqlAggregateCount(jetstream::CubeSchema_Aggregate _schema) : MysqlAggregate(_schema){};

    vector<string> getColumnTypes()
    {
      vector<string> decl;
      decl.push_back("INT");
      return decl;
    }
    
    string getUpdateWithNewEntrySql()
    {
      string sql = "`"+getBaseColumnName()+"` = `"+getBaseColumnName()+"` + 1";
      return sql;
    }

    void setValueForInsertEntry(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple t, int &tuple_index, int &field_index)
    {
      //should have no tuple element for this aggregate for a single entry.
      pstmt->setInt(field_index, 1);
      field_index += 1;
    }
};


} /* cube */
} /* jetstream */


#endif /* end of include guard: AGGREGATE_COUNT_Q8TYGR7Q */
