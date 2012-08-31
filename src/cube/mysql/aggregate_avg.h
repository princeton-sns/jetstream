#ifndef AGGREGATE_AVG_Q8TYGR7Q
#define AGGREGATE_AVG_Q8TYGR7Q

#include "aggregate.h"

namespace jetstream {
namespace cube {
  
class MysqlAggregateAvg: public MysqlAggregate{
  public:
    MysqlAggregateAvg(jetstream::CubeSchema_Aggregate _schema) : MysqlAggregate(_schema){};

    vector<string> getColumnTypes()
    {
      vector<string> decl;
      decl.push_back("INT");
      decl.push_back("INT");
      return decl;
    }
    
    vector<string> getColumnNames()
    {
      vector<string> decl;
      decl.push_back(getBaseColumnName()+"_sum");
      decl.push_back(getBaseColumnName()+"_count");
      return decl;
    }

    string getUpdateWithNewEntrySql()
    {
      //VALUES() allow you to incorporate the value of the new entry as it would be if the entry was inserted as a new row;  
      string sql = "`"+getBaseColumnName()+"_sum` = `"+getBaseColumnName()+"_sum` + VALUES(`"+getBaseColumnName()+"`), ";
      sql = "`"+getBaseColumnName()+"_count` = `"+getBaseColumnName()+"_count` + 1";
      return sql;
    }

    void setValueForInsertEntry(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple t, int &tuple_index, int &field_index)
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
};


} /* cube */
} /* jetstream */


#endif /* end of include guard: AGGREGATE_AVG_Q8TYGR7Q */
