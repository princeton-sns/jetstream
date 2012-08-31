#ifndef AGGREGATE_8BHO25NT
#define AGGREGATE_8BHO25NT

#include "../aggregate.h"

#include <cppconn/prepared_statement.h>

namespace jetstream {
namespace cube {
  
class MysqlAggregate : public Aggregate {
  public:
    MysqlAggregate(jetstream::CubeSchema_Aggregate _schema) : Aggregate(_schema){};

    virtual void setValueForInsertEntry(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple t, int &tuple_index, int &field_index) = 0;
    string getBaseColumnName() {
      return name;
    }

    virtual vector<string> getColumnTypes() = 0;

    virtual vector<string> getColumnNames()
    {
      vector<string> decl;
      decl.push_back(getBaseColumnName());
      return decl;
    }

    virtual string  getUpdateWithNewEntrySql() = 0;

};
  

} /* cube */
} /* jetstream */
#endif /* end of include guard: AGGREGATE_8BHO25NT */
