#ifndef AGGREGATE_8BHO25NT
#define AGGREGATE_8BHO25NT

#include "../aggregate.h"

#include <cppconn/prepared_statement.h>

namespace jetstream {
namespace cube {
  
class MysqlAggregate : public Aggregate {
  public:
    MysqlAggregate(jetstream::CubeSchema_Aggregate _schema) : Aggregate(_schema){};

    virtual void set_value_for_insert_entry(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple t, int &tuple_index, int &field_index) = 0;

    virtual void set_value_for_insert_partial_aggregate(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple t, int &tuple_index, int &field_index) = 0;
    
    string get_base_column_name() {
      return name;
    }

    virtual vector<string> get_column_types() = 0;

    virtual vector<string> get_column_names()
    {
      vector<string> decl;
      decl.push_back(get_base_column_name());
      return decl;
    }

    virtual string  get_update_with_new_entry_sql() = 0;
    virtual string  get_update_with_partial_aggregate_sql() = 0;

    virtual void populate_tuple_final(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) =0;
    virtual void populate_tuple_partial(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) =0;

};
  

} /* cube */
} /* jetstream */
#endif /* end of include guard: AGGREGATE_8BHO25NT */
