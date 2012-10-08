#ifndef DIMENSION_Q8TYGR7Q
#define DIMENSION_Q8TYGR7Q

#include "../dimension.h"
#include <glog/logging.h>

#include <cppconn/prepared_statement.h>

namespace jetstream {
namespace cube {
  
class MysqlDimension: public Dimension{
  public:
    MysqlDimension(jetstream::CubeSchema_Dimension _schema) : Dimension(_schema){};

    virtual void set_value_for_insert_tuple(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple const &t, int &field_index) const = 0;

    virtual void set_value_for_insert(shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple const &t, int &tuple_index, int &field_index) const = 0;
    virtual void populate_tuple(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const = 0;
    

    //TODO: right now this creates a full sql string. Maybe change to string with placehholders and preparedStatement?
    virtual string get_where_clause_exact(jetstream::Tuple const &t, int &tuple_index, bool is_optional = true) const;
    virtual string get_where_clause_exact_prepared() const;
    virtual string get_where_clause_greater_than_eq(jetstream::Tuple const &t, int &tuple_index, bool is_optional = true) const;
    virtual string get_where_clause_less_than_eq(jetstream::Tuple const &t, int &tuple_index, bool is_optional = true) const;
    virtual string get_select_clause_for_rollup(unsigned int const level) const = 0;
    virtual string get_groupby_clause_for_rollup(unsigned int const level) const = 0;

    string get_base_column_name() const;
    string get_rollup_level_column_name() const;
    virtual vector<string> get_column_names() const;
    virtual vector<string> get_column_types() const = 0;
    
    void set_connection(shared_ptr<sql::Connection> con) ;
    shared_ptr<sql::Connection> get_connection() const ;

  protected:
    virtual string get_where_clause(jetstream::Tuple const &t, int &tuple_index, string op, bool is_optional = true) const = 0;
    shared_ptr<sql::Connection> connection;
};


} /* cube */
} /* jetstream */


#endif /* end of include guard: DIMENSION_Q8TYGR7Q */
