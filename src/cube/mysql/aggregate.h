#ifndef AGGREGATE_8BHO25NT
#define AGGREGATE_8BHO25NT

#include "../aggregate.h"

#include <cppconn/prepared_statement.h>

namespace jetstream {
namespace cube {

class MysqlAggregate : public Aggregate {
  public:
    MysqlAggregate() : Aggregate() {};

    virtual void set_value_for_insert_tuple(
      boost::shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple const &t,
      int &field_index) = 0;
    
    virtual void make_full_tuple(jetstream::Tuple &t) const;
    virtual void insert_default_values_for_full_tuple(jetstream::Tuple &t) const =0;
    virtual void merge_tuple_into(jetstream::Tuple &into, jetstream::Tuple const &update) const;

    std::string get_base_column_name() const;

    virtual std::vector<std::string> get_column_types() const = 0;
    virtual std::vector<std::string> get_column_names() const;

    virtual std::string  get_update_on_insert_sql() const = 0;

    virtual void populate_tuple_final(
      boost::shared_ptr<jetstream::Tuple> t,
      boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const =0;

    virtual void populate_tuple_partial(
      boost::shared_ptr<jetstream::Tuple> t,
      boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const =0;

    virtual std::string get_select_clause_for_rollup() const = 0;
  
    virtual void update_from_delta(jetstream::Tuple & newV, const jetstream::Tuple& oldV) const {
        //no-op, just keep new
    }

  protected:
    virtual void merge_full_tuple_into(jetstream::Tuple &into, jetstream::Tuple const &update) const = 0;

};
    


} /* cube */
} /* jetstream */
#endif /* end of include guard: AGGREGATE_8BHO25NT */
