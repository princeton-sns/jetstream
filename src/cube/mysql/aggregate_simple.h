#ifndef AGGREGATE_SIMPLE_Q8TYGR7Q
#define AGGREGATE_SIMPLE_Q8TYGR7Q

#include "aggregate.h"

namespace jetstream {
namespace cube {

/**
 * @brief Aggregate for simple counts
 *
 * format of input tuples:
 *
 * single entry: (none)
 * partial aggregate: [count of items]
 */
template <class ValueType>
class MysqlAggregateSimple: public MysqlAggregate {
  public:
    MysqlAggregateSimple() : MysqlAggregate() {};

    virtual void insert_default_values_for_full_tuple(jetstream::Tuple &t) const {}
    size_t number_tuple_elements() const {
      return 1;
    }
    virtual void set_value_for_insert_tuple(
      shared_ptr<sql::PreparedStatement> pstmt, jetstream::Tuple const &t,
      int &field_index);
  
    void populate_tuple_final(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const ;
    void populate_tuple_partial(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const {
      populate_tuple_final(t, resultset, column_index);
    }

    inline void set_value(jetstream::Element * e, ValueType val) const;
    inline ValueType get_value(jetstream::Element const * e) const;
    inline void set_db_value(shared_ptr<sql::PreparedStatement> pstmt, int &field_index, ValueType& val) const;
    inline ValueType get_db_value(boost::shared_ptr<sql::ResultSet> resultset, int &col_index) const;
    inline vector<string> get_column_types() const;
    
    virtual ValueType combine(ValueType a, ValueType b) const = 0;
    virtual string get_update_on_insert_sql() const = 0;
    virtual string get_select_clause_for_rollup() const =0;
  protected:
    virtual void merge_full_tuple_into(jetstream::Tuple &into, jetstream::Tuple const &update) const;

};


//////////////// int /////////////////////
template<>
inline
void  MysqlAggregateSimple<int>::set_value(jetstream::Element * e, int val) const {
  e->set_i_val(val);
}

template<>
inline
int MysqlAggregateSimple<int>::get_value(jetstream::Element const * e) const {
  return e->i_val();
}

template<>
inline
void MysqlAggregateSimple<int>::set_db_value(shared_ptr<sql::PreparedStatement> pstmt, int &field_index, int& val) const {
  pstmt->setInt(field_index, val);
}

template<>
inline
int MysqlAggregateSimple<int>::get_db_value(boost::shared_ptr<sql::ResultSet> resultset, int &col_index) const {
  return resultset->getInt(col_index);
}

template<>
inline
vector<string> MysqlAggregateSimple<int>::get_column_types() const {
  vector<string> decl;
  decl.push_back("INT");
  return decl;
}


//////////////// double /////////////////////
template<>
inline
void  MysqlAggregateSimple<double>::set_value(jetstream::Element * e, double val) const{
  e->set_d_val(val);
}

template<>
inline
double MysqlAggregateSimple<double>::get_value(jetstream::Element const * e) const{
  return e->d_val();
}

template<>
inline
void MysqlAggregateSimple<double>::set_db_value(shared_ptr<sql::PreparedStatement> pstmt, int &field_index, double& val) const {
  pstmt->setDouble(field_index, val);
}

template<>
inline
double MysqlAggregateSimple<double>::get_db_value(boost::shared_ptr<sql::ResultSet> resultset, int &col_index) const {
  return resultset->getDouble(col_index);
}

template<>
inline
vector<string> MysqlAggregateSimple<double>::get_column_types() const {
  vector<string> decl;
  decl.push_back("DOUBLE");
  return decl;
}

//////////////// time_t /////////////////////
template<>
inline
void  MysqlAggregateSimple<time_t>::set_value(jetstream::Element * e, time_t val) const {
  e->set_t_val(val);
}

template<>
inline
time_t MysqlAggregateSimple<time_t>::get_value(jetstream::Element const * e) const {
  return e->t_val();
}

template<>
inline
void MysqlAggregateSimple<time_t>::set_db_value(shared_ptr<sql::PreparedStatement> pstmt, int &field_index, time_t& val) const {
  struct tm temptm;
  char timestring[30];
  gmtime_r(&val, &temptm);
  strftime(timestring, sizeof(timestring)-1, "%Y-%m-%d %H:%M:%S", &temptm);
  pstmt->setString(field_index, timestring);
}

template<>
inline
time_t MysqlAggregateSimple<time_t>::get_db_value(boost::shared_ptr<sql::ResultSet> resultset, int &col_index) const {
  string timestring = resultset->getString(col_index);
  struct tm temptm;
  //temptm.tm_isdst = -1; //not filled in by strptime. Make mktime figure it out
  if(strptime(timestring.c_str(), "%Y-%m-%d %H:%M:%S", &temptm) != NULL) {
    return timegm(&temptm);
  }
  else {
    LOG(FATAL)<<"Error in time conversion";
  }
}

template<>
inline
vector<string> MysqlAggregateSimple<time_t>::get_column_types() const {
  vector<string> decl;
  decl.push_back("DATETIME");
  return decl;
}

/*class MysqlAggregateSimple_Int: public MysqlAggregateSimple<int> {
  public:
    virtual void set_value(jetstream::Element * e, int val) {
      e->set_i_val(val)
    }
    virtual int get_value(jetstream::Element * e) {
      return e->i_val()
    }
    virtual void set_db_value(shared_ptr<sql::PreparedStatement> pstmt, int &field_index, int &val) {
      pstmt->setInt(field_index, val)
    }
    virtual int get_db_value(boost::shared_ptr<sql::ResultSet> resultset, int &col_index) {
      return resultset->getInt(col_index)
    }
    virtual vector<string> get_column_types() const {
      vector<string> decl;
      decl.push_back("INT");
      return decl;
    };

    virtual int combine(int a, int b) = 0;
    virtual string get_update_on_insert_sql() const = 0;
    virtual string get_select_clause_for_rollup() const =0;
}*/


template<class ValueType>
void MysqlAggregateSimple<ValueType>::merge_full_tuple_into(jetstream::Tuple &into, jetstream::Tuple const &update) const {
  jetstream::Element * const e_update = const_cast<jetstream::Tuple &>(update).mutable_e(tuple_indexes[0]);
  jetstream::Element * e_into = into.mutable_e(tuple_indexes[0]);

  set_value(e_into, combine(get_value(e_into), get_value(e_update)));
}

template<class ValueType>
void
MysqlAggregateSimple<ValueType>::set_value_for_insert_tuple ( shared_ptr<sql::PreparedStatement> pstmt,
                                                              const jetstream::Tuple &t,
                                                              int &field_index) {
  if(tuple_indexes.size() != 1) {
    LOG(FATAL) << "Wrong number of input tuple indexes for "<< name;
  }

  const jetstream::Element&  e = t.e(tuple_indexes[0]);
  ValueType val = get_value(&e);
  set_db_value(pstmt, field_index, val);
  field_index += 1;
}


template<class ValueType>
void MysqlAggregateSimple<ValueType>::populate_tuple_final(boost::shared_ptr<jetstream::Tuple> t, boost::shared_ptr<sql::ResultSet> resultset, int &column_index) const {
  ValueType val = get_db_value(resultset, column_index);
  ++column_index;
  jetstream::Element * elem = t->add_e();
  set_value(elem, val);
}

} /* cube */
} /* jetstream */


#endif /* end of include guard: AGGREGATE_SIMPLE_Q8TYGR7Q */
