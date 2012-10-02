#include "dimension_flat.h"
#include <boost/algorithm/string/join.hpp>
#include <boost/lexical_cast.hpp>

using namespace std;
using namespace jetstream::cube;


string MysqlDimensionFlat::get_select_clause_for_rollup(unsigned int const level) const {
  string sel;
  if(0==level) {
    sel = boost::algorithm::join(get_default_value(), ", ");
  }
  else {
     sel = boost::algorithm::join(get_column_names(), ", ");
  }

  return sel+", "+boost::lexical_cast<std::string>(level);

}
    
string MysqlDimensionFlat::get_groupby_clause_for_rollup(unsigned int const level) const {
  if (0 == level) {
    return "";
  }
  return boost::algorithm::join(get_column_names(), ", ");
}
