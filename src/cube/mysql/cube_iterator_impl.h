#ifndef CUBE_ITERATOR_OLV5GVPJ
#define CUBE_ITERATOR_OLV5GVPJ

#include "../cube_iterator.h"
#include "mysql_cube.h"
#include <cppconn/resultset.h>
#include <boost/make_shared.hpp>

namespace jetstream {
namespace cube {
  
class MysqlCubeIteratorImpl : public CubeIteratorImpl {
  public:
    MysqlCubeIteratorImpl(): final(true), num_cells(0){}
    
    MysqlCubeIteratorImpl(boost::shared_ptr<const jetstream::cube::MysqlCube> cube, boost::shared_ptr<sql::ResultSet> rs, bool final=true): cube(cube), res(rs), final(final), num_cells(rs->rowsCount()){};


    virtual size_t numCells() {
      return num_cells;
    }

    virtual void increment() {
      if(res && !res->next())
      {
        boost::shared_ptr<sql::ResultSet> rs;
        res=rs; 
      }
    }
  
    virtual bool equal(CubeIteratorImpl const& other) const  {
      try
      {
        MysqlCubeIteratorImpl const& otherm = dynamic_cast<MysqlCubeIteratorImpl const&>(other);
        if(!res)
        {
          return !otherm.res;
        }
        return (this->res == otherm.res && this->res->getRow() == otherm.res->getRow());

      }
      catch (const std::bad_cast& e)
      {
        return false;
      }
    }

    virtual boost::shared_ptr<jetstream::Tuple> dereference() const {
      if(!res) {
        boost::shared_ptr<jetstream::Tuple> tup;
        return tup;
      }
      return cube->make_tuple_from_result_set(res, final);
    }

    static boost::shared_ptr<MysqlCubeIteratorImpl> end() {
        return MysqlCubeIteratorImpl::impl_end;
    }

  private:
    static boost::shared_ptr<MysqlCubeIteratorImpl> const impl_end;
    boost::shared_ptr<const MysqlCube> const cube;
    boost::shared_ptr<sql::ResultSet> res;
    bool const final;
    size_t const num_cells;
};

}
}

#endif /* end of include guard: CUBE_ITERATOR_OLV5GVPJ */
