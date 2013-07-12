
#ifndef __JetStream__mt_iter__
#define __JetStream__mt_iter__


#include "../cube_iterator.h"
#include "mt_cube.h"

namespace jetstream {
namespace cube {

class MasstreeCubeIteratorImpl : public CubeIteratorImpl {
  public:
    MasstreeCubeIteratorImpl(): final(true), num_cells(0){}
    
    MasstreeCubeIteratorImpl(boost::shared_ptr<const jetstream::cube::MasstreeCube> cube, bool final=true): cube(cube), final(final), num_cells(0)
    {} 


    virtual size_t numCells() {
      return num_cells;
    }

    virtual void increment() ;
  
    virtual bool equal(CubeIteratorImpl const& other) const ; 

    virtual boost::shared_ptr<jetstream::Tuple> dereference() const; 

    static boost::shared_ptr<MasstreeCubeIteratorImpl> end(); 

  private:
    static boost::shared_ptr<MasstreeCubeIteratorImpl> const impl_end;
    boost::shared_ptr<const MasstreeCube> const cube;
//    boost::shared_ptr<sql::ResultSet> res;
    bool const final;
    size_t const num_cells;
};

}
}

#endif /* defined(__JetStream__mt_iter__) */
