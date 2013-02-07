#include "buckets.h"
#include <climits>

#define MIN(a, b) ((a) < (b) ? (a) : (b))

void buckets(unsigned int n, std::vector<unsigned int> &sequence) {
  buckets(n, sequence, UINT_MAX);
}

void buckets(unsigned int n, std::vector<unsigned int> &sequence, unsigned int max) {
  // return 1, 2, ..., 2^31, 3, 6, ..., 3 * 2^30, wrapping around before max
  // TODO how to indicate error?

  const int bits = 32; // FIXME portability
  max = MIN(max, UINT_MAX);

  int base = 1;
  int exp = 0;
  while (n-- > 0) {
    sequence.push_back(base * (1 << exp++)); 

    if (sequence.back() > (max >> 1)) {
      // the next element is always twice the previous element (until wrapping
      // around), so we wrap around now to avoid overflow. 

      //std::cout << "ending subsequence: " << base << " * 2^" << exp - 1 << std::endl;
      base += 2; // next odd number
      exp = 0;
    }
  }
}


#include <iostream>
static void print_vec(std::vector<unsigned int> v) {
  std::cout << "length: " << v.size() << ' ';
  std::cout << '[';
  for (std::vector<unsigned int>::const_iterator i = v.begin(); i != v.end(); ++i)
    std::cout << *i << ' ';
  std::cout << ']' << std::endl;
}

int main(void) {
  std::vector<unsigned int> sequence;

  //buckets(10, sequence);
  //print_vec(sequence);

  //sequence.clear();
  //buckets(32, sequence);
  //print_vec(sequence);

  //sequence.clear();
  //buckets(80, sequence);
  //print_vec(sequence);

  //sequence.clear();
  //buckets(160, sequence);
  //print_vec(sequence);

  //sequence.clear();
  buckets(400, sequence);
  print_vec(sequence);
}
