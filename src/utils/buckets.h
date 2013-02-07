#ifndef JS_BUCKET_H
#define JS_BUCKET_H

#include <vector>

// if you want to examine the output of a vector, uncomment one of the "bucket"
// and "print_vec" calls in the commented-out main, and run the following:
// ./a.out | tr -d '[]:' | tr -d '[[:alpha:]]' | tr '\n' ' ' | tr -s ' ' | tr ' ' '\n' | tail +3 | sort -n | less

// Fills up a sequence with values in geometric succession in the range
// [1, max]. For values of n larger than log_2(max), the sequence wraps around
// to a small number and roughly splits the arithmetic difference between each
// previously generated, consecutive, and geometrically spaced value. In this
// case, the values are not in sorted order, but are in the order in which they
// were generated.
void buckets(unsigned int n, std::vector<unsigned int> &sequence, unsigned int max);

// Same as above, with max = UINT_MAX;
void buckets(unsigned int n, std::vector<unsigned int> &sequence);

#endif
