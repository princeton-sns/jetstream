#include "js_defs.h"

namespace  jetstream {

/* This code is taken from the wikipedia page on the Jenkins Hash algorithms
and from http://www.burtleburtle.net/bob/hash/doobs.html .  The code is in the 
public domain and is therefore usable without legal incumbrence.  --ASR, 12/5/12
*/
u_int32_t jenkins_one_at_a_time_hash(const char *key, size_t len)
{
    u_int32_t hash, i;
    for(hash = i = 0; i < len; ++i)
    {
        hash += key[i];
        hash += (hash << 10);
        hash ^= (hash >> 6);
    }
    hash += (hash << 3);
    hash ^= (hash >> 11);
    hash += (hash << 15);
    return hash;
}

}