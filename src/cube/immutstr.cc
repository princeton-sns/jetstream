/*
 * just some immutable strings with lookup information
 *
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "immutstr.h"

#define NUM_BINS (1024*1024)
#define BINS_MASK (NUM_BINS-1)

static ImmutString *bins[NUM_BINS];

typedef struct DimInfo {
  ImmutString *di_head;
  int di_currIndex;
} DimInfo;

static DimInfo *dimChains;
static int numDimChains;

/*----------------------------------------------------------------*/
static int
ImmutStringCompare_Pop(const void *pa, const void *pb)
{
  const ImmutString *a = *((const ImmutString **) pa);
  const ImmutString *b = *((const ImmutString **) pb);

  /* reverse order */
  if (a->is_count < b->is_count)
    return(1);
  if (a->is_count > b->is_count)
    return(-1);
  return(0);
}
/*----------------------------------------------------------------*/
static int
ImmutStringCompare_Name(const void *pa, const void *pb)
{
  const ImmutString *a = *((const ImmutString **) pa);
  const ImmutString *b = *((const ImmutString **) pb);

  return(strcmp(a->is_sortName != NULL ? a->is_sortName : a->is_str, 
		b->is_sortName != NULL ? b->is_sortName : b->is_str));
}
/*----------------------------------------------------------------*/
static int
HierStrCmp(const char *pa, const char *pb)
{
  const unsigned char *a = (const unsigned char *) pa;
  const unsigned char *b = (const unsigned char *) pb;

  if (strcmp((const char *) a, ".") == 0) {
    if (strcmp((const char *) b, ".") == 0)
      return(0);
    return(-1);
  }
  if (strcmp((const char *) b, ".") == 0)
    return(1);

  for (; *a != 0 || *b != 0; a++, b++) {
    if (*a == 0)
      return(-1);
    if (*b == 0)
      return(1);
    if (*a == *b)
      continue;
    if (*a == '/')
      return(-1);
    if (*b == '/')
      return(1);
    if (*a < *b)
      return(-1);
    return(1);
  }
  return(0);
}
/*----------------------------------------------------------------*/
static int
ImmutStringCompare_Hier(const void *pa, const void *pb)
{
  const ImmutString *a = *((const ImmutString **) pa);
  const ImmutString *b = *((const ImmutString **) pb);

  return(HierStrCmp(a->is_hierName != NULL ? a->is_hierName : a->is_str, 
		    b->is_hierName != NULL ? b->is_hierName : b->is_str));
}
/*----------------------------------------------------------------*/
void
ImmutStringSortDimension(int whichDim, SortType sortType)
{
  /* currently either a pop sort or a name sort, based on flag */

  ImmutString **tempRay;
  int count;
  ImmutString *walk;
  int i;

  if (whichDim < 0 || whichDim >= numDimChains)
    exit(-1);
  if ((count = dimChains[whichDim].di_currIndex) < 1)
    return;

  tempRay = (ImmutString **) calloc(count, sizeof(ImmutString *));
  for (walk = dimChains[whichDim].di_head, i = 0; 
       i < count; 
       i++, walk = walk->is_dimNext) {
    tempRay[i] = walk;
  }

  if (sortType == ST_POP)
    qsort(tempRay, count, sizeof(ImmutString *), ImmutStringCompare_Pop);
  else if (sortType == ST_NAME)
    qsort(tempRay, count, sizeof(ImmutString *), ImmutStringCompare_Name);
  else if (sortType == ST_HIER)
    qsort(tempRay, count, sizeof(ImmutString *), ImmutStringCompare_Hier);
  else {
    fprintf(stderr, "bad sort type\n");
    exit(-1);
  }

  for (i = 0; i < count; i++) {
    tempRay[i]->is_index = i;
    if (tempRay[i]->is_hierName != NULL)
      printf("%s\n", tempRay[i]->is_hierName);
  }

  free(tempRay);
}
/*----------------------------------------------------------------*/
static int
Hash(const char *s, int d)
{
  int res = 5381 + d;
  char c;
  
  while ((c = *s++) != 0)
    res += (res << 5) + c;

  return(res);
}
/*----------------------------------------------------------------*/
static ImmutString *
ImmutStringCreateBack(const char *s, const char *sortName,
		      const char *hierName, int dim, int doCreate)
{
  int h = Hash(s, dim);
  int bin = h & BINS_MASK;
  ImmutString *walk;

  for (walk = bins[bin]; walk != NULL; walk = walk->is_next) {
    if (walk->is_strHash == h && strcmp(s, walk->is_str) == 0)
      return(walk);
  }

  if (!doCreate)
    return(NULL);

  walk = (ImmutString *) calloc(1, sizeof(ImmutString));
  walk->is_str = strdup(s);
  if (sortName != NULL)
    walk->is_sortName = strdup(sortName);
  if (hierName != NULL)
    walk->is_hierName = strdup(hierName);
  walk->is_dimension = dim;
  walk->is_strHash = h;
  walk->is_next = bins[bin];
  bins[bin] = walk;
  if (dim >= 0) {
    if (dim >= numDimChains) {
      int i;
      dimChains = (DimInfo *) realloc(dimChains, 
				      (dim+1) * sizeof(DimInfo));
      for (i = numDimChains; i < dim+1; i++)
	memset(&dimChains[i], 0, sizeof(DimInfo));
      numDimChains = dim+1;
    }
    walk->is_index = dimChains[dim].di_currIndex++;
    walk->is_dimNext = dimChains[dim].di_head;
    dimChains[dim].di_head = walk;
  }
  return(walk);
}
/*----------------------------------------------------------------*/
ImmutString *
ImmutStringCreate(const char *s, const char *sortName, 
		  const char *hierName, int dim)
{
  return(ImmutStringCreateBack(s, sortName, hierName, dim, TRUE));
}
/*----------------------------------------------------------------*/
char *
ImmutStringCreateStr(const char *s, const char *sortName, 
		     const char *hierName, int dim)
{
  return(ImmutStringCreate(s, sortName, hierName, dim)->is_str);
}
/*----------------------------------------------------------------*/
ImmutString *
ImmutStringFind(const char *s, int dim)
{
  return(ImmutStringCreateBack(s, NULL, NULL, dim, FALSE));
}
/*----------------------------------------------------------------*/
char *
ImmutStringFindStr(const char *s, int dim)
{
  ImmutString *res;
  if ((res = ImmutStringCreateBack(s, NULL, NULL, dim, FALSE)) == NULL)
    return(NULL);
  return(res->is_str);
}
/*----------------------------------------------------------------*/
