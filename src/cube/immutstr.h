/*
 * immutable string headers
 */

#ifndef _IMMUTSTR_H_
#define _IMMUTSTR_H_

typedef struct ImmutString {
  char *is_str;
  char *is_sortName;	 /* used for sorting */
  char *is_hierName;	 /* hierarchical name */
  int is_dimension;	 /* index # of dimension, -1 for don't care */
  char *is_memo;	 /* any memoized result */
  int is_index;		 /* index #, if desired */
  int is_count;		 /* count for pop count */
  /* items below this line are internal bookkeeping */
  int is_strHash;
  struct ImmutString *is_next;	/* next in bin */
  struct ImmutString *is_dimNext; /* next for this dimension */
} ImmutString;

ImmutString *ImmutStringCreate(const char *s, const char *sortName, 
			       const char *hierName, int dim);
char *ImmutStringCreateStr(const char *s, const char *sortName, 
			   const char *hierName, int dim);
ImmutString *ImmutStringFind(const char *s, int dim);
char *ImmutStringFindStr(const char *s, int dim);

typedef enum SortType {
  ST_POP,
  ST_NAME,
  ST_HIER
} SortType;

void ImmutStringSortDimension(int whichDim, SortType sortType);

#ifndef FAILURE
#define FAILURE 1
#endif

#ifndef SUCCESS
#define SUCCESS 0
#endif

#ifndef FALSE
#define FALSE 0
#endif

#ifndef TRUE
#define TRUE 1
#endif

#endif
