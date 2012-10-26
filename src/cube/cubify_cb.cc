#include <ctype.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "immutstr.h"


#define NUM_CB_FIELDS 22

#define CB_FIELD_BYTES_SERVED 11
#define CB_FIELD_CONTENT_TYPE 17

typedef int (*VerifyFunc)(char *, int pos);
typedef ImmutString *(*NamerFunc)(char *, int pos, int dim);

typedef struct AxisFuncs {
  VerifyFunc af_verify;
  NamerFunc af_namer;
  int af_pos;
} AxisFuncs;

/*
method
url
cont_type
ip_addr
bytes_served
response_code
domain
url_suffix
http_version
*/

/*----------------------------------------------------------------*/
char *
GetLine(FILE *f)
{
#define BUF_MAX 60000
  char buf[BUF_MAX];
  char *res;
  int len;

  if ((res = fgets(buf, BUF_MAX, f)) == NULL)
    return(NULL);
  if ((len = strlen(res)) == 0)
    return(NULL);
  if (buf[len-1] == '\n') {
    buf[len-1] = 0;
    len--;
  }
  else {
    fprintf(stderr, "encountered line longer than %d\n", BUF_MAX);
    exit(-1);
  }
  return(strdup(buf));
}
/*----------------------------------------------------------------*/
char *
FindWord(char *line, int pos)
{
  if (pos < 0 || line == NULL)
    return(NULL);
  while (pos >= 0) {
    /* skip spaces to get to start of word */
    while (*line != '\0' && isspace(*line))
      line++;
    if (*line == '\0')
      return(NULL);
    if (pos == 0)
      return(line);
    /* skip over word */
    while (*line != '\0' && (!isspace(*line)))
      line++;
    pos--;
  }
  return(NULL);
}
/*----------------------------------------------------------------*/
char *
DupWord(char *word)
{
  int len = 0;
  char *res;

  if (word == NULL)
    return(NULL);
  while (word[len] != '\0' && (!isspace(word[len])))
    len++;

  res = (char *) malloc(len+1);
  memcpy(res, word, len);
  res[len] = 0;
  return(res);
}
/*----------------------------------------------------------------*/
int
WordCount(char *s)
{
  int words = 0;
  
  while (FindWord(s, 0) != NULL) {
    words++;
    s = FindWord(s, 1);
  }
  return(words);
}
/*----------------------------------------------------------------*/
static int
Verify_BytesServed(char *line, int pos)
{
  char *res = FindWord(line, pos);
  int i;

  if (res == NULL || atoi(res) < 0) {
    if (res == NULL)
      fprintf(stderr, "encountered NULL in FindWord\n");
    else
      fprintf(stderr, "atoi bad for %s\n", res);
    return(FAILURE);
  }

  res = DupWord(res);

  for (i = 0; res[i] != '\0'; i++) {
    if (!isdigit(res[i])) {
      free(res);
      fprintf(stderr, "bad byteserved %s\n", res);
      return(FAILURE);
    }
  }

  free(res);
  return(SUCCESS);
}
/*----------------------------------------------------------------*/
static ImmutString *
Name_BytesServed(char *line, int pos, int dim)
{
#define LEN 20
  char *res = DupWord(FindWord(line, pos));
  ImmutString *im;
  unsigned long long val;
  char sortName[64];
  char hierName[500];
  char *walk;

  if (res == NULL)
    return(NULL);

  val = atoll(res);
  sprintf(sortName, "%020llu", val);

  if (strlen(sortName) != LEN) {
    fprintf(stderr, "name/len mismatch\n");
    exit(-1);
  }

  walk = hierName;
  for (int i = 0; i < LEN; i++) {
    for (int j = 0; j <= i; j++) {
      *walk++ = sortName[j];
    }
    if (i != LEN-1)
      *walk++ = '/';
  }
  *walk = 0;

  im = ImmutStringCreate(res, sortName, hierName, dim);
  free(res);
  return(im);
}
/*----------------------------------------------------------------*/
static int
Verify_ContentType(char *line, int pos)
{
  char *res = FindWord(line, pos);

  if (res == NULL) {
    fprintf(stderr, "failed to find content type: %s\n", line);
    return(FAILURE);
  }
  return(SUCCESS);
}
/*----------------------------------------------------------------*/
static ImmutString *
Name_ContentType(char *line, int pos, int dim)
{
  char *res = DupWord(FindWord(line, pos));
  char *temp;
  ImmutString *im;

  if (res == NULL) {
    fprintf(stderr, "failed to find content type: %s\n", line);
    return(NULL);
  }

  if ((temp = strchr(res, ';')) != NULL && temp != res)
    *temp = '\0';

  im = ImmutStringCreate(res, NULL, NULL, dim);
  free(res);
  return(im);
}
/*----------------------------------------------------------------*/
typedef struct ImmutArrayEnt {
  ImmutString **iae_ims;
  int iae_arraySize;
  int iae_count;
  int iae_hashVal;
  struct ImmutArrayEnt *iae_next;
  uint64_t iae_distance;		// just used for sorting
} ImmutArrayEnt;

#define IMMUTARRAY_BINS (1024*1024)
static ImmutArrayEnt *immutArrayBins[IMMUTARRAY_BINS];

static int numIaesAlloc;
static int numIaesUsed;
static ImmutArrayEnt **iaeArray;
/*----------------------------------------------------------------*/
static int
ImmutArrayCompare_Count(const void *pa, const void *pb)
{
  const ImmutArrayEnt *a = *((const ImmutArrayEnt **) pa);
  const ImmutArrayEnt *b = *((const ImmutArrayEnt **) pb);

  /* sort in descending order */
  if (a->iae_count < b->iae_count)
    return(1);
  if (a->iae_count > b->iae_count)
    return(-1);

  /* sort in ascending order */
  if (a->iae_distance < b->iae_distance)
    return(-1);
  if (a->iae_distance > b->iae_distance)
    return(1);
  return(0);
}
/*----------------------------------------------------------------*/
static int
ImmutArrayCompare_Distance(const void *pa, const void *pb)
{
  const ImmutArrayEnt *a = *((const ImmutArrayEnt **) pa);
  const ImmutArrayEnt *b = *((const ImmutArrayEnt **) pb);

  /* sort in ascending order */
  if (a->iae_distance < b->iae_distance)
    return(-1);
  if (a->iae_distance > b->iae_distance)
    return(1);

  /* sort in descending order */
  if (a->iae_count < b->iae_count)
    return(1);
  if (a->iae_count > b->iae_count)
    return(-1);

  return(0);
}
/*----------------------------------------------------------------*/
static void
ImmutArraySort(int sortCount, int sortDistance)
{
  /* if asking for count, do that first, then distance (if requested)
     as tiebreaker. otherwise, use distance first, and count as
     tiebreaker */

  if (sortDistance) {
    for (int i = 0; i < numIaesUsed; i++) {
      ImmutArrayEnt *iae = iaeArray[i];
      iae->iae_distance =0;
      for (int j = 0; j < iae->iae_arraySize; j++) {
	uint64_t ind = iae->iae_ims[j]->is_index;
	iae->iae_distance += ind * ind;
      }
    }
  }

  if (sortCount)
    qsort(iaeArray, numIaesUsed, sizeof(ImmutArrayEnt *),
	  ImmutArrayCompare_Count);  
  else if (sortDistance)
    qsort(iaeArray, numIaesUsed, sizeof(ImmutArrayEnt *),
	  ImmutArrayCompare_Distance);
}
/*----------------------------------------------------------------*/
static int
ImmutArrayHash(ImmutString *ims, int numElem)
{
  int res = 5381;
  
  for (; numElem > 0; numElem--, ims++) {
    char *s = (char *) ims;
    int i;
    for (i = 0; i < (int) sizeof(ImmutString *); i++)
      res += (res << 5) + s[i];
  }

  return(res);
}
/*----------------------------------------------------------------*/
#ifndef MAX
#define MAX(a,b) (((a) > (b)) ? (a) : (b))
#endif
/*----------------------------------------------------------------*/
static ImmutArrayEnt *
ImmutArrayFindBack(ImmutString **ims, int numElem, int doAdd)
{
  int hash = ImmutArrayHash(*ims, numElem);
  int bin = hash & (IMMUTARRAY_BINS-1);
  ImmutArrayEnt *walk;

  for (walk = immutArrayBins[bin]; walk != NULL; walk = walk->iae_next) {
    if (walk->iae_hashVal != hash)
      continue;
    if (memcmp(walk->iae_ims, ims, numElem * sizeof(ImmutString *)) == 0)
      return(walk);
  }

  if (!doAdd)
    return(NULL);

  walk = (ImmutArrayEnt *) calloc(1, sizeof(ImmutArrayEnt));
  walk->iae_ims = (ImmutString **) calloc(numElem, sizeof(ImmutString *));
  walk->iae_arraySize = numElem;
  memcpy(walk->iae_ims, ims, numElem * sizeof(ImmutString *));
  walk->iae_hashVal = hash;
  walk->iae_next = immutArrayBins[bin];
  immutArrayBins[bin] = walk;

  if (numIaesUsed >= numIaesAlloc) {
    numIaesAlloc = MAX(numIaesAlloc * 2, 16);
    iaeArray = (ImmutArrayEnt **) 
      realloc(iaeArray, numIaesAlloc * sizeof(ImmutArrayEnt *));
  }
  iaeArray[numIaesUsed++] = walk;

  return(walk);
}
/*----------------------------------------------------------------*/
ImmutArrayEnt *
ImmutArrayFind(ImmutString **ims, int numElem)
{
  return(ImmutArrayFindBack(ims, numElem, FALSE));
}
/*----------------------------------------------------------------*/
static void
ImmutArrayAdd(ImmutString **ims, int numElem)
{
  ImmutArrayEnt *ent = ImmutArrayFindBack(ims, numElem, TRUE);
  ent->iae_count++;
}
/*----------------------------------------------------------------*/

static AxisFuncs axisFuncs[] = {
  {Verify_BytesServed, Name_BytesServed, CB_FIELD_BYTES_SERVED},
  {Verify_ContentType, Name_ContentType, CB_FIELD_CONTENT_TYPE}
};

#define NUM_AXES ((int)(sizeof(axisFuncs)/sizeof(axisFuncs[0])))
/*----------------------------------------------------------------*/
int
main(int argc, char *argv[])
{
  char *line = NULL;
  int i;

  while (1) {
    int words;
    int skip;
    ImmutString *ims[NUM_AXES];

    free(line);
    if ((line = GetLine(stdin)) == NULL)
      break;
    if ((words = WordCount(line)) != NUM_CB_FIELDS) {
      fprintf(stderr, "expected %d words, had %d: %s\n",
	      NUM_CB_FIELDS, words, line);
      continue;
    }

    skip = FALSE;
    for (i = 0; i < NUM_AXES; i++) {
      AxisFuncs *af = &axisFuncs[i];
      if (af->af_verify(line, af->af_pos) != SUCCESS) {
	fprintf(stderr, "failed axis %d on line %s\n", i, line);
	skip = TRUE;
	break;
      }
    }

    if (skip)
      continue;
    
    for (i = 0; i < NUM_AXES; i++) {
      AxisFuncs *af = &axisFuncs[i];
      if ((ims[i] = af->af_namer(line, af->af_pos, i)) == NULL) {
	fprintf(stderr, "failed naming axis %d on line %s\n", i, line);
	skip = TRUE;
	break;
      }
      ims[i]->is_count++;
    }

    if (skip)
      continue;

    ImmutArrayAdd(ims, NUM_AXES);

  }
  
  for (i = 0; i < NUM_AXES; i++)
    ImmutStringSortDimension(i, ST_HIER);
  ImmutArraySort(TRUE, TRUE);

  for (int j = 0; j < numIaesUsed; j++) {
    ImmutArrayEnt *iae = iaeArray[j];

    printf("%d ", iae->iae_count);
    printf("(");
    for (int i = 0; i < iae->iae_arraySize; i++) {
      if (i > 0)
	printf(", ");
      printf("%d", iae->iae_ims[i]->is_index);
    }
    printf(") ");

    printf("(");
    for (int i = 0; i < iae->iae_arraySize; i++) {
      if (i > 0)
	printf(", ");
      printf("%s", iae->iae_ims[i]->is_hierName);
    }
    printf(")\n");
  }
    
  
}
/*----------------------------------------------------------------*/
