USE mysql;

DROP FUNCTION IF EXISTS merge_histogram;
CREATE AGGREGATE FUNCTION merge_histogram RETURNS STRING SONAME 'libjsmysqludfs_merge_histogram.dylib';

/* copy dylib to /opt/local/lib/mysql5/mysql/plugin/ */

