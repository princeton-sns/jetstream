USE mysql;

DROP FUNCTION IF EXISTS merge_histogram;
CREATE AGGREGATE FUNCTION merge_histogram RETURNS STRING SONAME 'libjsmysqludfs_merge_histogram.dylib';
