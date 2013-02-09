USE mysql;

DROP FUNCTION IF EXISTS merge_histogram;
CREATE AGGREGATE FUNCTION merge_histogram RETURNS STRING SONAME 'libjsmysqludfs_merge.dylib';
DROP FUNCTION IF EXISTS merge_pair_histogram;
CREATE FUNCTION merge_pair_histogram RETURNS STRING SONAME 'libjsmysqludfs_merge.dylib';

DROP FUNCTION IF EXISTS merge_reservoir_sample;
CREATE AGGREGATE FUNCTION merge_reservoir_sample RETURNS STRING SONAME 'libjsmysqludfs_merge.dylib';
DROP FUNCTION IF EXISTS merge_pair_reservoir_sample;
CREATE FUNCTION merge_pair_reservoir_sample RETURNS STRING SONAME 'libjsmysqludfs_merge.dylib';

DROP FUNCTION IF EXISTS merge_sketch;
CREATE AGGREGATE FUNCTION merge_sketch RETURNS STRING SONAME 'libjsmysqludfs_merge.dylib';
DROP FUNCTION IF EXISTS merge_pair_sketch;
CREATE FUNCTION merge_pair_sketch RETURNS STRING SONAME 'libjsmysqludfs_merge.dylib';


/* copy dylib to /opt/local/lib/mysql5/mysql/plugin/ */

