-- Runs automatically when spark_sql.sh starts. Gives the bronze Parquet folders
-- table-like names (source_quarter shows up as a normal column).
CREATE OR REPLACE TEMPORARY VIEW bronze_orig USING parquet OPTIONS (path 's3a://cecl-lake/bronze/orig');
CREATE OR REPLACE TEMPORARY VIEW bronze_perf USING parquet OPTIONS (path 's3a://cecl-lake/bronze/perf');
