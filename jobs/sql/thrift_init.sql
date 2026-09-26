-- One-time setup for the Thrift server's saved metadata (kept in the thrift_meta volume).
-- Views are saved queries, so they always see newly ingested bronze quarters.
CREATE DATABASE IF NOT EXISTS bronze;
CREATE OR REPLACE VIEW bronze.orig AS SELECT * FROM parquet.`s3a://cecl-lake/bronze/orig`;
CREATE OR REPLACE VIEW bronze.perf AS SELECT * FROM parquet.`s3a://cecl-lake/bronze/perf`;

-- DBeaver's tree only lists the default catalog, not the Iceberg catalog `lake`. These views make the
-- silver tables visible there as silver.<name>. They save the column list at creation time, so re-run
-- this file after any change to a silver table's columns.
CREATE DATABASE IF NOT EXISTS silver;
CREATE OR REPLACE VIEW silver.loans AS SELECT * FROM lake.silver.loans;
CREATE OR REPLACE VIEW silver.loan_performance AS SELECT * FROM lake.silver.loan_performance;
CREATE OR REPLACE VIEW silver.quality_report AS SELECT * FROM lake.silver.quality_report;
CREATE OR REPLACE VIEW silver.ref_zero_balance_codes AS SELECT * FROM lake.silver.ref_zero_balance_codes;
CREATE OR REPLACE VIEW silver.build_manifest AS SELECT * FROM lake.silver.build_manifest;
