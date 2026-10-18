-- Runs once when the postgres container is first created.
CREATE DATABASE metabase;           -- Metabase application database
CREATE SCHEMA IF NOT EXISTS gold;   -- serving tables written by spark_jobs/export_postgres.py
