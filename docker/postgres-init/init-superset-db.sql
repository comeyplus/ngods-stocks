-- 为Superset创建数据库和用户
CREATE DATABASE superset;
CREATE USER superset WITH ENCRYPTED PASSWORD '${SUPERSET_DATABASE_PASSWORD}';
GRANT ALL PRIVILEGES ON DATABASE superset TO superset;

-- 连接到superset数据库并授权
\c superset;
GRANT ALL ON SCHEMA public TO superset;

-- 为Airflow创建数据库和用户
\c postgres;
CREATE DATABASE airflow;
CREATE USER airflow WITH ENCRYPTED PASSWORD '${AIRFLOW_DB_PASSWORD}';
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

-- 连接到airflow数据库并授权
\c airflow;
GRANT ALL ON SCHEMA public TO airflow;

-- -- 启用pg_duckdb扩展
-- CREATE EXTENSION IF NOT EXISTS duckdb;

-- 为ngods数据库启用pg_duckdb扩展
-- \c ngods;
-- CREATE EXTENSION IF NOT EXISTS duckdb; 
