-- 为Superset创建数据库和用户
CREATE DATABASE superset;
CREATE USER superset WITH ENCRYPTED PASSWORD 'superset123';
GRANT ALL PRIVILEGES ON DATABASE superset TO superset;

-- 连接到superset数据库并授权
\c superset;
GRANT ALL ON SCHEMA public TO superset; 