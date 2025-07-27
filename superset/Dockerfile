# 基于官方 Superset 镜像，添加 Trino 支持
FROM apache/superset:3.1.0

# 切换到 root 用户以安装包
USER root

# 安装 Trino 驱动程序和其他数据库驱动
RUN pip install --no-cache-dir \
    sqlalchemy-trino \
    trino[sqlalchemy] \
    psycopg2-binary

# 切换回 superset 用户
USER superset 