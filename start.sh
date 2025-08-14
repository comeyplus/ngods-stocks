#!/bin/bash

# NGODS Stocks 启动脚本

echo "=========================================="
echo "NGODS Stocks 启动脚本"
echo "=========================================="

# 检查环境变量文件
if [ ! -f "env.local" ]; then
    echo "❌ env.local 文件不存在"
    echo "请先运行: cp env.local.sample env.local"
    echo "然后编辑 env.local 文件设置环境变量"
    exit 1
fi

echo "✅ 环境变量文件存在"

# 处理数据库初始化脚本
echo "处理数据库初始化脚本..."
if [ -f "docker/postgres-init/init-superset-db.sql" ]; then
    # 备份原文件
    cp docker/postgres-init/init-superset-db.sql docker/postgres-init/init-superset-db.sql.backup
    
    # 加载环境变量
    source env.local
    
    # 替换环境变量
    sed "s/\${SUPERSET_DATABASE_PASSWORD}/$SUPERSET_DATABASE_PASSWORD/g" docker/postgres-init/init-superset-db.sql.backup | \
    sed "s/\${AIRFLOW_DB_PASSWORD}/$AIRFLOW_DB_PASSWORD/g" > docker/postgres-init/init-superset-db.sql
    
    echo "✅ 数据库初始化脚本已更新"
else
    echo "⚠ 数据库初始化脚本不存在"
fi

# 启动服务
echo "启动 Docker Compose 服务..."
docker-compose --env-file env.local up -d

echo ""
echo "=========================================="
echo "服务启动完成！"
echo "=========================================="
echo ""
echo "服务访问地址："
echo "- Airflow: http://localhost:8080"
echo "- Superset: http://localhost:8088"
echo "- Metabase: http://localhost:3030"
echo "- Cube: http://localhost:4000"
echo "- MinIO: http://localhost:9001"
echo ""
echo "查看服务状态："
echo "docker-compose ps"
echo ""
echo "查看日志："
echo "docker-compose logs -f" 