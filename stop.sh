#!/bin/bash

# NGODS Stocks 停止脚本

echo "=========================================="
echo "NGODS Stocks 停止脚本"
echo "=========================================="

# 停止服务
echo "停止 Docker Compose 服务..."
docker-compose down

# 恢复数据库初始化脚本
echo "恢复数据库初始化脚本..."
if [ -f "docker/postgres-init/init-superset-db.sql.backup" ]; then
    mv docker/postgres-init/init-superset-db.sql.backup docker/postgres-init/init-superset-db.sql
    echo "✅ 数据库初始化脚本已恢复"
else
    echo "⚠ 数据库初始化脚本备份不存在"
fi

echo ""
echo "=========================================="
echo "服务已停止！"
echo "==========================================" 