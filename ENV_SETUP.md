# 环境变量配置说明

## 概述

为了安全起见，本项目已将敏感信息（如密码、密钥等）从配置文件中提取到环境变量中。

## 设置步骤

### 1. 复制示例文件

```bash
cp env.local.sample env.local
```

### 2. 编辑环境变量文件

编辑 `env.local` 文件，将所有的 `YOUR_*` 占位符替换为实际的值：

```bash
# 示例
# 将 YOUR_AIRFLOW_PASSWORD 替换为实际的 Airflow 密码
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:your_actual_password@postgres/airflow
```

### 3. 生成必要的密钥

对于某些服务，您需要生成新的密钥：

#### Airflow Fernet Key
```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

#### JWT Secret
```bash
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

#### Superset Secret Key
```bash
python -c "import secrets; print(secrets.token_urlsafe(32))"
```

### 4. 启动服务

确保在启动 Docker Compose 之前，环境变量文件已经正确配置：

```bash
# 方式一：使用启动脚本（推荐）
./start.sh

# 方式二：手动启动
docker-compose --env-file env.local up -d
```

**注意**：启动脚本会自动处理 SQL 文件中的环境变量替换。

## SQL 文件环境变量处理

项目中的 SQL 文件（如 `docker/postgres-init/init-superset-db.sql`）包含环境变量占位符：

```sql
CREATE USER superset WITH ENCRYPTED PASSWORD '${SUPERSET_DATABASE_PASSWORD}';
CREATE USER airflow WITH ENCRYPTED PASSWORD '${AIRFLOW_DB_PASSWORD}';
```

启动脚本会自动：
1. 读取 `env.local` 文件中的环境变量
2. 替换 SQL 文件中的 `${VARIABLE_NAME}` 占位符
3. 启动 Docker Compose 服务
4. 停止时恢复原始 SQL 文件

## 环境变量说明

### Airflow 相关
- `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`: Airflow 数据库连接字符串
- `AIRFLOW__CELERY__RESULT_BACKEND`: Celery 结果后端连接字符串
- `AIRFLOW__CORE__FERNET_KEY`: Airflow 加密密钥
- `AIRFLOW__API_AUTH__JWT_SECRET`: JWT 认证密钥
- `AIRFLOW__WEBSERVER__SECRET_KEY`: Web 服务器密钥

### MinIO 相关
- `MINIO_ROOT_USER`: MinIO 根用户
- `MINIO_ROOT_PASSWORD`: MinIO 根密码
- `AWS_ACCESS_KEY_ID`: AWS 访问密钥 ID
- `AWS_SECRET_ACCESS_KEY`: AWS 访问密钥

### PostgreSQL 相关
- `POSTGRES_PASSWORD`: PostgreSQL 密码
- `POSTGRES_USER`: PostgreSQL 用户
- `POSTGRES_DB`: PostgreSQL 数据库名

### Superset 相关
- `SUPERSET_ADMIN_USERNAME`: Superset 管理员用户名
- `SUPERSET_ADMIN_PASSWORD`: Superset 管理员密码
- `SUPERSET_SECRET_KEY`: Superset 密钥

## 安全注意事项

1. **永远不要提交 `env.local` 文件到 Git**
2. **定期更换密码和密钥**
3. **在生产环境中使用更强的密码**
4. **确保环境变量文件的权限设置正确**

## 故障排除

如果遇到环境变量未加载的问题，请检查：

1. 环境变量文件是否存在
2. 环境变量名称是否正确
3. 环境变量值是否包含特殊字符（如 `$`、`@` 等）

## 示例配置

完整的 `env.local` 文件示例：

```bash
# Airflow 配置
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow123@postgres/airflow
AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:airflow123@postgres/airflow
AIRFLOW__CELERY__BROKER_URL=redis://:@redis:6379/0
AIRFLOW__CORE__FERNET_KEY=your_generated_fernet_key
AIRFLOW__API_AUTH__JWT_SECRET=your_generated_jwt_secret
AIRFLOW__WEBSERVER__SECRET_KEY=your_generated_webserver_secret

# MinIO 配置
MINIO_ROOT_USER=minio
MINIO_ROOT_PASSWORD=your_minio_password
AWS_ACCESS_KEY_ID=minio
AWS_SECRET_ACCESS_KEY=your_minio_secret

# PostgreSQL 配置
POSTGRES_PASSWORD=your_postgres_password
POSTGRES_USER=ngods
POSTGRES_DB=ngods

# Superset 配置
SUPERSET_ADMIN_USERNAME=admin
SUPERSET_ADMIN_PASSWORD=your_superset_password
SUPERSET_SECRET_KEY=your_superset_secret_key
``` 