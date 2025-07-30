# syntax=docker/dockerfile:1
FROM apache/airflow:3.0.3

# 切换到root用户安装系统依赖
USER root

# 安装系统级依赖
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    python3-dev \
    libpq-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# 切换回airflow用户
USER airflow

# 设置工作目录
WORKDIR /opt/airflow

# 复制pyproject.toml和uv.lock文件
COPY pyproject.toml .
COPY uv.lock* .

# 安装uv并同步依赖
RUN pip install --no-cache-dir uv \
    && uv sync --frozen

# 设置虚拟环境路径
ENV PATH="/opt/airflow/.venv/bin:${PATH}" 