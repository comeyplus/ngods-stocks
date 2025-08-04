# syntax=docker/dockerfile:1
FROM apache/airflow:3.0.3

# 设置环境变量
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"

# 切换到root用户安装系统依赖
USER root

# 安装系统级依赖和Java
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    python3-dev \
    libpq-dev \
    build-essential \
    default-jdk \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# 创建Spark目录
RUN mkdir -p ${SPARK_HOME}

# 下载并安装Spark
WORKDIR /tmp
RUN curl https://archive.apache.org/dist/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz -o spark-3.3.1-bin-hadoop3.tgz \
    && tar xvzf spark-3.3.1-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
    && rm spark-3.3.1-bin-hadoop3.tgz

# 下载Iceberg Spark Runtime
RUN curl https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.1.0/iceberg-spark-runtime-3.3_2.12-1.1.0.jar -Lo iceberg-spark-runtime-3.3_2.12-1.1.0.jar \
    && mv iceberg-spark-runtime-3.3_2.12-1.1.0.jar /opt/spark/jars

# 下载Java AWS SDK
RUN curl https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.17.247/bundle-2.17.247.jar -Lo bundle-2.17.247.jar \
    && mv bundle-2.17.247.jar /opt/spark/jars

# 下载URL connection client required for S3FileIO
RUN curl https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/2.17.247/url-connection-client-2.17.247.jar -Lo url-connection-client-2.17.247.jar \
    && mv url-connection-client-2.17.247.jar /opt/spark/jars

# 下载Postgres driver for Hive metastore
RUN curl https://repo1.maven.org/maven2/org/postgresql/postgresql/42.5.3/postgresql-42.5.3.jar -Lo pgsql.jar \
    && mv pgsql.jar /opt/spark/jars

# 复制Spark配置文件
COPY spark-defaults.conf ${SPARK_HOME}/conf/

# 设置Spark目录权限
RUN chmod -R 755 ${SPARK_HOME}

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