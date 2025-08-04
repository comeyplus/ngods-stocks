#!/usr/bin/env python3
"""
测试Spark连接和命名空间
"""

import subprocess
import sys
import time

def test_spark_connection():
    """测试Spark连接"""
    print("=" * 50)
    print("测试Spark连接")
    print("=" * 50)
    
    # 测试1: 检查aio服务是否可访问
    print("1. 检查aio服务连接...")
    try:
        result = subprocess.run(
            ["curl", "-f", "http://aio:8061"],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode == 0:
            print("✅ aio服务可访问")
        else:
            print("❌ aio服务不可访问")
            return False
    except Exception as e:
        print(f"❌ 连接aio服务失败: {e}")
        return False
    
    # 测试2: 检查Kyuubi连接
    print("2. 检查Kyuubi连接...")
    try:
        result = subprocess.run(
            ["curl", "-f", "http://aio:10008"],
            capture_output=True,
            text=True,
            timeout=10
        )
        if result.returncode == 0:
            print("✅ Kyuubi服务可访问")
        else:
            print("❌ Kyuubi服务不可访问")
    except Exception as e:
        print(f"❌ 连接Kyuubi失败: {e}")
    
    # 测试3: 使用spark-sql测试命名空间
    print("3. 测试Spark SQL和命名空间...")
    spark_sql_cmd = [
        "spark-sql",
        "--master", "spark://aio:7077",
        "--conf", "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "--conf", "spark.sql.catalog.warehouse=org.apache.iceberg.spark.SparkCatalog",
        "--conf", "spark.sql.defaultCatalog=warehouse",
        "--conf", "spark.sql.catalog.warehouse.catalog-impl=org.apache.iceberg.jdbc.JdbcCatalog",
        "--conf", "spark.sql.catalog.warehouse.uri=jdbc:postgresql://postgres:5432/ngods?user=ngods&password=ngods",
        "--conf", "spark.sql.catalog.warehouse.jdbc.useSSL=false",
        "--conf", "spark.sql.catalog.warehouse.jdbc.user=ngods",
        "--conf", "spark.sql.catalog.warehouse.jdbc.password=ngods",
        "--conf", "spark.sql.catalog.warehouse.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
        "--conf", "spark.sql.catalog.warehouse.warehouse=s3a://warehouse",
        "--conf", "spark.sql.catalog.warehouse.s3.endpoint=http://minio:9000",
        "-e", "SHOW NAMESPACES;"
    ]
    
    try:
        result = subprocess.run(
            spark_sql_cmd,
            capture_output=True,
            text=True,
            timeout=30
        )
        print(f"Spark SQL输出: {result.stdout}")
        if result.returncode == 0:
            print("✅ Spark SQL连接成功")
        else:
            print(f"❌ Spark SQL连接失败: {result.stderr}")
    except Exception as e:
        print(f"❌ Spark SQL测试失败: {e}")
    
    # 测试4: 创建命名空间
    print("4. 创建必要的命名空间...")
    create_namespace_cmd = [
        "spark-sql",
        "--master", "spark://aio:7077",
        "--conf", "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "--conf", "spark.sql.catalog.warehouse=org.apache.iceberg.spark.SparkCatalog",
        "--conf", "spark.sql.defaultCatalog=warehouse",
        "--conf", "spark.sql.catalog.warehouse.catalog-impl=org.apache.iceberg.jdbc.JdbcCatalog",
        "--conf", "spark.sql.catalog.warehouse.uri=jdbc:postgresql://postgres:5432/ngods?user=ngods&password=ngods",
        "--conf", "spark.sql.catalog.warehouse.jdbc.useSSL=false",
        "--conf", "spark.sql.catalog.warehouse.jdbc.user=ngods",
        "--conf", "spark.sql.catalog.warehouse.jdbc.password=ngods",
        "--conf", "spark.sql.catalog.warehouse.io-impl=org.apache.iceberg.aws.s3.S3FileIO",
        "--conf", "spark.sql.catalog.warehouse.warehouse=s3a://warehouse",
        "--conf", "spark.sql.catalog.warehouse.s3.endpoint=http://minio:9000",
        "-e", """
        CREATE NAMESPACE IF NOT EXISTS default;
        CREATE NAMESPACE IF NOT EXISTS airflow_bronze;
        CREATE NAMESPACE IF NOT EXISTS airflow_silver;
        CREATE NAMESPACE IF NOT EXISTS airflow_gold;
        SHOW NAMESPACES;
        """
    ]
    
    try:
        result = subprocess.run(
            create_namespace_cmd,
            capture_output=True,
            text=True,
            timeout=30
        )
        print(f"创建命名空间输出: {result.stdout}")
        if result.returncode == 0:
            print("✅ 命名空间创建成功")
        else:
            print(f"❌ 命名空间创建失败: {result.stderr}")
    except Exception as e:
        print(f"❌ 创建命名空间失败: {e}")
    
    print("=" * 50)
    print("测试完成")
    print("=" * 50)
    
    return True

if __name__ == "__main__":
    test_spark_connection() 