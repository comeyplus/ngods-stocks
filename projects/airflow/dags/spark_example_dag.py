"""
示例DAG：演示如何使用aio提供的Spark集群
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'spark_aio_example',
    default_args=default_args,
    description='使用aio Spark集群的示例DAG',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['spark', 'aio', 'example'],
)

# 检查Spark集群状态
check_spark_cluster = BashOperator(
    task_id='check_spark_cluster',
    bash_command='curl -f http://aio:8061 || echo "Spark集群未就绪"',
    dag=dag,
)

# 使用SparkSubmitOperator提交Spark任务到aio集群
spark_job = SparkSubmitOperator(
    task_id='spark_job',
    application='/opt/airflow/dags/spark_example.py',  # Spark应用文件
    conn_id='spark_aio',  # 使用配置的Spark连接
    conf={
        'spark.master': 'spark://aio:7077',
        'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        'spark.sql.catalog.warehouse': 'org.apache.iceberg.spark.SparkCatalog',
        'spark.sql.defaultCatalog': 'warehouse',
        'spark.sql.catalog.warehouse.catalog-impl': 'org.apache.iceberg.jdbc.JdbcCatalog',
        'spark.sql.catalog.warehouse.uri': 'jdbc:postgresql://postgres:5432/ngods?user=ngods&password=ngods',
        'spark.sql.catalog.warehouse.jdbc.useSSL': 'false',
        'spark.sql.catalog.warehouse.jdbc.user': 'ngods',
        'spark.sql.catalog.warehouse.jdbc.password': 'ngods',
        'spark.sql.catalog.warehouse.io-impl': 'org.apache.iceberg.aws.s3.S3FileIO',
        'spark.sql.catalog.warehouse.warehouse': 's3a://warehouse',
        'spark.sql.catalog.warehouse.s3.endpoint': 'http://minio:9000',
        'spark.executor.memory': '1g',
        'spark.executor.cores': '2',
        'spark.driver.memory': '1g',
        'spark.driver.cores': '1',
    },
    dag=dag,
)

# 使用PythonOperator执行简单的Spark任务
def spark_python_task(**context):
    """使用PySpark的Python任务"""
    from pyspark.sql import SparkSession
    
    # 创建SparkSession连接到aio集群
    spark = SparkSession.builder \
        .appName("AirflowSparkExample") \
        .master("spark://aio:7077") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.warehouse", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.defaultCatalog", "warehouse") \
        .config("spark.sql.catalog.warehouse.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog") \
        .config("spark.sql.catalog.warehouse.uri", "jdbc:postgresql://postgres:5432/ngods?user=ngods&password=ngods") \
        .config("spark.sql.catalog.warehouse.jdbc.useSSL", "false") \
        .config("spark.sql.catalog.warehouse.jdbc.user", "ngods") \
        .config("spark.sql.catalog.warehouse.jdbc.password", "ngods") \
        .config("spark.sql.catalog.warehouse.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.sql.catalog.warehouse.warehouse", "s3a://warehouse") \
        .config("spark.sql.catalog.warehouse.s3.endpoint", "http://minio:9000") \
        .getOrCreate()
    
    # 创建一个简单的DataFrame
    data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    df = spark.createDataFrame(data, ["name", "age"])
    
    # 显示数据
    print("DataFrame内容:")
    df.show()
    
    # 执行一些简单的操作
    result = df.filter(df.age > 25).count()
    print(f"年龄大于25的人数: {result}")
    
    # 关闭SparkSession
    spark.stop()
    
    return f"处理完成，年龄大于25的人数: {result}"

spark_python_job = PythonOperator(
    task_id='spark_python_job',
    python_callable=spark_python_task,
    dag=dag,
)

# 任务依赖关系
check_spark_cluster >> [spark_job, spark_python_job] 