"""
Spark连接配置
配置Airflow连接到aio提供的Spark集群
"""

from airflow.models import Connection
from airflow.settings import Session

def setup_spark_connections():
    """设置Spark连接"""
    session = Session()
    
    # 检查是否已存在连接
    existing_conn = session.query(Connection).filter(
        Connection.conn_id == 'spark_aio'
    ).first()
    
    if not existing_conn:
        # 创建Spark连接
        spark_conn = Connection(
            conn_id='spark_aio',
            conn_type='spark',
            host='aio',
            port=7077,
            extra={
                'master': 'spark://aio:7077',
                'deploy_mode': 'client',
                'spark_home': '/opt/spark',
                'spark_conf_dir': '/opt/spark/conf',
                'spark_sql_extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
                'spark_sql_catalog_warehouse': 'org.apache.iceberg.spark.SparkCatalog',
                'spark_sql_default_catalog': 'warehouse',
                'spark_sql_catalog_warehouse_catalog_impl': 'org.apache.iceberg.jdbc.JdbcCatalog',
                'spark_sql_catalog_warehouse_uri': 'jdbc:postgresql://postgres:5432/ngods?user=ngods&password=ngods',
                'spark_sql_catalog_warehouse_jdbc_useSSL': 'false',
                'spark_sql_catalog_warehouse_jdbc_user': 'ngods',
                'spark_sql_catalog_warehouse_jdbc_password': 'ngods',
                'spark_sql_catalog_warehouse_io_impl': 'org.apache.iceberg.aws.s3.S3FileIO',
                'spark_sql_catalog_warehouse_warehouse': 's3a://warehouse',
                'spark_sql_catalog_warehouse_s3_endpoint': 'http://minio:9000'
            }
        )
        session.add(spark_conn)
        session.commit()
        print("Spark连接已创建: spark_aio")
    else:
        print("Spark连接已存在: spark_aio")
    
    session.close()

if __name__ == "__main__":
    setup_spark_connections() 