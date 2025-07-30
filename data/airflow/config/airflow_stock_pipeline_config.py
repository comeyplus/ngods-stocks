"""
Airflow股票数据管道配置文件
用于配置数据库连接和其他环境变量
"""

# Trino连接配置
TRINO_CONFIG = {
    'host': 'trino',
    'port': 8060,
    'user': 'trino',
    'password': '',
    'catalog': 'warehouse',
    'schema': 'default'
}

# Spark配置
SPARK_CONFIG = {
    'app_name': 'AirflowStockPipeline',
    'master': 'local[*]',
    'config': {
        'spark.sql.adaptive.enabled': 'true',
        'spark.sql.adaptive.coalescePartitions.enabled': 'true',
        'spark.sql.catalog.warehouse': 'org.apache.iceberg.spark.SparkCatalog',
        'spark.sql.catalog.warehouse.type': 'hive',
        'spark.sql.catalog.warehouse.uri': 'thrift://hive-metastore:9083',
        'spark.sql.catalog.warehouse.io-impl': 'org.apache.iceberg.aws.s3.S3FileIO',
        'spark.sql.catalog.warehouse.warehouse': 's3a://warehouse/',
        'spark.sql.catalog.analytics': 'org.apache.iceberg.spark.SparkCatalog',
        'spark.sql.catalog.analytics.type': 'hive',
        'spark.sql.catalog.analytics.uri': 'thrift://hive-metastore:9083',
        'spark.sql.catalog.analytics.io-impl': 'org.apache.iceberg.aws.s3.S3FileIO',
        'spark.sql.catalog.analytics.warehouse': 's3a://analytics/',
    }
}

# DBT配置
DBT_CONFIG = {
    'bronze_project_dir': '/opt/airflow/dags/dbt/bronze',
    'silver_project_dir': '/opt/airflow/dags/dbt/silver', 
    'gold_project_dir': '/opt/airflow/dags/dbt/gold',
    'profiles_dir': '/opt/airflow/dags/dbt',
    'target_schemas': {
        'bronze': 'airflow_bronze',
        'silver': 'airflow_silver',
        'gold': 'airflow_gold'
    }
}

# 数据文件配置（使用airflow前缀避免冲突）
DATA_CONFIG = {
    'target_file': '/var/lib/ngods/stage/stocks_airflow.csv',
    'symbols': ['AAPL', 'GOOGL', 'ORCL', 'MSFT', 'CRM', 'IBM', 'AMZN', 'GC=F', 'BTC-USD', 'ETH-USD'],
    'start_date': '2000-01-01',
    'backup_dir': '/var/lib/ngods/stage/backup/airflow/',
    'processed_dir': '/var/lib/ngods/stage/processed/airflow/'
}

# 日志配置
LOGGING_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'handlers': ['console', 'file']
}

# 任务重试配置
TASK_CONFIG = {
    'retries': 3,
    'retry_delay_minutes': 5,
    'timeout_minutes': 60,
    'email_on_failure': False,
    'email_on_retry': False
}

# 预测模型配置
PREDICTION_CONFIG = {
    'train_start_date': '2020-01-01',
    'train_end_date': '2022-06-30',
    'prediction_periods': 7,
    'arima_params': {
        'start_p': 1,
        'start_d': 1, 
        'start_q': 0,
        'max_p': 5,
        'max_d': 5,
        'max_q': 5,
        'start_P': 0,
        'start_D': 1,
        'start_Q': 0,
        'max_P': 5,
        'max_D': 5,
        'max_Q': 5,
        'm': 11,
        'seasonal': True,
        'random_state': 20,
        'suppress_warnings': True,
        'stepwise': True
    }
}

# 数据库schema配置（避免与Dagster冲突）
SCHEMA_CONFIG = {
    'warehouse_schemas': [
        'warehouse.airflow_bronze',
        'warehouse.airflow_silver'
    ],
    'analytics_schemas': [
        'analytics.airflow_gold'
    ],
    's3_locations': {
        'bronze': 's3a://warehouse/airflow_bronze',
        'silver': 's3a://warehouse/airflow_silver',
        'gold': 's3a://analytics/airflow_gold'
    }
} 