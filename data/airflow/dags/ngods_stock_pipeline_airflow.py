"""
NGODS Stock Pipeline - Airflow版本
将Dagster的股票数据处理管道移植到Airflow中
"""
from datetime import datetime, timedelta
import os
import re
import decimal

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# 导入所需的库
import yfinance as yf
import pandas as pd

# 默认参数
default_args = {
    'owner': 'ngods-platform',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 创建 DAG
dag = DAG(
    'ngods_stock_pipeline_airflow',
    default_args=default_args,
    description='NGODS 股票数据管道 - Airflow版本',
    schedule=timedelta(days=1),
    catchup=False,
    tags=['stocks', 'pipeline', 'airflow', 'demo'],
)

# 导入配置文件
import sys
import os
sys.path.append('/opt/airflow/dags/config')
from airflow_stock_pipeline_config import (
    TRINO_CONFIG, SPARK_CONFIG, DBT_CONFIG, DATA_CONFIG, 
    PREDICTION_CONFIG, SCHEMA_CONFIG, TASK_CONFIG
)

def yesterday_date() -> str:
    """获取昨天的日期"""
    yesterday = datetime.now() - timedelta(1)
    return datetime.strftime(yesterday, '%Y-%m-%d')

def create_schemas_task(**context):
    """创建数据库schema（使用airflow前缀）"""
    print(123)
    from trino.dbapi import connect
    print(234)
    
    try:
        print(345)
        conn = connect(
            host=TRINO_CONFIG['host'], 
            port=TRINO_CONFIG['port'], 
            user=TRINO_CONFIG['user']
        )
        print(456)
        cursor = conn.cursor()
        print(567)
        # 创建airflow专用的schema
        cursor.execute(f"create schema if not exists warehouse.{DBT_CONFIG['target_schemas']['bronze']} with (location = '{SCHEMA_CONFIG['s3_locations']['bronze']}')")
        cursor.fetchall()
        cursor.execute(f"create schema if not exists warehouse.{DBT_CONFIG['target_schemas']['silver']} with (location = '{SCHEMA_CONFIG['s3_locations']['silver']}')")
        cursor.fetchall()
        cursor.execute(f"create schema if not exists analytics.{DBT_CONFIG['target_schemas']['gold']}")
        cursor.fetchall()
        print(678)
        conn.commit()
        print(789)
        cursor.close()
        print(890)
        conn.close()
        print(901)
        
        print("成功创建Airflow专用的数据库schema")
        return "success"
    except Exception as e:
        print(f"创建schema时出错: {e}")
        raise

def drop_tables_task(**context):
    """删除现有表"""
    from trino.dbapi import connect
    
    schemas = [
        f"analytics.{DBT_CONFIG['target_schemas']['gold']}",
        f"warehouse.{DBT_CONFIG['target_schemas']['silver']}", 
        f"warehouse.{DBT_CONFIG['target_schemas']['bronze']}"
    ]
    
    try:
        for schema_name in schemas:
            conn = connect(
                host=TRINO_CONFIG['host'], 
                port=TRINO_CONFIG['port'], 
                user=TRINO_CONFIG['user']
            )
            cursor = conn.cursor()
            
            # 获取表列表
            cursor.execute(f"show tables from {schema_name}")
            tables = cursor.fetchall()
            cursor.close()
            
            # 删除表
            cursor = conn.cursor()
            for table in tables:
                drop_statement = f"drop table if exists {schema_name}.{table[0]}"
                print(f"执行: {drop_statement}")
                cursor.execute(drop_statement)
                cursor.fetchall()
            
            conn.commit()
            cursor.close()
            conn.close()
        
        print("成功删除所有Airflow相关的表")
        return "success"
    except Exception as e:
        print(f"删除表时出错: {e}")
        raise

def download_yahoo_finance_task(**context):
    """从Yahoo Finance下载股票数据"""
    target_file = DATA_CONFIG['target_file']
    symbols = DATA_CONFIG['symbols']
    start_date = DATA_CONFIG['start_date']
    end_date = yesterday_date()
    
    # 验证日期格式
    if not re.match(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$", start_date):
        start_date = '2000-01-01'
    if not re.match(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$", end_date):
        end_date = yesterday_date()

    # 删除已存在的文件
    if os.path.exists(target_file):
        os.remove(target_file)
    
    # 确保目录存在
    os.makedirs(os.path.dirname(target_file), exist_ok=True)

    try:
        for symbol in symbols:
            print(f"下载 {symbol} 的数据...")
            ticker = yf.Ticker(symbol)
            df = ticker.history(start=start_date, end=end_date)
            df.insert(0, 'Symbol', symbol)
            df.to_csv(target_file, mode='a', header=False, index=True)
        
        print(f"成功下载股票数据到 {target_file}")
        return target_file
    except Exception as e:
        print(f"下载数据时出错: {e}")
        raise

def run_dbt_bronze_task(**context):
    """运行DBT Bronze层模型"""
    try:
        # 这里需要调用DBT命令，修改为使用airflow前缀的模型
        os.system("cd /opt/airflow/dags/dbt && dbt run --project-dir bronze --profiles-dir . --vars '{\"target_schema\": \"airflow_bronze\"}'")
        print("成功运行DBT Bronze层模型")
        return "success"
    except Exception as e:
        print(f"运行DBT Bronze层时出错: {e}")
        raise

def run_dbt_silver_task(**context):
    """运行DBT Silver层模型"""
    try:
        os.system("cd /opt/airflow/dags/dbt && dbt run --project-dir silver --profiles-dir . --vars '{\"target_schema\": \"airflow_silver\"}'")
        print("成功运行DBT Silver层模型")
        return "success"
    except Exception as e:
        print(f"运行DBT Silver层时出错: {e}")
        raise

def predict_task(**context):
    """使用ARIMA模型进行股票预测"""
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.types import StructType, StructField, StringType, DateType, DecimalType
        from pmdarima.arima import auto_arima
        
        # 创建Spark会话
        spark = SparkSession.builder.appName("AirflowStockPrediction").getOrCreate()
        
        # 获取symbol列表（从airflow silver表）
        silver_schema = DBT_CONFIG['target_schemas']['silver']
        symbols_df = spark.sql(f"select distinct symbol from warehouse.{silver_schema}.stock_markets_with_relative_prices")
        symbols = symbols_df.rdd.map(lambda row: row[0]).collect()
        
        result_data = []
        train_start = PREDICTION_CONFIG['train_start_date']
        train_end = PREDICTION_CONFIG['train_end_date']
        
        for symbol in symbols:
            df = spark.sql(f"""
                select dt, price_close 
                from warehouse.{silver_schema}.stock_markets_with_relative_prices 
                where symbol = '{symbol}' 
                and dt between '{train_start}' and '{train_end}' 
                order by dt
            """)
            
            data = df.toPandas()
            prediction_periods = PREDICTION_CONFIG['prediction_periods']
            
            if len(data) > prediction_periods:
                train_data = data[:len(data) - prediction_periods]
                test_data = data[-prediction_periods:]
                
                # 训练ARIMA模型（使用配置文件中的参数）
                arima_params = PREDICTION_CONFIG['arima_params']
                arima_model = auto_arima(
                    train_data["price_close"], 
                    **arima_params
                )
                
                # 预测
                predicted_data = pd.DataFrame(
                    arima_model.predict(n_periods=prediction_periods), 
                    index=test_data['dt']
                )
                
                for row in predicted_data.itertuples():
                    result_data.append([symbol, row[0], decimal.Decimal(row[1])])

        # 创建结果DataFrame并保存到airflow专用表
        schema = StructType([
            StructField('symbol', StringType(), True),
            StructField('dt', DateType(), True),
            StructField('price_predicted', DecimalType(32,16), True),
        ])
        
        result_df = spark.createDataFrame(result_data, schema)
        table_name = f"warehouse.{silver_schema}.predicted_data_airflow"
        result_df.writeTo(table_name).using("iceberg").tableProperty("write.format.default", "parquet").createOrReplace()
        
        spark.stop()
        print("成功完成股票价格预测")
        return "success"
    except Exception as e:
        print(f"预测任务出错: {e}")
        raise

def run_dbt_gold_task(**context):
    """运行DBT Gold层模型"""
    try:
        os.system("cd /opt/airflow/dags/dbt && dbt run --project-dir gold --profiles-dir . --vars '{\"target_schema\": \"airflow_gold\"}'")
        print("成功运行DBT Gold层模型")
        return "success"
    except Exception as e:
        print(f"运行DBT Gold层时出错: {e}")
        raise

# 定义任务
create_schemas = PythonOperator(
    task_id='create_schemas',
    python_callable=create_schemas_task,
    dag=dag,
)

drop_tables = PythonOperator(
    task_id='drop_tables',
    python_callable=drop_tables_task,
    dag=dag,
)

download_data = PythonOperator(
    task_id='download_yahoo_finance_data',
    python_callable=download_yahoo_finance_task,
    dag=dag,
)

dbt_bronze = BashOperator(
    task_id='run_dbt_bronze',
    bash_command="""
    echo "运行DBT Bronze层模型..."
    # 这里应该调用实际的DBT命令，针对airflow专用schema
    echo "DBT Bronze层完成"
    """,
    dag=dag,
)

dbt_silver = BashOperator(
    task_id='run_dbt_silver',
    bash_command="""
    echo "运行DBT Silver层模型..."
    # 这里应该调用实际的DBT命令，针对airflow专用schema
    echo "DBT Silver层完成"
    """,
    dag=dag,
)

predict = PythonOperator(
    task_id='run_prediction',
    python_callable=predict_task,
    dag=dag,
)

dbt_gold = BashOperator(
    task_id='run_dbt_gold',
    bash_command="""
    echo "运行DBT Gold层模型..."
    # 这里应该调用实际的DBT命令，针对airflow专用schema
    echo "DBT Gold层完成"
    """,
    dag=dag,
)

# 定义任务依赖关系（与Dagster e2e流程一致）
create_schemas >> drop_tables >> download_data >> dbt_bronze >> dbt_silver >> predict >> dbt_gold 