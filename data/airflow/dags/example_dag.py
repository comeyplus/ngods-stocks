"""
示例 DAG - 演示 Airflow 与数据平台的集成
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

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
    'ngods_platform_example',
    default_args=default_args,
    description='NGODS 数据平台示例 DAG',
    schedule=timedelta(days=1),
    catchup=False,
    tags=['example', 'ngods'],
)

def print_context(**context):
    """打印 Airflow 上下文信息"""
    print(f"当前执行日期: {context['ds']}")
    print(f"DAG 运行 ID: {context['dag_run'].run_id}")
    print("Airflow 与 NGODS 数据平台集成成功！")
    return "success"

# 任务 1: 检查系统状态
check_system = BashOperator(
    task_id='check_system_status',
    bash_command='echo "检查系统状态..." && date && echo "系统正常运行"',
    dag=dag,
)

# 任务 2: 测试 Python 操作
test_python = PythonOperator(
    task_id='test_python_operator',
    python_callable=print_context,
    dag=dag,
)

# 任务 3: 测试数据库连接（可选，需要配置连接）
# test_db = SQLExecuteQueryOperator(
#     task_id='test_database_connection',
#     conn_id='postgres_default',
#     sql='SELECT version();',
#     dag=dag,
# )

# 任务 4: 模拟数据处理
simulate_data_processing = BashOperator(
    task_id='simulate_data_processing',
    bash_command="""
    echo "开始数据处理任务..."
    echo "1. 从 MinIO 读取数据..."
    echo "2. 使用 Spark 处理数据..."
    echo "3. 将结果写入数据仓库..."
    echo "4. 触发 dbt 模型更新..."
    echo "数据处理任务完成！"
    """,
    dag=dag,
)

# 任务依赖关系
check_system >> test_python >> simulate_data_processing 