"""
NGODS Stock Pipeline with DuckDB (Simplified)
使用DuckDB的简化版股票数据管道
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import duckdb
import pandas as pd
import yfinance as yf
import logging

# 默认参数
default_args = {
    'owner': 'ngods',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 创建DAG
dag = DAG(
    'ngods_stock_pipeline_duckdb_simple',
    default_args=default_args,
    description='使用DuckDB工具类的简化股票数据处理管道',
    schedule='0 3 * * *',  # 每天凌晨3点执行
    catchup=False,
    tags=['ngods', 'stock', 'duckdb', 'simple'],
)

def download_and_process_stocks(**context):
    """
    下载和处理股票数据
    """
    symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'META', 'NVDA']
    output_path = '/var/lib/ngods/stage/stock_data_duckdb.parquet'
    
    # 下载和处理数据
    all_data = []
    
    for symbol in symbols:
        logging.info(f"下载 {symbol} 的数据...")
        
        ticker = yf.Ticker(symbol)
        data = ticker.history(period='1y')
        
        if not data.empty:
            data = data.reset_index()
            data['symbol'] = symbol
            
            data = data.rename(columns={
                'Date': 'date',
                'Open': 'open',
                'High': 'high',
                'Low': 'low',
                'Close': 'close',
                'Volume': 'volume'
            })
            
            data = data[['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']]
            all_data.append(data)
    
    if all_data:
        result_df = pd.concat(all_data, ignore_index=True)
        result_df.to_parquet(output_path)
        
        context['task_instance'].xcom_push(key='processed_records', value=len(result_df))
        return f"成功处理 {len(result_df)} 条记录"
    else:
        return "没有数据被处理"

def analyze_stock_trends(**context):
    """
    分析股票趋势
    """
    # 读取数据
    df = pd.read_parquet('/var/lib/ngods/stage/stock_data_duckdb.parquet')
    
    if df.empty:
        return "没有数据可分析"
    
    # 使用DuckDB进行分析（内存数据库）
    con = duckdb.connect(':memory:')
    
    try:
        # 注册DataFrame
        con.register("stock_data", df)
        
        # 计算技术指标
        analysis_query = """
            WITH daily_metrics AS (
                SELECT 
                    symbol,
                    date,
                    close,
                    LAG(close, 1) OVER (PARTITION BY symbol ORDER BY date) as prev_close,
                    LAG(close, 5) OVER (PARTITION BY symbol ORDER BY date) as ma_5,
                    LAG(close, 20) OVER (PARTITION BY symbol ORDER BY date) as ma_20,
                    LAG(close, 50) OVER (PARTITION BY symbol ORDER BY date) as ma_50
                FROM stock_data
                WHERE date >= CURRENT_DATE - INTERVAL 30 DAY
            )
            SELECT 
                symbol,
                date,
                close,
                ROUND((close - prev_close) / prev_close * 100, 2) as daily_return_pct,
                ROUND((close - ma_5) / ma_5 * 100, 2) as vs_ma5_pct,
                ROUND((close - ma_20) / ma_20 * 100, 2) as vs_ma20_pct,
                ROUND((close - ma_50) / ma_50 * 100, 2) as vs_ma50_pct,
                CASE 
                    WHEN close > ma_20 AND ma_20 > ma_50 THEN 'BULLISH'
                    WHEN close < ma_20 AND ma_20 < ma_50 THEN 'BEARISH'
                    ELSE 'NEUTRAL'
                END as trend
            FROM daily_metrics
            WHERE prev_close IS NOT NULL
            ORDER BY symbol, date DESC
        """
        
        analysis_df = con.execute(analysis_query).df()
        
        # 保存分析结果
        analysis_df.to_parquet('/var/lib/ngods/stage/stock_analysis_duckdb.parquet')
        
        # 计算汇总统计
        summary_query = """
            SELECT 
                symbol,
                COUNT(*) as data_points,
                ROUND(AVG(close), 2) as avg_close,
                ROUND(MIN(close), 2) as min_close,
                ROUND(MAX(close), 2) as max_close,
                ROUND(STDDEV(close), 2) as volatility,
                ROUND((MAX(close) - MIN(close)) / MIN(close) * 100, 2) as price_range_pct
            FROM stock_data
            WHERE date >= CURRENT_DATE - INTERVAL 30 DAY
            GROUP BY symbol
            ORDER BY avg_close DESC
        """
        
        summary_df = con.execute(summary_query).df()
        summary_df.to_parquet('/var/lib/ngods/stage/stock_summary_duckdb.parquet')
        
        return f"分析完成，生成了 {len(analysis_df)} 条分析记录"
        
    finally:
        con.close()

def export_to_minio(**context):
    """
    导出数据到MinIO
    """
    # 读取分析结果
    analysis_df = pd.read_parquet('/var/lib/ngods/stage/stock_analysis_duckdb.parquet')
    summary_df = pd.read_parquet('/var/lib/ngods/stage/stock_summary_duckdb.parquet')
    
    if not analysis_df.empty:
        # 使用DuckDB导出到MinIO（内存数据库）
        con = duckdb.connect(':memory:')
        
        try:
            # 安装和配置httpfs扩展
            con.execute("INSTALL httpfs")
            con.execute("LOAD httpfs")
            con.execute("SET s3_endpoint='minio:9000'")
            con.execute("SET s3_access_key_id='minio'")
            con.execute("SET s3_secret_access_key='minio123'")
            con.execute("SET s3_use_ssl=false")
            con.execute("SET s3_url_style='path'")
            
            # 注册DataFrame
            con.register("analysis_data", analysis_df)
            con.register("summary_data", summary_df)
            
            # 导出到MinIO
            con.execute("COPY analysis_data TO 's3://warehouse/stock_analysis_duckdb.parquet' (FORMAT PARQUET)")
            con.execute("COPY summary_data TO 's3://warehouse/stock_summary_duckdb.parquet' (FORMAT PARQUET)")
            
            return f"成功导出 {len(analysis_df)} 条分析记录到MinIO"
            
        finally:
            con.close()
    else:
        return "没有数据可导出"

def generate_report(**context):
    """
    生成报告
    """
    # 读取汇总数据
    summary_df = pd.read_parquet('/var/lib/ngods/stage/stock_summary_duckdb.parquet')
    
    if not summary_df.empty:
        # 生成简单的HTML报告
        html_content = f"""
        <html>
        <head><title>股票数据分析报告</title></head>
        <body>
        <h1>股票数据分析报告</h1>
        <p>生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <h2>股票汇总统计</h2>
        <table border="1">
        <tr><th>股票代码</th><th>数据点</th><th>平均价格</th><th>最低价格</th><th>最高价格</th><th>波动率</th><th>价格区间(%)</th></tr>
        """
        
        for _, row in summary_df.iterrows():
            html_content += f"""
            <tr>
                <td>{row['symbol']}</td>
                <td>{row['data_points']}</td>
                <td>{row['avg_close']}</td>
                <td>{row['min_close']}</td>
                <td>{row['max_close']}</td>
                <td>{row['volatility']}</td>
                <td>{row['price_range_pct']}</td>
            </tr>
            """
        
        html_content += """
        </table>
        </body>
        </html>
        """
        
        # 保存HTML报告
        with open('/var/lib/ngods/stage/stock_report_duckdb.html', 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        return f"报告生成完成，包含 {len(summary_df)} 只股票的数据"
    else:
        return "没有数据生成报告"

# 定义任务
download_task = PythonOperator(
    task_id='download_and_process_stocks',
    python_callable=download_and_process_stocks,
    dag=dag,
)

analyze_task = PythonOperator(
    task_id='analyze_stock_trends',
    python_callable=analyze_stock_trends,
    dag=dag,
)

export_task = PythonOperator(
    task_id='export_to_minio',
    python_callable=export_to_minio,
    dag=dag,
)

report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag,
)

# 设置任务依赖
download_task >> analyze_task >> [export_task, report_task] 