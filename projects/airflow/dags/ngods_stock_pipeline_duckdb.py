"""
NGODS Stock Pipeline with DuckDB
使用DuckDB作为数据处理引擎的股票数据管道
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import duckdb
import pandas as pd
import yfinance as yf
import logging
import numpy as np
from scipy import stats
import io
import json
import boto3
from botocore.exceptions import ClientError

def get_minio_client():
    """
    获取MinIO S3客户端，使用环境变量或Airflow变量
    """
    # 优先使用环境变量，如果没有则使用Airflow变量
    endpoint_url = os.getenv('MINIO_ENDPOINT_URL', 
                            Variable.get('minio_endpoint_url', default_var='http://minio:9000'))
    access_key_id = os.getenv('MINIO_ACCESS_KEY_ID', 
                             Variable.get('minio_access_key_id', default_var='minio'))
    secret_access_key = os.getenv('MINIO_SECRET_ACCESS_KEY', 
                                 Variable.get('minio_secret_access_key', default_var='minio123'))
    region_name = os.getenv('MINIO_REGION_NAME', 
                           Variable.get('minio_region_name', default_var='us-east-1'))
    
    return boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name=region_name
    )

def save_to_minio(client, bucket_name, object_name, data):
    """
    保存数据到MinIO
    """
    try:
        # 确保bucket存在
        try:
            client.head_bucket(Bucket=bucket_name)
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                client.create_bucket(Bucket=bucket_name)
                logging.info(f"创建bucket: {bucket_name}")
            else:
                raise
        
        # 将DataFrame转换为Parquet格式的字节流
        parquet_buffer = io.BytesIO()
        data.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        
        # 上传到MinIO
        client.put_object(
            Bucket=bucket_name,
            Key=object_name,
            Body=parquet_buffer.getvalue(),
            ContentType='application/octet-stream'
        )
        
        logging.info(f"成功保存到MinIO: {bucket_name}/{object_name}, 文件大小: {parquet_buffer.getbuffer().nbytes} bytes")
        
    except ClientError as e:
        logging.error(f"MinIO操作失败: {e}")
        raise

def create_sample_stock_data():
    """
    创建示例股票数据用于测试
    """
    symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'NVDA', 'META', 'NFLX', 'ADBE', 'CRM']
    dates = pd.date_range(start='2024-01-01', end='2024-12-31', freq='D')
    
    data = []
    for symbol in symbols:
        # 为每个股票生成模拟价格数据
        base_price = 100 + np.random.randint(50, 200)
        prices = []
        for i in range(len(dates)):
            if i == 0:
                price = base_price
            else:
                # 添加一些随机波动
                change = np.random.normal(0, 0.02)  # 2%的标准差
                price = prices[-1] * (1 + change)
            prices.append(max(price, 10))  # 确保价格不为负
        
        for i, date in enumerate(dates):
            price = prices[i]
            volume = np.random.randint(1000000, 10000000)
            data.append({
                'symbol': symbol,
                'date': date.date(),
                'open': price * (1 + np.random.normal(0, 0.01)),
                'high': price * (1 + abs(np.random.normal(0, 0.02))),
                'low': price * (1 - abs(np.random.normal(0, 0.02))),
                'close': price,
                'volume': volume
            })
    
    return pd.DataFrame(data)

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
    'ngods_stock_pipeline_duckdb',
    default_args=default_args,
    description='使用DuckDB的股票数据处理管道',
    schedule='0 2 * * *',  # 每天凌晨2点执行
    catchup=False,
    tags=['ngods', 'stock', 'duckdb'],
)

def download_stock_data_duckdb(**context):
    """
    使用DuckDB下载和处理股票数据
    """
    # 股票代码列表 - 扩展更多股票
    symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'NVDA', 'META', 'NFLX', 'ADBE', 'CRM']
    
    # 连接到DuckDB（使用内存数据库避免文件锁冲突）
    con = duckdb.connect(':memory:')
    
    try:
        # 创建表结构
        con.execute("""
            CREATE TABLE IF NOT EXISTS stock_data (
                symbol VARCHAR,
                date DATE,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        all_data = []
        
        for symbol in symbols:
            logging.info(f"下载 {symbol} 的数据...")
            
            # 使用yfinance下载数据
            ticker = yf.Ticker(symbol)
            data = ticker.history(period='1y')
            
            if not data.empty:
                # 重置索引，将日期变为列
                data = data.reset_index()
                data['symbol'] = symbol
                
                # 重命名列
                data = data.rename(columns={
                    'Date': 'date',
                    'Open': 'open',
                    'High': 'high',
                    'Low': 'low',
                    'Close': 'close',
                    'Volume': 'volume'
                })
                
                # 选择需要的列
                data = data[['symbol', 'date', 'open', 'high', 'low', 'close', 'volume']]
                all_data.append(data)
        
        if all_data:
            # 合并所有数据
            combined_data = pd.concat(all_data, ignore_index=True)
            
            # 确保数据类型正确
            combined_data['date'] = pd.to_datetime(combined_data['date']).dt.date
            combined_data['open'] = combined_data['open'].astype(float)
            combined_data['high'] = combined_data['high'].astype(float)
            combined_data['low'] = combined_data['low'].astype(float)
            combined_data['close'] = combined_data['close'].astype(float)
            combined_data['volume'] = combined_data['volume'].astype(int)
            
            # 将数据插入DuckDB - 使用更兼容的方式
            con.execute("DELETE FROM stock_data WHERE date >= CURRENT_DATE - INTERVAL 1 DAY")
            
            # 使用register方法注册DataFrame，然后插入
            con.register("temp_data", combined_data)
            con.execute("INSERT INTO stock_data (symbol, date, open, high, low, close, volume) SELECT symbol, date, open, high, low, close, volume FROM temp_data")
            
            logging.info(f"成功插入 {len(combined_data)} 条记录")
            
            # 保存为Parquet文件到MinIO
            minio_client = get_minio_client()
            save_to_minio(minio_client, 'stock-data', 'stock_data_latest.parquet', combined_data)
            
    finally:
        con.close()
    
    return "数据下载完成"

def calculate_technical_indicators(df):
    """
    计算技术指标
    """
    # 计算移动平均线
    df['ma_5'] = df.groupby('symbol')['close'].rolling(window=5).mean().reset_index(0, drop=True)
    df['ma_20'] = df.groupby('symbol')['close'].rolling(window=20).mean().reset_index(0, drop=True)
    df['ma_50'] = df.groupby('symbol')['close'].rolling(window=50).mean().reset_index(0, drop=True)
    
    # 计算RSI
    def calculate_rsi(prices, window=14):
        delta = prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    df['rsi'] = df.groupby('symbol')['close'].apply(calculate_rsi).reset_index(0, drop=True)
    
    # 计算布林带
    df['bb_middle'] = df.groupby('symbol')['close'].rolling(window=20).mean().reset_index(0, drop=True)
    bb_std = df.groupby('symbol')['close'].rolling(window=20).std().reset_index(0, drop=True)
    df['bb_upper'] = df['bb_middle'] + (bb_std * 2)
    df['bb_lower'] = df['bb_middle'] - (bb_std * 2)
    
    # 计算MACD
    def calculate_macd(prices, fast=12, slow=26, signal=9):
        ema_fast = prices.ewm(span=fast).mean()
        ema_slow = prices.ewm(span=slow).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal).mean()
        histogram = macd_line - signal_line
        return pd.Series({
            'macd': macd_line,
            'macd_signal': signal_line,
            'macd_histogram': histogram
        })
    
    # 为每个股票计算MACD
    macd_data = []
    for symbol in df['symbol'].unique():
        symbol_data = df[df['symbol'] == symbol].copy()
        macd_result = calculate_macd(symbol_data['close'])
        symbol_data['macd'] = macd_result['macd']
        symbol_data['macd_signal'] = macd_result['macd_signal']
        symbol_data['macd_histogram'] = macd_result['macd_histogram']
        macd_data.append(symbol_data)
    
    # 合并所有数据
    df = pd.concat(macd_data, ignore_index=True)
    
    # 计算成交量加权平均价格 (VWAP)
    df['vwap'] = (df['close'] * df['volume']).rolling(window=20).sum() / df['volume'].rolling(window=20).sum()
    
    return df

def process_stock_analytics_duckdb(**context):
    """
    使用DuckDB进行股票数据分析，通过PostgreSQL扩展直接访问MinIO数据
    """
    con = duckdb.connect(':memory:')
    
    try:
        # 1. 配置MinIO S3连接
        logging.info("配置MinIO S3连接...")
        
        # 获取MinIO配置信息
        endpoint_url = os.getenv('MINIO_ENDPOINT_URL', 
                                Variable.get('minio_endpoint_url', default_var='http://minio:9000'))
        access_key_id = os.getenv('MINIO_ACCESS_KEY_ID', 
                                 Variable.get('minio_access_key_id', default_var='minio'))
        secret_access_key = os.getenv('MINIO_SECRET_ACCESS_KEY', 
                                     Variable.get('minio_secret_access_key', default_var='minio123'))
        region_name = os.getenv('MINIO_REGION_NAME', 
                               Variable.get('minio_region_name', default_var='us-east-1'))
        
        # 从URL中提取主机名和端口
        if endpoint_url.startswith('http://'):
            endpoint_host = endpoint_url[7:]  # 移除 'http://'
        elif endpoint_url.startswith('https://'):
            endpoint_host = endpoint_url[8:]  # 移除 'https://'
        else:
            endpoint_host = endpoint_url
        
        # 配置DuckDB S3连接
        s3_config_sql = f"""
            CREATE OR REPLACE PERSISTENT SECRET minio_s3 (
                TYPE s3,
                KEY_ID '{access_key_id}',
                SECRET '{secret_access_key}',
                ENDPOINT '{endpoint_host}',
                REGION '{region_name}',
                URL_STYLE 'path',
                USE_SSL FALSE
            );
        """
        
        con.execute(s3_config_sql)
        logging.info(f"已配置MinIO连接: {endpoint_host}")
        
        # 2. 直接从MinIO读取Parquet数据
        logging.info("从MinIO读取股票数据...")
        stock_data_query = """
            SELECT 
                r['symbol'] as symbol,
                r['date'] as date,
                r['open'] as open,
                r['high'] as high,
                r['low'] as low,
                r['close'] as close,
                r['volume'] as volume
            FROM read_parquet('s3://stock-data/stock_data_latest.parquet') AS r
        """
        
        try:
            stock_data = con.execute(stock_data_query).df()
            
            # 确保数据类型正确
            stock_data['date'] = pd.to_datetime(stock_data['date']).dt.date
            stock_data['open'] = stock_data['open'].astype(float)
            stock_data['high'] = stock_data['high'].astype(float)
            stock_data['low'] = stock_data['low'].astype(float)
            stock_data['close'] = stock_data['close'].astype(float)
            stock_data['volume'] = stock_data['volume'].astype(int)
            
            logging.info(f"成功从MinIO读取股票数据，共 {len(stock_data)} 条记录")
            
        except Exception as e:
            logging.error(f"从MinIO读取数据失败: {e}")
            # 如果文件不存在或损坏，创建示例数据
            logging.info("创建示例数据用于测试...")
            stock_data = create_sample_stock_data()
        
        # 计算技术指标
        stock_data = calculate_technical_indicators(stock_data)
        
        # 注册DataFrame到DuckDB
        con.register("stock_data", stock_data)
        
        # 1. 技术指标分析
        con.execute("""
            COPY (
                WITH technical_indicators AS (
                    SELECT 
                        symbol,
                        date,
                        close,
                        ma_5,
                        ma_20,
                        ma_50,
                        rsi,
                        bb_upper,
                        bb_lower,
                        macd,
                        macd_signal,
                        vwap,
                        CASE 
                            WHEN close > ma_20 AND close > ma_50 THEN 'BULLISH'
                            WHEN close < ma_20 AND close < ma_50 THEN 'BEARISH'
                            ELSE 'NEUTRAL'
                        END as trend_signal,
                        CASE 
                            WHEN rsi > 70 THEN 'OVERBOUGHT'
                            WHEN rsi < 30 THEN 'OVERSOLD'
                            ELSE 'NORMAL'
                        END as rsi_signal,
                        CASE 
                            WHEN close > bb_upper THEN 'ABOVE_BB'
                            WHEN close < bb_lower THEN 'BELOW_BB'
                            ELSE 'INSIDE_BB'
                        END as bb_signal
                    FROM stock_data
                    WHERE date >= CURRENT_DATE - INTERVAL 30 DAY
                        AND ma_20 IS NOT NULL
                        AND rsi IS NOT NULL
                )
                SELECT * FROM technical_indicators
                ORDER BY symbol, date DESC
            ) TO 's3://stock-analytics/technical_analysis.parquet' (FORMAT PARQUET);
        """)
        logging.info("技术指标分析已保存到MinIO")
        
        # 2. 风险评估分析
        risk_analysis_query = """
            WITH daily_returns AS (
                SELECT 
                    symbol,
                    date,
                    close,
                    LAG(close, 1) OVER (PARTITION BY symbol ORDER BY date) as prev_close,
                    (close - LAG(close, 1) OVER (PARTITION BY symbol ORDER BY date)) / 
                    LAG(close, 1) OVER (PARTITION BY symbol ORDER BY date) * 100 as daily_return_pct
                FROM stock_data
                WHERE date >= CURRENT_DATE - INTERVAL 90 DAY
            ),
            risk_metrics AS (
                SELECT 
                    symbol,
                    COUNT(*) as trading_days,
                    ROUND(AVG(daily_return_pct), 4) as avg_daily_return,
                    ROUND(STDDEV(daily_return_pct), 4) as volatility,
                    ROUND(MIN(daily_return_pct), 4) as worst_day,
                    ROUND(MAX(daily_return_pct), 4) as best_day,
                    ROUND(AVG(ABS(daily_return_pct)), 4) as avg_absolute_return,
                    ROUND(PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY daily_return_pct), 4) as var_95,
                    ROUND(PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY daily_return_pct), 4) as var_99
                FROM daily_returns
                WHERE daily_return_pct IS NOT NULL
                GROUP BY symbol
            )
            SELECT 
                *,
                ROUND(avg_daily_return / NULLIF(volatility, 0), 4) as sharpe_ratio,
                CASE 
                    WHEN volatility < 2 THEN 'LOW'
                    WHEN volatility < 4 THEN 'MEDIUM'
                    ELSE 'HIGH'
                END as risk_level
            FROM risk_metrics
            ORDER BY volatility DESC
        """
        
        con.execute("""
            COPY (
                WITH daily_returns AS (
                    SELECT 
                        symbol,
                        date,
                        close,
                        LAG(close, 1) OVER (PARTITION BY symbol ORDER BY date) as prev_close,
                        (close - LAG(close, 1) OVER (PARTITION BY symbol ORDER BY date)) / 
                        LAG(close, 1) OVER (PARTITION BY symbol ORDER BY date) * 100 as daily_return_pct
                    FROM stock_data
                    WHERE date >= CURRENT_DATE - INTERVAL 90 DAY
                ),
                risk_metrics AS (
                    SELECT 
                        symbol,
                        COUNT(*) as trading_days,
                        ROUND(AVG(daily_return_pct), 4) as avg_daily_return,
                        ROUND(STDDEV(daily_return_pct), 4) as volatility,
                        ROUND(MIN(daily_return_pct), 4) as worst_day,
                        ROUND(MAX(daily_return_pct), 4) as best_day,
                        ROUND(AVG(ABS(daily_return_pct)), 4) as avg_absolute_return,
                        ROUND(PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY daily_return_pct), 4) as var_95,
                        ROUND(PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY daily_return_pct), 4) as var_99
                    FROM daily_returns
                    WHERE daily_return_pct IS NOT NULL
                    GROUP BY symbol
                )
                SELECT 
                    *,
                    ROUND(avg_daily_return / NULLIF(volatility, 0), 4) as sharpe_ratio,
                    CASE 
                        WHEN volatility < 2 THEN 'LOW'
                        WHEN volatility < 4 THEN 'MEDIUM'
                        ELSE 'HIGH'
                    END as risk_level
                FROM risk_metrics
                ORDER BY volatility DESC
            ) TO 's3://stock-analytics/risk_analysis.parquet' (FORMAT PARQUET);
        """)
        logging.info("风险评估分析已保存到MinIO")
        
        # 3. 相关性分析
        con.execute("""
            COPY (
                WITH pivot_data AS (
                    SELECT 
                        date,
                        symbol,
                        close
                    FROM stock_data
                    WHERE date >= CURRENT_DATE - INTERVAL 30 DAY
                ),
                correlation_matrix AS (
                    SELECT 
                        a.symbol as symbol1,
                        b.symbol as symbol2,
                        CORR(a.close, b.close) as correlation
                    FROM pivot_data a
                    JOIN pivot_data b ON a.date = b.date AND a.symbol < b.symbol
                    WHERE a.close IS NOT NULL AND b.close IS NOT NULL
                    GROUP BY a.symbol, b.symbol
                )
                SELECT * FROM correlation_matrix
                WHERE correlation IS NOT NULL
                ORDER BY ABS(correlation) DESC
            ) TO 's3://stock-analytics/correlation_analysis.parquet' (FORMAT PARQUET);
        """)
        logging.info("相关性分析已保存到MinIO")
        
        # 4. 趋势分析
        con.execute("""
            COPY (
                WITH trend_data AS (
                    SELECT 
                        symbol,
                        date,
                        close,
                        LAG(close, 5) OVER (PARTITION BY symbol ORDER BY date) as close_5d_ago,
                        LAG(close, 10) OVER (PARTITION BY symbol ORDER BY date) as close_10d_ago,
                        LAG(close, 20) OVER (PARTITION BY symbol ORDER BY date) as close_20d_ago
                    FROM stock_data
                    WHERE date >= CURRENT_DATE - INTERVAL 30 DAY
                ),
                trend_signals AS (
                    SELECT 
                        symbol,
                        date,
                        close,
                        ROUND((close - close_5d_ago) / close_5d_ago * 100, 2) as change_5d,
                        ROUND((close - close_10d_ago) / close_10d_ago * 100, 2) as change_10d,
                        ROUND((close - close_20d_ago) / close_20d_ago * 100, 2) as change_20d,
                        CASE 
                            WHEN (close - close_5d_ago) > 0 AND (close - close_10d_ago) > 0 AND (close - close_20d_ago) > 0 THEN 'STRONG_UPTREND'
                            WHEN (close - close_5d_ago) > 0 AND (close - close_10d_ago) > 0 THEN 'UPTREND'
                            WHEN (close - close_5d_ago) < 0 AND (close - close_10d_ago) < 0 AND (close - close_20d_ago) < 0 THEN 'STRONG_DOWNTREND'
                            WHEN (close - close_5d_ago) < 0 AND (close - close_10d_ago) < 0 THEN 'DOWNTREND'
                            ELSE 'SIDEWAYS'
                        END as trend_direction
                    FROM trend_data
                    WHERE close_5d_ago IS NOT NULL AND close_10d_ago IS NOT NULL AND close_20d_ago IS NOT NULL
                )
                SELECT * FROM trend_signals
                ORDER BY symbol, date DESC
            ) TO 's3://stock-analytics/trend_analysis.parquet' (FORMAT PARQUET);
        """)
        logging.info("趋势分析已保存到MinIO")
        
        # 5. 成交量分析
        con.execute("""
            COPY (
                WITH volume_stats AS (
                    SELECT 
                        symbol,
                        date,
                        volume,
                        close,
                        LAG(volume, 1) OVER (PARTITION BY symbol ORDER BY date) as prev_volume,
                        LAG(volume, 5) OVER (PARTITION BY symbol ORDER BY date) as avg_volume_5d,
                        LAG(volume, 20) OVER (PARTITION BY symbol ORDER BY date) as avg_volume_20d
                    FROM stock_data
                    WHERE date >= CURRENT_DATE - INTERVAL 30 DAY
                ),
                volume_analysis AS (
                    SELECT 
                        symbol,
                        date,
                        volume,
                        close,
                        ROUND((volume - prev_volume) / prev_volume * 100, 2) as volume_change_pct,
                        ROUND(volume / avg_volume_5d, 2) as volume_vs_5d_avg,
                        ROUND(volume / avg_volume_20d, 2) as volume_vs_20d_avg,
                        CASE 
                            WHEN volume > avg_volume_20d * 1.5 THEN 'HIGH_VOLUME'
                            WHEN volume < avg_volume_20d * 0.5 THEN 'LOW_VOLUME'
                            ELSE 'NORMAL_VOLUME'
                        END as volume_signal
                    FROM volume_stats
                    WHERE prev_volume IS NOT NULL AND avg_volume_5d IS NOT NULL AND avg_volume_20d IS NOT NULL
                )
                SELECT * FROM volume_analysis
                ORDER BY symbol, date DESC
            ) TO 's3://stock-analytics/volume_analysis.parquet' (FORMAT PARQUET);
        """)
        logging.info("成交量分析已保存到MinIO")
        
        # 6. 综合评分系统
        con.execute("""
            COPY (
                WITH technical_scores AS (
                    SELECT 
                        symbol,
                        date,
                        close,
                        ma_20,
                        rsi,
                        CASE 
                            WHEN close > ma_20 THEN 1 ELSE 0
                        END as ma_score,
                        CASE 
                            WHEN rsi BETWEEN 30 AND 70 THEN 1 ELSE 0
                        END as rsi_score,
                        CASE 
                            WHEN close BETWEEN bb_lower AND bb_upper THEN 1 ELSE 0
                        END as bb_score
                    FROM stock_data
                    WHERE date >= CURRENT_DATE - INTERVAL 7 DAY
                        AND ma_20 IS NOT NULL AND rsi IS NOT NULL
                ),
                final_scores AS (
                    SELECT 
                        symbol,
                        date,
                        close,
                        (ma_score + rsi_score + bb_score) as technical_score,
                        CASE 
                            WHEN (ma_score + rsi_score + bb_score) = 3 THEN 'STRONG_BUY'
                            WHEN (ma_score + rsi_score + bb_score) = 2 THEN 'BUY'
                            WHEN (ma_score + rsi_score + bb_score) = 1 THEN 'HOLD'
                            ELSE 'SELL'
                        END as recommendation
                    FROM technical_scores
                )
                SELECT * FROM final_scores
                ORDER BY symbol, date DESC
            ) TO 's3://stock-analytics/stock_recommendations.parquet' (FORMAT PARQUET);
        """)
        logging.info("股票推荐分析已保存到MinIO")
        
        # 7. 市场概览报告
        con.execute("""
            COPY (
                SELECT 
                    COUNT(DISTINCT symbol) as total_stocks,
                    COUNT(*) as total_data_points,
                    ROUND(AVG(close), 2) as avg_market_price,
                    ROUND(STDDEV(close), 2) as market_volatility,
                    ROUND(MIN(close), 2) as min_price,
                    ROUND(MAX(close), 2) as max_price
                FROM stock_data
                WHERE date >= CURRENT_DATE - INTERVAL 7 DAY
            ) TO 's3://stock-analytics/market_summary.parquet' (FORMAT PARQUET);
        """)
        
        con.execute("""
            COPY (
                SELECT 
                    symbol,
                    ROUND((MAX(close) - MIN(close)) / MIN(close) * 100, 2) as weekly_return
                FROM stock_data
                WHERE date >= CURRENT_DATE - INTERVAL 7 DAY
                GROUP BY symbol
                ORDER BY weekly_return DESC
                LIMIT 5
            ) TO 's3://stock-analytics/top_performers.parquet' (FORMAT PARQUET);
        """)
        
        logging.info("市场概览报告已保存到MinIO")
        
        logging.info(f"分析完成，生成了多个分析文件")
    except Exception as e:
        logging.error(f"分析过程中发生错误: {e}")
        # raise
    finally:
        con.close()
    
    return "深度分析完成"

def export_to_minio_duckdb(**context):
    """
    将分析结果导出到MinIO
    """
    # 获取MinIO客户端
    minio_client = get_minio_client()
    
    # 定义要检查的分析文件
    analysis_files = [
        'stock-analytics/technical_analysis.parquet',
        'stock-analytics/risk_analysis.parquet',
        'stock-analytics/correlation_analysis.parquet',
        'stock-analytics/trend_analysis.parquet',
        'stock-analytics/volume_analysis.parquet',
        'stock-analytics/stock_recommendations.parquet',
        'stock-analytics/market_summary.parquet',
        'stock-analytics/top_performers.parquet'
    ]
    
    # 检查MinIO中的文件并记录信息
    for file_path in analysis_files:
        bucket_name, object_name = file_path.split('/', 1)
        try:
            # 获取对象信息
            response = minio_client.head_object(Bucket=bucket_name, Key=object_name)
            logging.info(f"MinIO文件: {file_path} (大小: {response['ContentLength']} bytes)")
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                logging.warning(f"MinIO文件不存在: {file_path}")
            else:
                logging.error(f"检查MinIO文件失败: {e}")
    
    # 创建汇总报告
    summary_report = {
        'analysis_timestamp': datetime.now().isoformat(),
        'analysis_files': analysis_files,
        'total_analyses': len(analysis_files),
        'storage_location': 'MinIO'
    }
    
    # 保存汇总报告到MinIO
    summary_json = json.dumps(summary_report, indent=2)
    minio_client.put_object(
        Bucket='stock-analytics',
        Key='analysis_summary.json',
        Body=summary_json.encode('utf-8'),
        ContentType='application/json'
    )
    
    logging.info(f"成功导出 {len(analysis_files)} 个分析文件到MinIO")
    
    return "分析结果导出完成"

# 定义任务
download_task = PythonOperator(
    task_id='download_stock_data_duckdb',
    python_callable=download_stock_data_duckdb,
    dag=dag,
)

analytics_task = PythonOperator(
    task_id='process_stock_analytics_duckdb',
    python_callable=process_stock_analytics_duckdb,
    dag=dag,
)

export_task = PythonOperator(
    task_id='export_to_minio_duckdb',
    python_callable=export_to_minio_duckdb,
    dag=dag,
)

# 设置任务依赖
download_task >> analytics_task >> export_task 