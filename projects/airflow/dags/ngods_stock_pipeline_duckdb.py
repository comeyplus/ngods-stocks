"""
NGODS Stock Pipeline with DuckDB
使用DuckDB作为数据处理引擎的股票数据管道
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import duckdb
import pandas as pd
import yfinance as yf
import logging
import numpy as np
from scipy import stats

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
            
            # 保存为Parquet文件
            combined_data.to_parquet('/var/lib/ngods/stage/stock_data_latest.parquet')
            
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
    使用DuckDB进行股票数据分析
    """
    try:
        # 读取Parquet文件数据
        stock_data = pd.read_parquet('/var/lib/ngods/stage/stock_data_latest.parquet')
        
        # 确保数据类型正确
        stock_data['date'] = pd.to_datetime(stock_data['date']).dt.date
        stock_data['open'] = stock_data['open'].astype(float)
        stock_data['high'] = stock_data['high'].astype(float)
        stock_data['low'] = stock_data['low'].astype(float)
        stock_data['close'] = stock_data['close'].astype(float)
        stock_data['volume'] = stock_data['volume'].astype(int)
        
        logging.info(f"成功读取股票数据，共 {len(stock_data)} 条记录")
        
    except Exception as e:
        logging.error(f"读取Parquet文件失败: {e}")
        # 如果文件不存在或损坏，创建示例数据
        logging.info("创建示例数据用于测试...")
        stock_data = create_sample_stock_data()
    
    # 计算技术指标
    stock_data = calculate_technical_indicators(stock_data)
    
    con = duckdb.connect(':memory:')
    
    try:
        # 注册DataFrame到DuckDB
        con.register("stock_data", stock_data)
        
        # 1. 技术指标分析
        technical_analysis_query = """
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
        """
        
        technical_df = con.execute(technical_analysis_query).df()
        technical_df.to_parquet('/var/lib/ngods/stage/technical_analysis.parquet')
        
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
        
        risk_df = con.execute(risk_analysis_query).df()
        risk_df.to_parquet('/var/lib/ngods/stage/risk_analysis.parquet')
        
        # 3. 相关性分析
        correlation_query = """
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
        """
        
        correlation_df = con.execute(correlation_query).df()
        correlation_df.to_parquet('/var/lib/ngods/stage/correlation_analysis.parquet')
        
        # 4. 趋势分析
        trend_analysis_query = """
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
        """
        
        trend_df = con.execute(trend_analysis_query).df()
        trend_df.to_parquet('/var/lib/ngods/stage/trend_analysis.parquet')
        
        # 5. 成交量分析
        volume_analysis_query = """
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
        """
        
        volume_df = con.execute(volume_analysis_query).df()
        volume_df.to_parquet('/var/lib/ngods/stage/volume_analysis.parquet')
        
        # 6. 综合评分系统
        scoring_query = """
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
        """
        
        scoring_df = con.execute(scoring_query).df()
        scoring_df.to_parquet('/var/lib/ngods/stage/stock_recommendations.parquet')
        
        # 7. 市场概览报告 - 分别查询避免UNION列数不匹配
        market_summary_query = """
            SELECT 
                COUNT(DISTINCT symbol) as total_stocks,
                COUNT(*) as total_data_points,
                ROUND(AVG(close), 2) as avg_market_price,
                ROUND(STDDEV(close), 2) as market_volatility,
                ROUND(MIN(close), 2) as min_price,
                ROUND(MAX(close), 2) as max_price
            FROM stock_data
            WHERE date >= CURRENT_DATE - INTERVAL 7 DAY
        """
        
        top_performers_query = """
            SELECT 
                symbol,
                ROUND((MAX(close) - MIN(close)) / MIN(close) * 100, 2) as weekly_return
            FROM stock_data
            WHERE date >= CURRENT_DATE - INTERVAL 7 DAY
            GROUP BY symbol
            ORDER BY weekly_return DESC
            LIMIT 5
        """
        
        market_summary_df = con.execute(market_summary_query).df()
        top_performers_df = con.execute(top_performers_query).df()
        
        # 保存市场概览
        market_summary_df.to_parquet('/var/lib/ngods/stage/market_summary.parquet')
        top_performers_df.to_parquet('/var/lib/ngods/stage/top_performers.parquet')
        
        logging.info(f"分析完成，生成了多个分析文件")
        
    finally:
        con.close()
    
    return "深度分析完成"

def export_to_minio_duckdb(**context):
    """
    将分析结果导出到MinIO
    """
    import os
    
    # 定义要导出的分析文件
    analysis_files = [
        '/var/lib/ngods/stage/technical_analysis.parquet',
        '/var/lib/ngods/stage/risk_analysis.parquet',
        '/var/lib/ngods/stage/correlation_analysis.parquet',
        '/var/lib/ngods/stage/trend_analysis.parquet',
        '/var/lib/ngods/stage/volume_analysis.parquet',
        '/var/lib/ngods/stage/stock_recommendations.parquet',
        '/var/lib/ngods/stage/market_summary.parquet',
        '/var/lib/ngods/stage/top_performers.parquet'
    ]
    
    # 检查文件是否存在并记录信息
    for file_path in analysis_files:
        if os.path.exists(file_path):
            file_size = os.path.getsize(file_path)
            logging.info(f"导出文件: {file_path} (大小: {file_size} bytes)")
        else:
            logging.warning(f"文件不存在: {file_path}")
    
    # 创建汇总报告
    summary_report = {
        'analysis_timestamp': datetime.now().isoformat(),
        'analysis_files': analysis_files,
        'total_analyses': len(analysis_files)
    }
    
    # 保存汇总报告
    import json
    with open('/var/lib/ngods/stage/analysis_summary.json', 'w') as f:
        json.dump(summary_report, f, indent=2)
    
    logging.info(f"成功导出 {len(analysis_files)} 个分析文件")
    
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