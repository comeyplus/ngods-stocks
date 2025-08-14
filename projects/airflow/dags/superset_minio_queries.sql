-- Superset中使用pg_duckdb查询MinIO中的Parquet文件
-- 这些查询可以在Superset的SQL Lab中执行

-- 首先配置MinIO S3连接
SELECT duckdb.create_simple_secret(
    type          := 'S3',
    key_id        := 'minio',
    secret        := 'minio123',
    region        := 'us-east-1',
    endpoint      := 'http://minio:9000'
);

-- 1. 查询技术指标分析数据
SELECT 
    symbol,
    date,
    close,
    ma_20,
    rsi,
    trend_signal,
    rsi_signal,
    bb_signal
FROM read_parquet('s3://stock-analytics/technical_analysis.parquet')
WHERE date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY symbol, date DESC;

-- 2. 查询风险评估数据
SELECT 
    symbol,
    trading_days,
    avg_daily_return,
    volatility,
    risk_level,
    sharpe_ratio
FROM read_parquet('s3://stock-analytics/risk_analysis.parquet')
ORDER BY volatility DESC;

-- 3. 查询相关性分析数据
SELECT 
    symbol1,
    symbol2,
    correlation
FROM read_parquet('s3://stock-analytics/correlation_analysis.parquet')
WHERE ABS(correlation) > 0.5
ORDER BY ABS(correlation) DESC;

-- 4. 查询趋势分析数据
SELECT 
    symbol,
    date,
    close,
    change_5d,
    change_10d,
    change_20d,
    trend_direction
FROM read_parquet('s3://stock-analytics/trend_analysis.parquet')
WHERE date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY symbol, date DESC;

-- 5. 查询成交量分析数据
SELECT 
    symbol,
    date,
    volume,
    volume_change_pct,
    volume_vs_20d_avg,
    volume_signal
FROM read_parquet('s3://stock-analytics/volume_analysis.parquet')
WHERE date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY symbol, date DESC;

-- 6. 查询股票推荐数据
SELECT 
    symbol,
    date,
    close,
    technical_score,
    recommendation
FROM read_parquet('s3://stock-analytics/stock_recommendations.parquet')
WHERE date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY technical_score DESC, symbol;

-- 7. 查询市场概览数据
SELECT 
    total_stocks,
    total_data_points,
    avg_market_price,
    market_volatility,
    min_price,
    max_price
FROM read_parquet('s3://stock-analytics/market_summary.parquet');

-- 8. 查询表现最佳股票
SELECT 
    symbol,
    weekly_return
FROM read_parquet('s3://stock-analytics/top_performers.parquet')
ORDER BY weekly_return DESC;

-- 9. 创建视图示例（如果支持）
-- CREATE VIEW technical_analysis_view AS
-- SELECT * FROM read_parquet('s3://stock-analytics/technical_analysis.parquet');

-- 10. 复杂查询：结合多个分析结果
WITH technical AS (
    SELECT symbol, date, close, rsi, trend_signal
    FROM read_parquet('s3://stock-analytics/technical_analysis.parquet')
    WHERE date >= CURRENT_DATE - INTERVAL '7 days'
),
risk AS (
    SELECT symbol, volatility, risk_level, sharpe_ratio
    FROM read_parquet('s3://stock-analytics/risk_analysis.parquet')
),
recommendations AS (
    SELECT symbol, technical_score, recommendation
    FROM read_parquet('s3://stock-analytics/stock_recommendations.parquet')
    WHERE date >= CURRENT_DATE - INTERVAL '7 days'
)
SELECT 
    t.symbol,
    t.close,
    t.rsi,
    t.trend_signal,
    r.volatility,
    r.risk_level,
    r.sharpe_ratio,
    rec.technical_score,
    rec.recommendation
FROM technical t
JOIN risk r ON t.symbol = r.symbol
JOIN recommendations rec ON t.symbol = rec.symbol
ORDER BY rec.technical_score DESC, r.volatility ASC;

-- 11. 查询原始股票数据
SELECT 
    symbol,
    date,
    open,
    high,
    low,
    close,
    volume
FROM read_parquet('s3://stock-data/stock_data_latest.parquet')
WHERE date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY symbol, date DESC;

-- 注意事项：
-- 1. 确保MinIO服务正在运行
-- 2. 确保pg_duckdb已正确配置S3连接
-- 3. 路径格式为：s3://bucket-name/object-name
-- 4. 时间间隔使用单引号：INTERVAL '30 days'
-- 5. 如果遇到权限问题，检查MinIO的访问策略 