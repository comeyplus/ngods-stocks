-- Superset MinIO配置脚本
-- 在Superset SQL Lab中执行此脚本来配置MinIO连接

-- 1. 配置MinIO S3连接
-- 这个配置只需要执行一次
SELECT duckdb.create_simple_secret(
    type          := 'S3',
    key_id        := 'minio',
    secret        := 'minio123',
    region        := 'us-east-1',
    endpoint      := 'http://minio:9000'
);

-- 2. 验证连接 - 列出所有bucket
SELECT * FROM s3_list_directory('s3://');

-- 3. 检查stock-data bucket中的文件
SELECT * FROM s3_list_directory('s3://stock-data/');

-- 4. 检查stock-analytics bucket中的文件
SELECT * FROM s3_list_directory('s3://stock-analytics/');

-- 5. 测试读取原始股票数据
SELECT 
    symbol,
    date,
    close,
    volume
FROM read_parquet('s3://stock-data/stock_data_latest.parquet')
LIMIT 10;

-- 6. 测试读取技术分析数据
SELECT 
    symbol,
    date,
    close,
    ma_20,
    rsi
FROM read_parquet('s3://stock-analytics/technical_analysis.parquet')
LIMIT 10;

-- 配置完成后，您就可以使用以下查询模式：
-- SELECT * FROM read_parquet('s3://bucket-name/file.parquet')
-- WHERE conditions
-- ORDER BY columns; 