#!/usr/bin/env python3
"""
示例Spark应用
演示如何在Airflow中通过SparkSubmitOperator提交到aio集群
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count
import sys

def main():
    """主函数"""
    # 创建SparkSession
    spark = SparkSession.builder \
        .appName("AirflowSparkExample") \
        .getOrCreate()
    
    print("=" * 50)
    print("Spark应用开始执行")
    print(f"Spark版本: {spark.version}")
    print(f"应用名称: {spark.conf.get('spark.app.name')}")
    print("=" * 50)
    
    try:
        # 创建示例数据
        data = [
            ("Alice", 25, "Engineer", 50000),
            ("Bob", 30, "Manager", 60000),
            ("Charlie", 35, "Engineer", 55000),
            ("Diana", 28, "Analyst", 45000),
            ("Eve", 32, "Manager", 65000),
            ("Frank", 29, "Engineer", 52000),
            ("Grace", 31, "Analyst", 48000),
            ("Henry", 33, "Manager", 70000)
        ]
        
        # 创建DataFrame
        df = spark.createDataFrame(data, ["name", "age", "role", "salary"])
        
        print("原始数据:")
        df.show()
        
        # 数据分析
        print("\n按角色统计:")
        role_stats = df.groupBy("role") \
            .agg(
                count("*").alias("count"),
                avg("age").alias("avg_age"),
                avg("salary").alias("avg_salary")
            )
        role_stats.show()
        
        # 年龄分析
        print("\n年龄分析:")
        age_stats = df.groupBy("age") \
            .agg(count("*").alias("count")) \
            .orderBy("age")
        age_stats.show()
        
        # 薪资分析
        print("\n薪资分析:")
        salary_stats = df.select(
            avg("salary").alias("avg_salary"),
            df.salary.min().alias("min_salary"),
            df.salary.max().alias("max_salary")
        )
        salary_stats.show()
        
        # 复杂查询示例
        print("\n高薪员工分析 (薪资 > 平均薪资):")
        avg_salary = df.select(avg("salary")).collect()[0][0]
        high_salary_employees = df.filter(col("salary") > avg_salary) \
            .select("name", "role", "salary") \
            .orderBy(col("salary").desc())
        high_salary_employees.show()
        
        print("=" * 50)
        print("Spark应用执行完成")
        print("=" * 50)
        
    except Exception as e:
        print(f"执行过程中出现错误: {e}")
        sys.exit(1)
    finally:
        # 关闭SparkSession
        spark.stop()

if __name__ == "__main__":
    main() 