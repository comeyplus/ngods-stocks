# ngods stock market demo 
This repository contains a stock market analysis demo of the ngods data stack. The demo performs the following steps:

1. Download selected stock symbols data from [Yahoo Finance API](https://finance.yahoo.com/).
2. Store the stock data in ngods data warehouse (using [Iceberg](https://iceberg.apache.org/) format).
3. Transform the data (e.g. normalize stock prices) using [dbt](https://www.getdbt.com/).
4. Expose analytics data model using [cube.dev](https://cube.dev/).
5. Visualize data as reports and dashboards using [Metabase](https://www.metabase.com/).
6. Predicts stock prices using ARIMA in Apache Spark.

The demo is packaged as [docker-compose](https://github.com/docker/compose) script that downloads, installs, and runs all components of the data stack. The data workflows can be orchestrated using either [Dagster](https://dagster.io/) or [Apache Airflow](https://airflow.apache.org/).

## UPDATES
- 2023-02-03: 
    - Upgrade to Apache Iceberg 1.1.0
    - Upgrade to Trino 406
    - Migrated to the new JDBC catalog (removed the heavyweigt Hive Metastore)

# ngods
ngods stands for New Generation Opensource Data Stack. It includes the following components: 

- [Apache Spark](https://spark.apache.org) for data transformation 
- [DuckDB](https://duckdb.org/) for embedded analytics (Airflow ecosystem)
- [Apache Iceberg](https://iceberg.apache.org) as a data storage format 
- [Trino](https://trino.io/) for federated data query 
- [dbt](https://www.getdbt.com/) for ELT 
- [Dagster](https://dagster.io/) for data orchestration 
- [Apache Airflow](https://airflow.apache.org/) for workflow orchestration and scheduling
- [cube.dev](https://cube.dev/) for data analysis and semantic data model 
- [Metabase](https://www.metabase.com/) for self-service data visualization (dashboards) 
- [Apache Superset](https://superset.apache.org/) for modern data exploration and visualization platform
- [Minio](https://min.io) for local S3 storage 

![ngods components](./img/ngods.architecture.png)

ngods is open-sourced under a [BSD license](https://github.com/zsvoboda/ngods-stocks/blob/main/LICENSE) and it is distributed as a docker-compose script that supports Intel and ARM architectures.

# Running the demo
ngods requires a machine with at least 16GB RAM and Intel or Arm 64 CPU running [Docker](https://www.docker.com/). It requires [docker-compose](https://github.com/docker/compose).

## 环境变量设置

在启动服务之前，您需要设置环境变量：

```bash
# 复制示例文件
cp env.local.sample env.local

# 编辑环境变量文件，替换所有 YOUR_* 占位符
nano env.local
```

更多详细信息请参考 [ENV_SETUP.md](./ENV_SETUP.md) 文件。

### 验证环境变量设置

在启动服务之前，您可以运行测试脚本验证环境变量是否正确设置：

```bash
./test_env.sh
```

## 启动服务

1. Clone the [ngods repo](https://github.com/zsvoboda/ngods-stocks)

```bash
git clone https://github.com/zsvoboda/ngods-stocks.git
```

2. 设置环境变量（见上文）

3. 启动服务

```bash
cd ngods-stocks

# 使用启动脚本（自动处理环境变量和数据库初始化）
./start.sh
```

**NOTE:** This can take quite long depending on your network speed.

3. Stop the data stack via the `docker-compose down` command

```bash
docker-compose down
```

4. Execute the data pipeline from the Dagster console at http://localhost:3070/ with [this yaml config file](./projects/dagster/e2e.yaml).

![Dagster e2e](./img/demo/dagster.e2e.png)

Cut and paste the content of the [e2e.yaml file](./projects/dagster/e2e.yaml) to this [Dagster UI console page](http://localhost:3070/workspace/workspace@workspace.py/jobs/e2e/playground) and start the data pipeline by clicking the `Launch Run` button. 

**NOTE:** You can customize the list of stock symbols that will be downloaded. 

5. Review and customize the [cube.dev metrics, and dimensions](./conf/cube/schema/). Test these metrics in the [cube.dev playground](http://localhost:4000/#/build?query={%22measures%22:[%22StockMarketsMonthly.price_close_relative_avg%22],%22timeDimensions%22:[{%22dimension%22:%22StockMarketsMonthly.dt%22,%22granularity%22:%22month%22,%22dateRange%22:[%222014-09-01%22,%222022-07-03%22]}],%22dimensions%22:[%22StockMarketsMonthly.symbol%22],%22filters%22:[{%22member%22:%22StockMarketsMonthly.symbol%22,%22operator%22:%22equals%22,%22values%22:[%22AAPL%22,%22GC=F%22,%22BTC-USD%22]}],%22order%22:[[%22StockMarketsMonthly.symbol%22,%22asc%22],[%22StockMarketsMonthly.dt%22,%22desc%22]]}).

![cube.dev playground](./img/demo/cube.playground.png)

See the [cube.dev documentation](https://cube.dev/docs/) for more information.

6. Check out the Metabase [data visualizations](http://localhost:3030/question#eyJkYXRhc2V0X3F1ZXJ5Ijp7InR5cGUiOiJuYXRpdmUiLCJuYXRpdmUiOnsicXVlcnkiOiJzZWxlY3QgXG4gICAgICAgIGR0LCBcbiAgICAgICAgc3ltYm9sLCBcbiAgICAgICAgcHJpY2VfY2xvc2VfcmVsYXRpdmVfYXZnIFxuICAgIGZyb20gU3RvY2tNYXJrZXRzTW9udGhseVxuICAgIHdoZXJlIFxuICAgICAgICBzeW1ib2wgaW4gKCdBQVBMJywgJ0JUQy1VU0QnLCAnR0M9RicpIGFuZCBcbiAgICAgICAgZHQgPj0gJzIwMTQtMDktMDEnXG4gICAgb3JkZXIgYnkgZHQsIHN5bWJvbFxuICAgICIsInRlbXBsYXRlLXRhZ3MiOnt9fSwiZGF0YWJhc2UiOjN9LCJkaXNwbGF5IjoibGluZSIsImRpc3BsYXlJc0xvY2tlZCI6dHJ1ZSwidmlzdWFsaXphdGlvbl9zZXR0aW5ncyI6eyJncmFwaC5kaW1lbnNpb25zIjpbImR0Iiwic3ltYm9sIl0sImdyYXBoLm1ldHJpY3MiOlsicHJpY2VfY2xvc2VfcmVsYXRpdmVfYXZnIl0sImdyYXBoLnhfYXhpcy50aXRsZV90ZXh0IjoiRGF0ZSAobW9udGhzKSIsImdyYXBoLnlfYXhpcy50aXRsZV90ZXh0IjoiQ2xvc2UgcHJpY2UgKHJlbGF0aXZlIHRvIEphbiAxc3QgMjAwMCkifSwib3JpZ2luYWxfY2FyZF9pZCI6MzN9) that is connected to the cube.dev analytical model. You can run [SQL queries](https://cube.dev/docs/backend/sql) on top of the cube.dev schema.  
 
 Use username `metabase@ngods.com` and password `metabase1`.

![Metabase](./img/metabase.png)

You can create your own data visualizations and dashboards. See the [Metabase documentation](https://metabase.com/docs/latest) for more information.

7. Explore data with Apache Superset. Access the [Superset interface](http://localhost:8088) for modern data exploration and advanced visualizations.

Use username `admin` and password `admin` to login.

To get started with data exploration:
- Go to **Settings** → **Database Connections** 
- Add a PostgreSQL connection: `postgresql://ngods:ngods@postgres:5432/ngods` (recommended for beginners)
- Or add a Trino connection: `trino://admin@trino:8060/warehouse` (for advanced analytics)
- Use **SQL Lab** to write queries and create visualizations
- Build interactive dashboards with rich chart types

See the [Superset documentation](https://superset.apache.org/docs/) for more information on creating charts and dashboards.

8. Orchestrate workflows with Apache Airflow. Access the [Airflow interface](http://localhost:8080) for workflow management and scheduling.

Use username `admin` and password `admin` to login.

**DuckDB Integration**: Airflow includes DuckDB for embedded analytics. Example DAGs:
- `ngods_stock_pipeline_duckdb.py`: Complete stock data processing
- `ngods_stock_pipeline_duckdb_simple.py`: Simplified version

DuckDB runs as an embedded database within Airflow workers, providing fast analytics without additional services.

To get started with workflow orchestration:
- Review the example DAG `ngods_platform_example` on the **DAGs** page
- Create your own DAGs to automate data pipeline tasks
- Use Airflow to schedule and monitor ETL processes
- Integrate with Spark, dbt, Trino, and other platform components
- Monitor task execution and logs in the **Browse** → **Task Instances** section

Airflow is already configured to connect to your PostgreSQL database and can interact with all platform components through the shared Docker network. See the [Airflow Integration Guide](./AIRFLOW_INTEGRATION.md) for detailed setup and usage instructions.

9. Predict stock close price. Run the [ARIMA time-series prediction model](http://localhost:8888/notebooks/arima.ipynb) notebook that is trained on 29 months of the `Apple:AAPL` stock data and predicts the next month.

![Jupyter ARIMA](./img/jupyter.arima.png)

10. Download [DBeaver](https://dbeaver.io/download/) SQL tool.

11. Connect to the Postgres database that contains the `gold` stage data. Use `jdbc:postgresql://localhost:5432/ngods` JDBC URL with username `ngods` and password `ngods`.

![Postgres JDBC connection](./img/demo/postgres.jdbc.connection.png)

12. Connect to the Trino database that has access to all data stages (`bronze`, `silver`, and `gold` schemas of the `warehouse` database). Use `jdbc:trino://localhost:8060` JDBC URL with username `trino` and password `trino`. 

![Trino JDBC connection](./img/demo/trino.jdbc.connection.png)

![Trino schemas](./img/demo/trino.schemas.png)

13. Connect to the Spark database that is used for data transformations. Use `jdbc:hive2://localhost:10009` JDBC URL with no username and password.

![Spark JDBC connection](./img/demo/spark.jdbc.connection.png)

# Customizing the demo
This chapter contains useful information for customizing the demo.

## ngods directories
Here are few distribution's directories that you may need to customize:

- `conf` configuration of all data stack components
    - `cube` cube.dev schema (semantic model definition)
- `data` main data directory 
    - `airflow` Airflow DAGs, logs, config and plugins
    - `minio` root data directory (contains buckets and file data)
    - `spark` Jupyter notebooks
    - `stage` file stage data. Spark can access this directory via `/var/lib/ngods/stage` path. 
- `projects` dbt, Dagster, and DataHub projects
    - `dagster` Dagster orchestration project
    - `dbt` dbt transformations (one project per each medallion stage: `bronze`, `silver`, and `gold`) 

## ngods endpoints
The data stack has the following endpoints

- Spark
    - http://localhost:8888 - Jupyter notebooks 
    - `jdbc:hive2://localhost:10009` JDBC URL (no username / password)
    - localhost:7077 - Spark API endpoint
    - http://localhost:8061 - Spark master node monitoring page 
    - http://localhost:8062 - Spark slave node monitoring page 
    - http://localhost:18080 - Spark history server page 
- Trino
    - `jdbc:trino://localhost:8060` JDBC URL (username `trino` / no password)
- Postgres
    - `jdbc:postgresql://localhost:5432/ngods` JDBC URL (username `ngods` / password `ngods`)
- Cube.dev
    - http://localhost:4000 - cube.dev development UI 
    - `jdbc:postgresql://localhost:3245/cube` JDBC URL (username `cube` / password `cube`)
- Metabase
    - http://localhost:3030 Metabase UI (username `metabase@ngods.com` / password `metabase1`)
- Superset
    - http://localhost:8088 - Apache Superset UI (username `admin` / password `admin`)
    - **PostgreSQL 连接** (推荐开始使用): `postgresql://ngods:ngods@postgres:5432/ngods` - 用于存储和查询结构化数据
    - **Trino 连接** (高级分析): `trino://admin@trino:8060/warehouse` - 用于大数据查询和分析
- Airflow
    - http://localhost:8080 - Apache Airflow UI (username `admin` / password `admin`)
    - Workflow orchestration and scheduling
    - DAG management and task monitoring
- Dagster
    - http://localhost:3070 - Dagster orchestration UI
- Minio
    - http://localhost:9001 - Minio UI (username `minio` / password `minio123`)

## ngods databases: Spark, Trino, and Postgres
ngods stack includes three database engines: Spark, Trino, and Postgres. Both Spark and Trino have access to Iceberg tables in `warehouse.bronze` and `warehouse.silver` schemas. Trino engine can also access the `analytics.gold` schema in Postgres. Trino can federate queries between the Postgres and Iceberg tables. 

The Spark engine is configured for ELT and pyspark data transformations. 

![Spark](./img/spark.schemas.png)

The Trino engine is configured for data federation between the Iceberg and Postgres tables. Additional catalogs can be [configured](./conf/trino/catalog) as needed. 

![Trino](./img/trino.schemas.png)

The Postgres database has accesses only to the `analytics.gold` schema and it is used for executing analytical queries over the gold data.

## Demo data pipeline
The demo data pipeline is utilizes the [medallion architecture](https://databricks.com/fr/glossary/medallion-architecture) with `bronze`, `silver`, and `gold` data stages. 


![data pipeline](./img/data.pipeline.png)

and consists of the following phases:

1. Data are downloaded from Yahoo Finance REST API to the local Minio bucket ([./data/stage](./data/stage)) using this [Dagster operation](./projects/dagster/download.py).
2. The downloaded CSV file is loaded to the bronze stage Iceberg tables (warehouse.bronze Spark schema) using dbt models that are executed in Spark ([./projects/dbt/bronze](./projects/dbt/bronze/models/in_yahoo_finance.sql)).
3. Silver stage Iceberg tables (warehouse.silver Spark schema) are created using dbt models that are executed in Spark ([./projects/dbt/silver](./projects/dbt/silver/models/stock_markets_with_relative_prices.sql)). 
5. Gold stage Postgres tables (analytics.gold Trino schema) are created using dbt models that are executed in Trino ([./projects/dbt/gold](./projects/dbt/gold/models/stock_markets.sql)).

![DBT models](./img/dbt.models.png)

All data pipeline phases can be orchestrated by either [Dagster](https://www.dagster.io/) or [Apache Airflow](https://airflow.apache.org/) frameworks:

**Dagster** operations, resources and jobs are defined in the [Dagster project](./projects/dagster/). 

![Dagster console](./img/dagster.console.png)

The pipeline is executed by running the e2e job from the Dagster console at http://localhost:3070/ using [this yaml config file](./projects/dagster/e2e.yaml)

**Apache Airflow** provides an alternative workflow orchestration solution with:
- Web-based UI for monitoring and managing workflows
- Powerful scheduling and retry mechanisms  
- Extensive integration with external systems
- **Complete stock pipeline**: A full Airflow version of the Dagster demo pipeline
- Sample DAGs demonstrating platform integration

The **Airflow Stock Pipeline** is a complete port of the Dagster demo, featuring:
- **Modern dependency management**: Uses `uv` and `pyproject.toml` for fast, reliable builds
- **Independent schema namespace**: `airflow_bronze`, `airflow_silver`, `airflow_gold`
- **Identical functionality**: data download, DBT processing, ARIMA prediction
- **Zero conflict**: Runs parallel to existing Dagster workflows
- **One-command setup**: `./quick-start-airflow.sh`

✨ **Quick Start**: `./quick-start-airflow.sh` → http://localhost:8080

See the [Airflow Integration Guide](./AIRFLOW_INTEGRATION.md), [Pipeline Documentation](./AIRFLOW_STOCK_PIPELINE.md), and [Success Summary](./AIRFLOW_INTEGRATION_SUCCESS.md) for detailed information.

## ngods analytics layer
ngods includes [cube.dev](https://cube.dev/) for [semantic data model](./conf/cube/schema), [Metabase](https://www.metabase.com/) and [Apache Superset](https://superset.apache.org/) for self-service analytics (dashboards, reports, and visualizations).

![Analytics](./img/analytics.png)

Analytical (semantic) model is defined in [cube.dev](https://cube.dev/) and is used for executing analytical queries over the gold data.

![cube.dev](./img/cube.png)

[Metabase](https://www.metabase.com/) is connected to the [cube.dev](https://cube.dev/) via [SQL API](https://cube.dev/docs/backend/sql). End users can use it for self-service creation of dashboards, reports, and data visualizations. [Metabase](https://www.metabase.com/) is also directly connected to the gold schema in the Postgres database.

![Metabase](./img/demo/metabase.cube.connection.png)

[Apache Superset](https://superset.apache.org/) provides a modern, enterprise-ready business intelligence web application. It can connect to both PostgreSQL and Trino databases, offering advanced visualization capabilities and SQL Lab for interactive data exploration. Superset supports rich visualizations, dashboard creation, and can handle large-scale analytics workloads through its Trino integration.

## ngods machine learning
[Jupyter Notebooks](https://jupyter.org/) with Scala, Java and Python backends can be used for machine learning.

![Jupyter](./img/jupyter.arima.png)

# Support
Create a [github issue](https://github.com/zsvoboda/ngods-stocks/issues) if you have any questions.
