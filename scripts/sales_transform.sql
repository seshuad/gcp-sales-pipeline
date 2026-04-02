-- Create external tables pointing to raw CSV data in GCS

CREATE DATABASE IF NOT EXISTS sales;

DROP TABLE IF EXISTS sales.orders;
CREATE EXTERNAL TABLE sales.orders (
    order_id     STRING,
    customer_id  STRING,
    product_id   STRING,
    quantity     INT,
    unit_price   DOUBLE,
    order_date   DATE,
    status       STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'gs://sales-pipeline-demo-261244601320/raw/orders/'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP TABLE IF EXISTS sales.customers;
CREATE EXTERNAL TABLE sales.customers (
    customer_id  STRING,
    name         STRING,
    email        STRING,
    region       STRING,
    signup_date  DATE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'gs://sales-pipeline-demo-261244601320/raw/customers/'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP TABLE IF EXISTS sales.products;
CREATE EXTERNAL TABLE sales.products (
    product_id    STRING,
    product_name  STRING,
    unit_price    DOUBLE,
    category      STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'gs://sales-pipeline-demo-261244601320/raw/products/'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE DATABASE IF NOT EXISTS sales_processed;

-- Aggregation 1: revenue by region
CREATE OR REPLACE TABLE sales_processed.revenue_by_region
USING iceberg
LOCATION 'gs://sales-pipeline-demo-261244601320/processed/revenue_by_region'
TBLPROPERTIES (
    'write.format.default'             = 'parquet',
    'write.parquet.compression-codec'  = 'snappy'
)
AS
SELECT
    c.region,
    COUNT(o.order_id)                        AS total_orders,
    ROUND(SUM(o.quantity * o.unit_price), 2) AS total_revenue,
    ROUND(AVG(o.quantity * o.unit_price), 2) AS avg_order_value
FROM sales.orders o
JOIN sales.customers c ON o.customer_id = c.customer_id
WHERE o.status = 'completed'
GROUP BY c.region
ORDER BY total_revenue DESC;

-- Aggregation 2: revenue by product
CREATE OR REPLACE TABLE sales_processed.revenue_by_product
USING iceberg
LOCATION 'gs://sales-pipeline-demo-261244601320/processed/revenue_by_product'
TBLPROPERTIES (
    'write.format.default'             = 'parquet',
    'write.parquet.compression-codec'  = 'snappy'
)
AS
SELECT
    o.product_id,
    p.product_name,
    p.category,
    SUM(o.quantity)                          AS units_sold,
    ROUND(SUM(o.quantity * o.unit_price), 2) AS total_revenue
FROM sales.orders o
JOIN sales.products p ON o.product_id = p.product_id
WHERE o.status = 'completed'
GROUP BY o.product_id, p.product_name, p.category
ORDER BY total_revenue DESC;

-- Aggregation 3: monthly revenue trend
CREATE OR REPLACE TABLE sales_processed.monthly_trend
USING iceberg
LOCATION 'gs://sales-pipeline-demo-261244601320/processed/monthly_trend'
TBLPROPERTIES (
    'write.format.default'             = 'parquet',
    'write.parquet.compression-codec'  = 'snappy'
)
AS
SELECT
    DATE_FORMAT(o.order_date, 'yyyy-MM')     AS month,
    COUNT(o.order_id)                        AS total_orders,
    ROUND(SUM(o.quantity * o.unit_price), 2) AS total_revenue
FROM sales.orders o
WHERE o.status = 'completed'
GROUP BY DATE_FORMAT(o.order_date, 'yyyy-MM')
ORDER BY month;
