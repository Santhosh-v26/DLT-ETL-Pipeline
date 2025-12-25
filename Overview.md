📌 Delta Live Tables (DLT) Pipeline Overview

This project implements an end-to-end Delta Live Tables (DLT) pipeline using the Medallion Architecture (Bronze, Silver, Gold) to process both batch and streaming data in a unified and automated manner. The pipeline is designed to ensure data quality, scalability, and reliability while minimizing manual orchestration.

The solution ingests raw data, applies cleansing and change data capture (CDC) logic, and produces analytics-ready datasets for downstream reporting and dashboards.

🏗 Architecture Design

The pipeline follows a three-layer Medallion Architecture:

🔹 Bronze Layer – Raw Ingestion

--Ingests customers, products, and categories as batch data.
--Ingests orders and payments as streaming data using Auto Loader (cloudFiles).
--Adds metadata columns such as:
--ingest_time for sequencing and CDC
--source_file for data lineage
--Stores raw data in Delta tables without transformation.

Purpose: Preserve raw data and provide a reliable ingestion layer.

🔹 Silver Layer – Cleaned & CDC-Enabled Data

--Applies data quality checks using dlt.expect_or_drop to remove invalid records.
--Removes duplicates for master data (customers, products, categories).
--Implements CDC logic for streaming tables (orders, payments) using:

dlt.apply_changes

Primary keys (order_id, payment_id)

ingest_time as the sequencing column

Maintains the latest snapshot of transactional data.

Purpose: Create clean, trustworthy, and up-to-date datasets.

🔹 Gold Layer – Business Aggregations

Builds analytics-ready summary tables for reporting:

Customer Summary: total orders, total order amount, successful payments

Product Summary: total orders and revenue per product

Category Summary: product count, order count, and revenue by category

Uses optimized joins and aggregations on Silver tables.

Purpose: Enable fast querying and dashboard consumption.

⚙️ Key Features of the Pipeline

--Fully managed ETL orchestration using Delta Live Tables
--Supports batch + streaming ingestion in a single pipeline
--Built-in data quality enforcement
--CDC handling using SCD Type 1
--Automatic dependency management between tables
--Fault-tolerant streaming with checkpointing

📊 Business Use Case

This pipeline enables:

--Near real-time visibility into orders and payments
--Reliable customer, product, and category analytics
--Scalable foundation for BI dashboards and reporting tools

🎯 Why Delta Live Tables?

--Reduces manual orchestration effort
--Enforces data quality declaratively

Automatically manages table dependencies

Simplifies production-grade data pipeline development
