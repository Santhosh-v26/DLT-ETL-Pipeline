import dlt
from pyspark.sql.functions import *

@dlt.table(
    comment="Silver customers with latest snapshot",
    name='main.dlt_silver.customers'
)

@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
def silver_customers():
    return (dlt.read("main.dlt_bronze.customers")\
            .dropDuplicates(['customer_id'])
    )
    

@dlt.table(
    comment="Silver products with latest snapshot",
    name = 'main.dlt_silver.products'
)

@dlt.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
def silver_products():
    return (dlt.read("main.dlt_bronze.products")\
            .dropDuplicates(['product_id'])
    )

@dlt.table(
    comment="Silver categories with latest snapshot",
    name = 'main.dlt_silver.categories'
)

@dlt.expect_or_drop("valid_category_id", "category_id IS NOT NULL")
def silver_categories():
    return (dlt.read("main.dlt_bronze.categories")\
            .dropDuplicates(['category_id'])
    )
    
dlt.create_streaming_table(
    name="main.dlt_silver.orders",
    comment="Silver orders CDC table"
)


@dlt.view
@dlt.expect_or_drop('valid_order_id','order_id IS NOT NULL')

def silver_orders():
    return dlt.read_stream('main.dlt_bronze.orders')

dlt.apply_changes(
    target="main.dlt_silver.orders",
    source="silver_orders",
    keys=["order_id"],
    sequence_by=col("ingest_time"),
    stored_as_scd_type=1
)

dlt.create_streaming_table(
    name="main.dlt_silver.payments",
    comment="Silver orders CDC table"
)


@dlt.view
@dlt.expect_or_drop('valid_payment_id','payment_id IS NOT NULL')

def silver_payments():
    return dlt.read_stream('main.dlt_bronze.payments')

dlt.apply_changes(
    target="main.dlt_silver.payments",
    source="silver_payments",
    keys=["payment_id"],
    sequence_by=col("ingest_time"),
    stored_as_scd_type=1
)