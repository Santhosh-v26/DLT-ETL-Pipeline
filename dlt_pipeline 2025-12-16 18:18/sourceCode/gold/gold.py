import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

#report 1
@dlt.table(
    name="main.dlt_gold.customer_summary",
    comment="customer summary"
)
def gold_customer_summary():
    customers = dlt.read("main.dlt_silver.customers")
    orders = dlt.read("main.dlt_silver.orders")
    payments = dlt.read("main.dlt_silver.payments")

    customer_orders = orders.groupBy("customer_id").agg(
        count("order_id").alias("total_orders"),
        sum("amount").alias("total_order_amount")
    )

    customer_payments = payments.filter(col("payment_status")=="SUCCESS").groupBy("customer_id").agg(
        count("payment_id").alias("total_payments"),
        sum("amount").alias("total_amount_paid")
    )

    return customers.join(customer_orders, "customer_id", "left") \
                    .join(customer_payments, "customer_id", "left")

#report 2
@dlt.table(
    name="main.dlt_gold.product_summary",
    comment="product summary"
)
def gold_product_summary():
    products = dlt.read("main.dlt_silver.products")
    orders = dlt.read("main.dlt_silver.orders")

    product_agg = orders.groupBy("product_id").agg(
        count("order_id").alias("total_orders"),
        sum("amount").alias("total_revenue")
    )

    return products.join(product_agg, "product_id", "left")

#report 3
@dlt.table(
    name="main.dlt_gold.category_summary",
    comment="Simple category summary"
)
def gold_category_summary():
    categories = dlt.read("main.dlt_silver.categories")
    products = dlt.read("main.dlt_silver.products")
    orders = dlt.read("main.dlt_silver.orders")

    # Join orders with products to get category
    orders_products = orders.join(products.select("product_id","category_id"), "product_id", "left")

    category_agg = orders_products.groupBy("category_id").agg(
        countDistinct("product_id").alias("total_products"),
        count("order_id").alias("total_orders"),
        sum("amount").alias("total_revenue")
    )

    return categories.join(category_agg, "category_id", "left")

