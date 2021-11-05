# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ./setup-external

# COMMAND ----------

csv_table_name = "csv_orders"
jdbc_table_name = "jdbc_users"

def parse_csv_orders():
  spark.sql(f"DROP TABLE IF EXISTS {csv_table_name}")

  spark.sql(f"""
  CREATE TABLE {csv_table_name} (
    order_id STRING,
    email STRING,
    transactions_timestamp STRING,
    total_item_quantity INTEGER,
    purchase_revenue_in_usd STRING,
    unique_items STRING,
    items STRING
  ) 
  USING CSV
  OPTIONS (
    header = "true",
    delimiter = "|"
  ) 
  LOCATION "{Paths.source}/sales/sales.csv"
  """)

def connect_jdbc_users():
  spark.sql(f"DROP TABLE IF EXISTS {jdbc_table_name}")

  spark.sql(f"""
  CREATE TABLE {jdbc_table_name} 
  USING org.apache.spark.sql.jdbc
  OPTIONS (
    url "jdbc:sqlite:/{username}_ecommerce.db",
    dbtable "users"
  )
  """)

# COMMAND ----------

if mode != "clean":
  parse_csv_orders()
  connect_jdbc_users()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
