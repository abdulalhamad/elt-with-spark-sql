# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ./setup

# COMMAND ----------

def migrate_historical():
  spark.sql(f"""
  CREATE OR REPLACE TABLE sales
  LOCATION "{Paths.sales_table_path}" AS
  SELECT * FROM parquet.`{Paths.source}/sales/sales.parquet`
  """)

  spark.sql(f"""
  CREATE OR REPLACE TABLE users
  LOCATION "{Paths.users_table_path}" AS
  SELECT current_timestamp() updated, *
  FROM parquet.`{Paths.source}/users/users.parquet`
  """)

  spark.sql(f"""
  CREATE OR REPLACE TABLE events_clean
  LOCATION "{Paths.events_clean_table_path}" AS
  SELECT * FROM parquet.`{Paths.source}/events/events.parquet`
  """)

# COMMAND ----------

if mode != "clean":
  migrate_historical()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
