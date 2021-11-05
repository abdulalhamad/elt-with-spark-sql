# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ./setup

# COMMAND ----------

if mode != "clean":      
    dbutils.fs.rm(f"{Paths.source}/sales/sales.csv", True)
    dbutils.fs.cp(f"{URI}/sales/sales.csv", f"{Paths.source}/sales/sales.csv", True)   

    (spark
        .read
        .format("parquet")
        .load(f"{Paths.source}/users/users.parquet")
        .repartition(1)
        .write
        .format("org.apache.spark.sql.jdbc")
        .option("url", f"jdbc:sqlite:/{username}_ecommerce.db")
        .option("dbtable", "users")
        .mode("overwrite")
        .save())

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
