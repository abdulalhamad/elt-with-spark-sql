# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

import pyspark.sql.functions as F
import re

course_name = "eltsql"
URI = "wasbs://courseware@dbacademy.blob.core.windows.net/elt-with-spark-sql/v01"

#notebook_path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
#notebook_name = notebook_path.split("/")[-1]
#lesson_name = re.sub("[^a-zA-Z0-9]", "_", notebook_name)
#for i in range(10): lesson_name = lesson_name.replace("__", "_")

username = spark.sql("SELECT current_user()").first()[0]
clean_username = re.sub("[^a-zA-Z0-9]", "_", username)
database = f"""{clean_username}_dbacademy_{course_name}"""
userhome = f"dbfs:/user/{username}/dbacademy/{course_name}"

print(f"username:     {username}")
print(f"database:     {database}")
print(f"userhome:     {userhome}")

class BuildPaths:

    def __init__(self, userhome=userhome):
        self.userhome = userhome
        
        self.source = f"{self.userhome}/source_datasets"
        self.base_path=f"{self.userhome}/tables"
        
        self.sales_table_path = f"{self.base_path}/sales"
        self.users_table_path = f"{self.base_path}/users"
        self.events_raw_table_path = f"{self.base_path}/events_raw"
        self.events_clean_table_path = f"{self.base_path}/events_clean"
        self.transactions_table_path = f"{self.base_path}/transactions"        
        self.clickpaths_table_path = f"{self.base_path}/clickpaths"
        
    def set_hive_variables(self):
        for (k, v) in self.__dict__.items():
            spark.sql(f"SET c.{k} = {v}")
  
    def __repr__(self):
        return self.__dict__.__repr__().replace(", ", ",\n")

def path_exists(path):
    try:
        return len(dbutils.fs.ls(path)) >= 0
    except Exception:
        return False

# COMMAND ----------

Paths = BuildPaths()

# COMMAND ----------

dbutils.widgets.text("mode", "default")
mode = dbutils.widgets.get("mode")

if mode == "reset" or mode == "cleanup":
    # Drop the database and remove all data for both reset and cleanup
    print(f"Removing previously installed datasets from\n{source}")    
    spark.sql(f"DROP DATABASE IF EXISTS {database} CASCADE")
    dbutils.fs.rm(Paths.userhome, True)
    
if mode != "cleanup":
    # We are not cleaning up so we want to setup the environment
    
    # RESET is in case we want to force a reset
    # not-existing for net-new install
    if mode == "reset" or not path_exists(Paths.source): 
        print(f"\nInstalling datasets to\n{source}")
        print(f"""\nNOTE: The datasets that we are installing are located in Washington, USA - depending on the 
      region that your workspace is in, this operation can take as little as 3 minutes and 
      upwards to 6 minutes, but this is a one-time operation.""")
        
        dbutils.fs.cp(URI, Paths.source, True)
        print(f"""\nThe install of the datasets completed successfully.""") 

    # Create the database and use it.
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database} LOCATION '{Paths.userhome}/db'")
    spark.sql(f"USE {database}")
    
    # Once the database is created, init the hive variables
    Paths.set_hive_variables()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
