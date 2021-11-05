# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %run ./setup-load

# COMMAND ----------

def update_users():
  
  spark.sql(f"""  
    SELECT user_id, user_first_touch_timestamp, max(email) email, max(updated) updated
    FROM users_update
    GROUP BY user_id, user_first_touch_timestamp
  """).createOrReplaceTempView("deduped_users")
  
  spark.sql(f"""
    MERGE INTO users a
    USING deduped_users b
    ON a.user_id = b.user_id
    WHEN MATCHED AND a.email IS NULL AND b.email IS NOT NULL THEN
      UPDATE SET email = b.email, updated = b.updated
    WHEN NOT MATCHED THEN
    INSERT *
  """)

update_users()

# COMMAND ----------

def bronze_to_silver():

  spark.sql(f"""  
    SELECT json.* FROM (
    SELECT max(json) json FROM json_payload
    GROUP BY json.user_id, json.event_timestamp)
  """).createOrReplaceTempView("deduped_events")
  
  spark.sql(f"""
    MERGE INTO events_clean a
    USING deduped_events b
    ON a.user_id = b.user_id AND a.event_timestamp = b.event_timestamp
    WHEN NOT MATCHED THEN INSERT *
  """)  
  
bronze_to_silver()


# COMMAND ----------

if mode != "clean":
  update_users()
  bronze_to_silver()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2021 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
