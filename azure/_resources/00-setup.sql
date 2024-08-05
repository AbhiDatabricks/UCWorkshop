-- Databricks notebook source
-- MAGIC %python
-- MAGIC %pip install faker

-- COMMAND ----------

-- MAGIC %python
-- MAGIC catalog = "apjworkshop24"
-- MAGIC sql_statement = f"USE catalog {catalog}"
-- MAGIC spark.sql(sql_statement)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC current_user_id = (
-- MAGIC     dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
-- MAGIC )
-- MAGIC database = current_user_id.split("@")[0].replace(".", "_")+"_upgrade_db"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pyspark.sql.functions as F
-- MAGIC import re
-- MAGIC
-- MAGIC catalog_exists = False
-- MAGIC for r in spark.sql("SHOW CATALOGS").collect():
-- MAGIC     if r['catalog'] == catalog:
-- MAGIC         catalog_exists = True
-- MAGIC
-- MAGIC #As non-admin users don't have permission by default, let's do that only if the catalog doesn't exist (an admin need to run it first)     
-- MAGIC if not catalog_exists:
-- MAGIC     spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
-- MAGIC     spark.sql(f"ALTER CATALOG {catalog} OWNER TO `account users`")
-- MAGIC     spark.sql(f"GRANT CREATE, USAGE on CATALOG {catalog} TO `account users`")
-- MAGIC spark.sql(f"USE CATALOG {catalog}")
-- MAGIC
-- MAGIC print(f"Using catalog {catalog}")
-- MAGIC print(f"creating {database} database")
-- MAGIC spark.sql(f"DROP DATABASE IF EXISTS {catalog}.{database} CASCADE")
-- MAGIC spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog}.{database}")
-- MAGIC spark.sql(f"GRANT CREATE, USAGE on DATABASE {catalog}.{database} TO `account users`")
-- MAGIC spark.sql(f"ALTER SCHEMA {catalog}.{database} OWNER TO `account users`")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC folder = "/dbdemos/uc/delta_dataset"
-- MAGIC spark.sql('drop database if exists hive_metastore.uc_database_to_upgrade cascade')
-- MAGIC #fix a bug from legacy version
-- MAGIC spark.sql(f'drop database if exists {catalog}.uc_database_to_upgrade cascade')
-- MAGIC dbutils.fs.rm("/transactions", True)
-- MAGIC
-- MAGIC print("generating the data...")
-- MAGIC from pyspark.sql import functions as F
-- MAGIC from faker import Faker
-- MAGIC from collections import OrderedDict 
-- MAGIC import uuid
-- MAGIC import random
-- MAGIC fake = Faker()
-- MAGIC
-- MAGIC fake_firstname = F.udf(fake.first_name)
-- MAGIC fake_lastname = F.udf(fake.last_name)
-- MAGIC fake_email = F.udf(fake.ascii_company_email)
-- MAGIC fake_date = F.udf(lambda:fake.date_time_this_month().strftime("%m-%d-%Y %H:%M:%S"))
-- MAGIC fake_address = F.udf(fake.address)
-- MAGIC fake_credit_card_expire = F.udf(fake.credit_card_expire)
-- MAGIC
-- MAGIC fake_id = F.udf(lambda: str(uuid.uuid4()))
-- MAGIC countries = ['FR', 'USA', 'SPAIN']
-- MAGIC fake_country = F.udf(lambda: countries[random.randint(0,2)])
-- MAGIC
-- MAGIC df = spark.range(0, 10000)
-- MAGIC df = df.withColumn("id", F.monotonically_increasing_id())
-- MAGIC df = df.withColumn("creation_date", fake_date())
-- MAGIC df = df.withColumn("customer_firstname", fake_firstname())
-- MAGIC df = df.withColumn("customer_lastname", fake_lastname())
-- MAGIC df = df.withColumn("country", fake_country())
-- MAGIC df = df.withColumn("customer_email", fake_email())
-- MAGIC df = df.withColumn("address", fake_address())
-- MAGIC df = df.withColumn("gender", F.round(F.rand()+0.2))
-- MAGIC df = df.withColumn("age_group", F.round(F.rand()*10))
-- MAGIC df.repartition(3).write.mode('overwrite').format("delta").save(folder+"/users")
-- MAGIC
-- MAGIC
-- MAGIC df = spark.range(0, 10000)
-- MAGIC df = df.withColumn("id", F.monotonically_increasing_id())
-- MAGIC df = df.withColumn("customer_id",  F.monotonically_increasing_id())
-- MAGIC df = df.withColumn("transaction_date", fake_date())
-- MAGIC df = df.withColumn("credit_card_expire", fake_credit_card_expire())
-- MAGIC df = df.withColumn("amount", F.round(F.rand()*1000+200))
-- MAGIC
-- MAGIC df = df.cache()
-- MAGIC spark.sql('create database if not exists hive_metastore.uc_database_to_upgrade')
-- MAGIC df.repartition(3).write.mode('overwrite').format("delta").saveAsTable("hive_metastore.uc_database_to_upgrade.users")
-- MAGIC
-- MAGIC #Note: this requires hard-coded external location.
-- MAGIC #get from cloudLabs team
-- MAGIC #df.repartition(3).write.mode('overwrite').format("delta").save(external_location_path+"/transactions")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #df.repartition(3).write.mode('overwrite').format("delta").save(external_location_path+"/transactions")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Need to switch to hive metastore to avoid having a : org.apache.spark.SparkException: Your query is attempting to access overlapping paths through multiple authorization mechanisms, which is not currently supported.
-- MAGIC spark.sql("USE CATALOG hive_metastore")
-- MAGIC #spark.sql(f"create table if not exists hive_metastore.uc_database_to_upgrade.transactions location '{external_location_path}/transactions'")
-- MAGIC spark.sql(f"create or replace view `hive_metastore`.`uc_database_to_upgrade`.users_view_to_upgrade as select * from hive_metastore.uc_database_to_upgrade.users where id is not null")
-- MAGIC
-- MAGIC spark.sql(f"USE CATALOG {catalog}")
