# Databricks notebook source
from pyspark.sql.functions import col,dense_rank,avg,round,countDistinct
from pyspark.sql import dataframe
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "Clientid",
"fs.azure.account.oauth2.client.secret": 'Secret',
"fs.azure.account.oauth2.client.endpoint": "endpoint"}


dbutils.fs.mount(
source = "abfss://tokyo-olympic-data@tokyoolympoicdata.dfs.core.windows.net", # contrainer@storageacc
mount_point = "/mnt/tokyoolymic",
extra_configs = configs)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "/mnt/tokyoolymic"

# COMMAND ----------

spark

# COMMAND ----------

athletes=spark.read.format('csv').option('header','True').option('inferSchema','True').load('/mnt/tokyoolymic/raw-data/Athletes.csv')
coaches=spark.read.format('csv').option('header','True').option('inferSchema','True').load('/mnt/tokyoolymic/raw-data/Coaches.csv')
entriesgender=spark.read.format('csv').option('header','True').option('inferSchema','True').load('/mnt/tokyoolymic/raw-data/EntriesGender.csv')
medals=spark.read.format('csv').option('header','True').option('inferSchema','True').load('/mnt/tokyoolymic/raw-data/Medals.csv')
teams=spark.read.format('csv').option('header','True').option('inferSchema','True').load('/mnt/tokyoolymic/raw-data/Teams.csv')

# COMMAND ----------

athletes.show()

# COMMAND ----------

athletes.printSchema()

# COMMAND ----------

coaches.show()

# COMMAND ----------

coaches.printSchema()

# COMMAND ----------

entriesgender.show()

# COMMAND ----------

entriesgender.printSchema()

# COMMAND ----------

medals.show()

# COMMAND ----------

medals.printSchema()

# COMMAND ----------

teams.show()

# COMMAND ----------

teams.printSchema()

# COMMAND ----------


# Find the top 5 countries with the highest number of gold medals
top_gold_medal=medals.withColumn("rn",dense_rank().over(Window.orderBy(col("Gold").desc()))).filter(col("rn")<6).select(col('TeamCountry'),col('Gold'),col('rn'))
top_gold_medal.show()



# COMMAND ----------

# Calculate the average number of entries by gender for each discipline
avg_entries_discipline=entriesgender.withColumn("avg_male",round(col("Male")/col("Total"),2))\
.withColumn("avg_female",round(col("Female")/col("Total"),2))
avg_entries_discipline.show()

# COMMAND ----------

#Distinct events
events=teams.select(col("Event")).distinct()
events.show()

# COMMAND ----------

#count Distinct events
Toteventscount=teams.select(countDistinct(col("Event")).alias('count'))
Toteventscount.show()

# COMMAND ----------

athletes.repartition(1).write.mode("overwrite").option("header","True").csv("/mnt/tokyoolymic/transformed-data/athletes")

# COMMAND ----------

if top_gold_medal is not None:
    top_gold_medal.write.format("csv").option("header", "True").mode('overwrite').save("/mnt/tokyoolymic/transformed-data/top_gold_medal")
else:
    print("top_gold_medal DataFrame is None")

if avg_entries_discipline is not None:
    avg_entries_discipline.write.format("csv").option("header", "True").mode('overwrite').save("/mnt/tokyoolymic/transformed-data/avg_entries_discipline")
else:
    print("avg_entries_discipline DataFrame is None")

if events is not None:
    events.write.format("csv").option("header", "True").save("/mnt/tokyoolymic/transformed-data/events")
else:
    print("events DataFrame is None")

if Toteventscount is not None:
    Toteventscount.write.format("csv").option("header", "True").save("/mnt/tokyoolymic/transformed-data/Toteventscount")
else:
    print("Toteventscount DataFrame is None")
