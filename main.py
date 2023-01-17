# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.window import Window


# COMMAND ----------

PROJECT_DIR = 'dbfs:/splio/d-centric/'
CUSTOMER = 'customer1-prod'

# COMMAND ----------

# MAGIC %sql CREATE DATABASE IF NOT EXISTS splio

# COMMAND ----------

spark.read.parquet(f'{PROJECT_DIR}/{CUSTOMER}/dc__product__catalog.parquet').write.mode('overwrite').saveAsTable('splio.catalog')

# COMMAND ----------

spark.read.parquet(f'{PROJECT_DIR}/{CUSTOMER}/pre_dpredict__clients.parquet').write.mode('overwrite').saveAsTable('splio.clients')

# COMMAND ----------

display(spark.read.table('splio.catalog'))

# COMMAND ----------

display(spark.read.table('splio.clients'))

# COMMAND ----------

checkpoint_path = "dbfs:/splio/_checkpoint3/"

(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "parquet")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .load("dbfs:/splio/d-centric/customer1-prod/facts_purchases")
  .withColumn('date', F.col('datetime').cast('date'))
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .trigger(availableNow=True)
  .toTable("splio.purchases"))

# COMMAND ----------

display(spark.read.table('splio.purchases').where(F.col('_rescued_data').isNotNull()))

# COMMAND ----------

# MAGIC %sql DROP TABLE splio.purchases

# COMMAND ----------

display(spark.read.table('splio.purchases'))

# COMMAND ----------

# period_name : period length in days
PERIOD_LENGTHS_DAY = {
  'Y': 365,
  'M': 30,
  'Q': 90,
  'S': 180,
  'W': 7
}

PRIMARY_AGGREGATION_FIELD = 'uid'

# ('column_name', pyspark aggregation function) - single value now
MAIN_AGGREGATION = ('amount_paid', F.sum)

# todo implement periods with computing date
restricted_periods = [
  # ('Y', 1),
  ('W', 1),
  # ('W', 2)
]

primary_categories = ['famille']
computing_date = '2023-01-10'

# periods are included in every aggregation by default
# primary aggregation is by default on 'uid' 
# column prefix ; aggregation column name(s) ; 
AGGREGATIONS = {
  ('ca_category', primary_categories, )
}


# join table with catalog and setup computing date
catalog = spark.read.table('splio.catalog')
purchases = spark.read.table('splio.purchases')

df = (
  purchases.join(catalog, ['product_id'])
  .withColumn('computing_date', F.to_date(F.lit(computing_date)))
  .where(F.col(MAIN_AGGREGATION[0]) > 0) # filter out only positive values
)

res_df = None
for period_name, period_n in restricted_periods: # across all the defined periods
  for cat in primary_categories: # across all the primary categories
    rdf = (
      df
        .filter(F.col('date').between( # filter out defined period
          F.date_sub(F.col('computing_date'), period_n * PERIOD_LENGTHS_DAY[period_name]), # substract N * PERIOD_LENGTHS_DAY days from computing_date
          F.col('computing_date') # until computing date (included)
        ))
        .groupBy(PRIMARY_AGGREGATION_FIELD, cat).agg(MAIN_AGGREGATION[1](MAIN_AGGREGATION[0]).alias('value')) # group by and calculate aggregations
        .withColumn('category', F.concat(F.lit('ca_category_'), F.col(cat), F.lit('_'), F.lit(f'{period_name}{period_n}')))
        .drop(cat) # drop original category for pivoting
    )
    # append to the dataframe
    res_df = res_df.union(rdf) if res_df else rdf 

display(res_df.where(F.col('uid') == '746991')) # check for single user
# res_df = res_df.groupBy('uid').pivot('category').agg(F.first('value')) # pivot table (except uid)
# 
display(res_df)

# COMMAND ----------

# nb_purchase_dates ()
display(df.groupBy('uid').agg(F.countDistinct('date')))

# COMMAND ----------

display(df.where(F.col('uid') == '746991').select('datetime', 'amount_paid', 'famille', 'categorie')) # single client check

# COMMAND ----------


w = Window.partitionBy('uid', 'famille')

display(df.withColumn('a', F.sum('amount_paid').over(w)).where(F.col('uid') == '644034'))

# COMMAND ----------


  display(
    df
    
    
  ).select('date').distinct().sort('date')
  )

# COMMAND ----------

"print(agg)

# COMMAND ----------


