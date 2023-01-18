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

##### CLIENT SETUP

# todo extra fields from client table

periods = [
  ('Y', 100)
]

restricted_periods = [
  ('Y', 200),
  # ('W', 1),
  # ('W', 5)
]

#format: dict(category => whitelisted values)
primary_product_categories = {
  # 'famille': ['famille_129', 'famille_132'],
  'categorie': []
}
secondary_product_categories = {
  # 'famille': ['famille_129', 'famille_132'],
  # 'categorie': []
}
product_attributes = {
  'couleur': []
}
web_points_of_sales = [
  17,
]
computing_date = '2030-01-01'

# todo: activity barrette? 

##### GLOBAL SETUP

# period_name : period length in days
PERIOD_LENGTHS_DAY = {
  'Y': 365,
  'M': 30,
  'Q': 90,
  'S': 180,
  'W': 7
}

PRIMARY_AGGREGATION_FIELD = 'uid'

DEFAULT_AGGREGATION = ('amount_paid', F.sum)
DEFAULT_FILTERS = [
  F.col(DEFAULT_AGGREGATION[0]) > 0 # amount paid is positive
]

######## FORMAT: (column_prefix, agg_columns, agg_def, extra_filters)
### column_prefix: prefix that will be used in the column result
### periods: aggregation periods list
### agg_columns: dict ('col_name' => [whitelisted values]) to use for aggregation
###              can be empty (first level of aggregation is by default on 'uid' field)
### agg_def: tuple of (target_column, pyspark aggregation function))
### extra_filters: list of where conditions for applying extra filters (default filter is always applied)
AGGREGATIONS = [
  ##### restricted_periods
  # ('ca', restricted_periods, {}, DEFAULT_AGGREGATION, []),
  # ('ca_category', restricted_periods, primary_product_categories, DEFAULT_AGGREGATION, []), # fe. this will result in ca_category_{category_name}_{period} columns
  # ('ca_category', restricted_periods, secondary_product_categories, DEFAULT_AGGREGATION, []),
  # ('ca_attribute', restricted_periods, product_attributes, DEFAULT_AGGREGATION, []),
  # ('nb_purchase_dates', restricted_periods, {}, ('datetime', F.countDistinct), []),
  # ('nb_purchase_dates_category_', restricted_periods, primary_product_categories, ('datetime', F.countDistinct), []),
  # ('nb_purchase_dates_attribute_', restricted_periods, product_attributes, ('datetime', F.countDistinct), []),
  # ('quantity', restricted_periods, {}, ('quantity', F.sum), [F.col('quantity') > 0]),
  # ('nb_distinct_products', restricted_periods, {}, ('product_id', F.countDistinct), []),
  # ('nb_purchase_dates_web', restricted_periods, {}, ('datetime', F.countDistinct), [F.col('point_of_sales').isin(web_points_of_sales)]), # only bought in web stores
  # ('nb_purchase_dates_store', restricted_periods, {}, ('datetime', F.countDistinct), [~F.col('point_of_sales').isin(web_points_of_sales)]), # not bought in web stores
  # ('last_purchase_date', restricted_periods, {}, ('datetime', F.max), []),
  # ('last_purchase_date_category', restricted_periods, primary_product_categories, ('datetime', F.max), []),
  
  ##### periods
  ('ca', periods, {}, DEFAULT_AGGREGATION, []),
  ('nb_purchase_dates', periods, {}, ('datetime', F.countDistinct), []),
  ('quantity', periods, {}, ('quantity', F.sum), [F.col('quantity') > 0]),
]

# original definitions that are missing
#   197,3:   - name: 'top_buyer' NO PERIOD, PER WHOLE DATASET?, id of customer who bought the most? (volume, money?)
#   210,3:   - name: 'top_buyer_category_{{ d['field'] }}_' SAME AS ABOVE 

DEBUG = True

# join table with catalog and setup computing date
catalog = spark.read.table('splio.catalog')
purchases = spark.read.table('splio.purchases')

df = (
  purchases.join(catalog, ['product_id'])
  .withColumn('computing_date', F.to_date(F.lit(computing_date)))
)

res_df = None
for column_prefix, periods, agg_columns, agg_def, extra_filters in AGGREGATIONS: # across all the predefined aggregations
  agg_columns = agg_columns if agg_columns else {None: None} # perform 1 loop if there are no aggregation cols
  for agg_col, whitelisted in agg_columns.items(): 
    for period_name, period_n in periods: # across all the defined periods
      
      # concatenate default settings with current aggregation settings
      group_cols = [PRIMARY_AGGREGATION_FIELD, agg_col] if agg_col else PRIMARY_AGGREGATION_FIELD
      filters = DEFAULT_FILTERS + extra_filters

      if DEBUG:
        print(f"PER: {period_name}{period_n}, COLS: {group_cols}, WL:{whitelisted}, TAR: {agg_def[1].__name__}({agg_def[0]}), FIL: {extra_filters}, ==> {column_prefix}")

      # apply filters before aggregation
      rdf = df
      for f in filters:
        rdf = rdf.filter(f)

      # filter whitelisted cols (ignore if empty)
      if whitelisted:
        rdf = rdf.where(F.col(agg_col).isin(whitelisted))

      rdf = (
        rdf
          .filter(F.col('date').between( # filter out defined period
            F.date_sub(F.col('computing_date'), period_n * PERIOD_LENGTHS_DAY[period_name]), # substract days from computing_date
            F.col('computing_date') # until computing date (included)
          ))
          .groupBy(group_cols).agg(agg_def[1](agg_def[0]).cast('double').alias('value')) # group by and call aggregation function on specified col
          .withColumn('aggregation', F.concat( # name the column
            F.lit(column_prefix), F.lit('_'),
            F.col(agg_col) if agg_col else F.lit("") ,
            F.lit('_') if agg_col else F.lit(""),
            F.lit(f'{period_name}{period_n}'))
          )
      )
      # drop original aggregation col if present (table will be pivoted)
      if agg_col:
        rdf = rdf.drop(agg_col) 

      res_df = res_df.union(rdf) if res_df else rdf # append to the dataframe

display(res_df.where(F.col('uid') == '746991')) # check for single user (with most purchases)
# res_df = res_df.groupBy('uid').pivot('aggregation').agg(F.first('value')) # pivot table (except uid)
# display(res_df.where(F.col('uid') == '608685'))

# todo: extra columns from client table
# todo: finalize all original aggregations on checklist

###### questions
# todo: fill null values??
# todo: if there are no results found for particular aggregation for any user or period, the column will not be included in the result
#       if there is result for at least single user, the column will be present in the final pivoted table (rest will be NULL)

# COMMAND ----------

for i in {}.items():
  print(i)

# COMMAND ----------

for i in {}.items():
  print(i)

# COMMAND ----------

display(df.where(F.col('uid') == '746991').where(F.col('amount_paid') > 0))

# COMMAND ----------

# nb_purchase_dates ()
display(df.groupBy('uid', 'point_of_sales').count().groupBy('uid').count().sort(F.col('count').desc()))

# COMMAND ----------

display(df.groupBy('uid').agg(F.count('uid').alias('c')).sort(F.col('c').desc()))

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


