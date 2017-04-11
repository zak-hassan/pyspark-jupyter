
# coding: utf-8

# In[22]:

import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, SQLContext

spark = SparkSession.builder.master("spark://sparkly:7077").getOrCreate()
sc = spark.sparkContext


# In[23]:

df = spark.read.option("header","true").csv("datasets/monthly_births_by_state.csv")


# In[28]:

df.show()


# In[33]:

df.createOrReplaceTempView("births")

sqlDF = spark.sql("SELECT * FROM births WHERE State == 'ALABAMA'")
sqlDF.show()

