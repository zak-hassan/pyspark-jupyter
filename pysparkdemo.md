

```python
import pyspark
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, SQLContext

spark = SparkSession.builder.master("spark://sparkly:7077").getOrCreate()
sc = spark.sparkContext
```


```python
df = spark.read.option("header","true").csv("datasets/monthly_births_by_state.csv")
```


```python
df.show()
```

    +-------+----+---------+---------------------------------+
    |  State|Year|    Month|Provisional Number of Live Births|
    +-------+----+---------+---------------------------------+
    |ALABAMA|2016|  January|                             4816|
    |ALABAMA|2016| February|                             4690|
    |ALABAMA|2016|    March|                             4847|
    |ALABAMA|2016|    April|                             4536|
    |ALABAMA|2016|      May|                             4789|
    |ALABAMA|2016|     June|                             5057|
    |ALABAMA|2016|     July|                             5038|
    |ALABAMA|2016|   August|                             5331|
    |ALABAMA|2016|September|                             5242|
    | ALASKA|2016|  January|                              943|
    | ALASKA|2016| February|                              829|
    | ALASKA|2016|    March|                              966|
    | ALASKA|2016|    April|                              915|
    | ALASKA|2016|      May|                              959|
    | ALASKA|2016|     June|                              986|
    | ALASKA|2016|     July|                              957|
    | ALASKA|2016|   August|                              952|
    | ALASKA|2016|September|                              951|
    |ARIZONA|2016|  January|                             7082|
    |ARIZONA|2016| February|                             6765|
    +-------+----+---------+---------------------------------+
    only showing top 20 rows
    



```python
df.createOrReplaceTempView("births")

sqlDF = spark.sql("SELECT * FROM births WHERE State == 'ALABAMA'")
sqlDF.show()

```

    +-------+----+---------+---------------------------------+
    |  State|Year|    Month|Provisional Number of Live Births|
    +-------+----+---------+---------------------------------+
    |ALABAMA|2016|  January|                             4816|
    |ALABAMA|2016| February|                             4690|
    |ALABAMA|2016|    March|                             4847|
    |ALABAMA|2016|    April|                             4536|
    |ALABAMA|2016|      May|                             4789|
    |ALABAMA|2016|     June|                             5057|
    |ALABAMA|2016|     July|                             5038|
    |ALABAMA|2016|   August|                             5331|
    |ALABAMA|2016|September|                             5242|
    +-------+----+---------+---------------------------------+
    

