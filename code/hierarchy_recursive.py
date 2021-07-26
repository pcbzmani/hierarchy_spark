from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("pySpark").getOrCreate()

df=spark.read\
.format('csv')\
.option('header',True)\
.option("TreatEmptyValuesAsNulls", True)\
.option("IgnoreLeadingWhiteSpace", True)\
.option("IgnoreTrailingWhiteSpace", True)\
.load('F:/employee.csv')

from pyspark.sql.functions import *

join_type='left_outer'

# initial DataFrame
empDF = df \
  .withColumnRenamed('EmployeeID', 'level_0') \
  .withColumnRenamed('SupervisiorID', 'level_1')
  

i = 1

# Loop Through if you dont know recusrsive depth
while i<5:
    this_level = 'level_{}'.format(i)
    next_level = 'level_{}'.format(i+1)
    next_level_1 = 'level_{}_1'.format(i+1)
    emp_level = df \
    .withColumnRenamed('EmployeeID', this_level) \
    .withColumnRenamed('SupervisiorID', next_level)
    empDF = empDF.join(emp_level, on=this_level, how=join_type)
    empDF = empDF.select('*', coalesce(empDF[next_level],empDF[this_level]).alias(next_level_1))
    empDF = empDF.drop(next_level)
    empDF = empDF.withColumnRenamed(next_level_1,next_level)
    i+=1

empDF.select('level_0','level_1','level_2','level_3','level_4','level_5').show()

+-------+-------+-------+-------+-------+-------+
|level_0|level_1|level_2|level_3|level_4|level_5|
+-------+-------+-------+-------+-------+-------+
|      1|      2|      4|     17|     20|     20|
|      2|      4|     17|     20|     20|     20|
|      8|      6|      3|     15|     20|     20|
|      9|      5|     10|     20|     20|     20|
|      6|      3|     15|     20|     20|     20|
|      5|     10|     20|     20|     20|     20|
|      4|     17|     20|     20|     20|     20|
|      3|     15|     20|     20|     20|     20|
|     10|     20|     20|     20|     20|     20|
|     15|     20|     20|     20|     20|     20|
|     17|     20|     20|     20|     20|     20|
|     16|     21|     21|     21|     21|     21|
|     14|     12|     12|     12|     12|     12|
|     13|     11|     11|     11|     11|     11|
+-------+-------+-------+-------+-------+-------+

spark.stop()
