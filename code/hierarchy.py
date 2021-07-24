# Import SparkSession
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("pySpark").getOrCreate()

# Read spark dataframe
df=spark.read\
.format('csv')\
.option('header',True)\
.option("TreatEmptyValuesAsNulls", True)\
.option("IgnoreLeadingWhiteSpace", True)\
.option("IgnoreTrailingWhiteSpace", True)\
.load('F:/employee.csv')

df1=df
df1=df1.withColumnRenamed('EmployeeID','EmployeeID1').withColumnRenamed('SupervisiorID','SupervisiorID1')
df2=df
df2=df2.withColumnRenamed('EmployeeID','EmployeeID2').withColumnRenamed('SupervisiorID','SupervisiorID2')
df3=df
df3=df3.withColumnRenamed('EmployeeID','EmployeeID3').withColumnRenamed('SupervisiorID','SupervisiorID3')
df4=df
df4=df4.withColumnRenamed('EmployeeID','EmployeeID4').withColumnRenamed('SupervisiorID','SupervisiorID4')
df5=df
df5=df5.withColumnRenamed('EmployeeID','EmployeeID5').withColumnRenamed('SupervisiorID','SupervisiorID5')

from pyspark.sql.functions import *

join_type='left_outer'

join_cond=df['SupervisiorID']==df1['EmployeeID1']
df_1=df.join(df1,join_cond,join_type)
df_1=df_1.select('*',coalesce(df_1["SupervisiorID1"], df_1["SupervisiorID"]).alias('SupervisiorID_1'))
df_1=df_1.drop('EmployeeID1','SupervisiorID1')

join_cond=df_1['SupervisiorID_1']==df2['EmployeeID2']
df_2=df_1.join(df2,join_cond,join_type)
df_2=df_2.select('*',coalesce(df_2["SupervisiorID2"], df_2["SupervisiorID_1"]).alias('SupervisiorID_2'))
df_2=df_2.drop('EmployeeID2','SupervisiorID2')

join_cond=df_2['SupervisiorID_2']==df3['EmployeeID3']
df_3=df_2.join(df3,join_cond,join_type)
df_3=df_3.select('*',coalesce(df_3["SupervisiorID3"], df_3["SupervisiorID_2"]).alias('SupervisiorID_3'))
df_3=df_3.drop('EmployeeID3','SupervisiorID3')


#df_3.show()
# +----------+-------------+---------------+---------------+---------------+
# |EmployeeID|SupervisiorID|SupervisiorID_1|SupervisiorID_2|SupervisiorID_3|
# +----------+-------------+---------------+---------------+---------------+
# |         1|            2|              4|             17|             20|
# |         2|            4|             17|             20|             20|
# |         8|            6|              3|             15|             20|
# |         9|            5|             10|             20|             20|
# |         6|            3|             15|             20|             20|
# |         5|           10|             20|             20|             20|
# |         4|           17|             20|             20|             20|
# |         3|           15|             20|             20|             20|
# |        10|           20|             20|             20|             20|
# |        15|           20|             20|             20|             20|
# |        17|           20|             20|             20|             20|
# |        16|           21|             21|             21|             21|
# |        14|           12|             12|             12|             12|
# |        13|           11|             11|             11|             11|
# +----------+-------------+---------------+---------------+---------------+

spark.stop()
