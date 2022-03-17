#!/usr/bin/env python

"""
# Pyspark - Preprocessing

#### Description
Filtering and slicing techniques using pyspark

- create a Spark Session, and store the data into a Spark DataFrame;
- query data with PySpark using standard SQL;
- create a new column inside the Spark DataFrame;
- perform standard data cleaning - type consistency, filtering, slicing;
- pivoting and manipulating a Spark DataFrame.

"""

import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"

# Initialize a SparkSession
spark = SparkSession.builder.getOrCreate()
# Read the data file with header

file_path = "tips.csv"
tips = spark.read.csv(file_path, header=True)
tips.show()
"""
+----------+----+------+------+---+------+----+
|total_bill| tip|   sex|smoker|day|  time|size|
+----------+----+------+------+---+------+----+
|     16.99|1.01|Female|    No|Sun|Dinner|   2|
|     10.34|1.66|  Male|    No|Sun|Dinner|   3|
|     21.01| 3.5|  Male|    No|Sun|Dinner|   3|
|     23.68|3.31|  Male|    No|Sun|Dinner|   2|
|     24.59|3.61|Female|    No|Sun|Dinner|   4|
|     25.29|4.71|  Male|    No|Sun|Dinner|   4|
|      8.77|   2|  Male|    No|Sun|Dinner|   2|
|     26.88|3.12|  Male|    No|Sun|Dinner|   4|
|     15.04|1.96|  Male|    No|Sun|Dinner|   2|
|     14.78|3.23|  Male|    No|Sun|Dinner|   2|
|     10.27|1.71|  Male|    No|Sun|Dinner|   2|
|     35.26|   5|Female|    No|Sun|Dinner|   4|
|     15.42|1.57|  Male|    No|Sun|Dinner|   2|
|     18.43|   3|  Male|    No|Sun|Dinner|   4|
|     14.83|3.02|Female|    No|Sun|Dinner|   2|
|     21.58|3.92|  Male|    No|Sun|Dinner|   2|
|     10.33|1.67|Female|    No|Sun|Dinner|   3|
|     16.29|3.71|  Male|    No|Sun|Dinner|   3|
|     16.97| 3.5|Female|    No|Sun|Dinner|   3|
|     20.65|3.35|  Male|    No|Sat|Dinner|   3|
+----------+----+------+------+---+------+----+
only showing top 20 rows
"""
type(tips)

"""
pyspark.sql.dataframe.DataFrame
"""

"""
Default database present but empty
"""

spark.catalog.listDatabases()
"""
[Database(name='default', description='default database', locationUri='file:/home/project/spark-warehouse')]
"""

"""
If we inspect the available Tables, we easily see that there are not tables available.
"""

spark.catalog.listTables('default')
"""
[]
"""

"""
To create a table in the default database, we use the createOrReplaceTempView method

Add tips data to the catalog
"""
tips.createOrReplaceTempView("tips")
spark.catalog.listTables()
"""
[Table(name='tips', database=None, description=None, tableType='TEMPORARY', isTemporary=True)]
"""


# Querying data with PySpark (using SQL commands)

QUERY_TIPS = "FROM tips SELECT * LIMIT 10"

# Passing the query into the sql module

tips10 = spark.sql(QUERY_TIPS)
tips10.show()
"""
+----------+----+------+------+---+------+----+
|total_bill| tip|   sex|smoker|day|  time|size|
+----------+----+------+------+---+------+----+
|     16.99|1.01|Female|    No|Sun|Dinner|   2|
|     10.34|1.66|  Male|    No|Sun|Dinner|   3|
|     21.01| 3.5|  Male|    No|Sun|Dinner|   3|
|     23.68|3.31|  Male|    No|Sun|Dinner|   2|
|     24.59|3.61|Female|    No|Sun|Dinner|   4|
|     25.29|4.71|  Male|    No|Sun|Dinner|   4|
|      8.77|   2|  Male|    No|Sun|Dinner|   2|
|     26.88|3.12|  Male|    No|Sun|Dinner|   4|
|     15.04|1.96|  Male|    No|Sun|Dinner|   2|
|     14.78|3.23|  Male|    No|Sun|Dinner|   2|
+----------+----+------+------+---+------+----+
"""

# Switching to a panda dataframe for manipulation of data in python space

tips10 = spark.sql(QUERY_TIPS)
tips10_df = tips10.toPandas()
tips10_df
"""
total_bill	tip	sex	smoker	day	time	size
--------------------------------------------------------------
0	16.99	1.01	Female	No	Sun	Dinner	2
1	10.34	1.66	Male	No	Sun	Dinner	3
2	21.01	3.5	Male	No	Sun	Dinner	3
3	23.68	3.31	Male	No	Sun	Dinner	2
4	24.59	3.61	Female	No	Sun	Dinner	4
5	25.29	4.71	Male	No	Sun	Dinner	4
6	8.77	2	Male	No	Sun	Dinner	2
7	26.88	3.12	Male	No	Sun	Dinner	4
8	15.04	1.96	Male	No	Sun	Dinner	2
9	14.78	3.23	Male	No	Sun	Dinner	2
"""

# Exercising groupby and orderby commands

STUDENT_QUERY = "SELECT day, sex, COUNT(*) as N FROM tips GROUP BY day, sex ORDER BY day"
tips_counts = spark.sql(STUDENT_QUERY)
pd_counts = tips_counts.toPandas() # Convert the results to a pandas DataFrame
"""

        day	sex	N
~~~~~~~~~~~~~~~~~~~~~~~~~~~
0	Fri	Male	10
1	Fri	Female	9
2	Sat	Male	59
3	Sat	Female	28
4	Sun	Male	58
5	Sun	Female	18
6	Thur	Male	30
7	Thur	Female	32
"""
pd_counts.shape
"""
(8,3)
"""

vcf_01 =  pd_counts.shape[0]
with open('vcf_01.txt', 'w') as f:
    f.write("%s\n" % vcf_01)
    """8"""
# Creating new columns and massaging the existing ones with PySpark
# ETL

# Adding column 'perc_tips'
tips = spark.table("tips")
tips = tips.withColumn("perc_tips", (tips.tip/tips.total_bill)*100)
tips.show()

"""
+----------+----+------+------+---+------+----+------------------+
|total_bill| tip|   sex|smoker|day|  time|size|         perc_tips|
+----------+----+------+------+---+------+----+------------------+
|     16.99|1.01|Female|    No|Sun|Dinner|   2|5.9446733372572105|
|     10.34|1.66|  Male|    No|Sun|Dinner|   3|16.054158607350097|
|     21.01| 3.5|  Male|    No|Sun|Dinner|   3|16.658733936220845|
|     23.68|3.31|  Male|    No|Sun|Dinner|   2| 13.97804054054054|
|     24.59|3.61|Female|    No|Sun|Dinner|   4|14.680764538430255|
|     25.29|4.71|  Male|    No|Sun|Dinner|   4| 18.62396204033215|
|      8.77|   2|  Male|    No|Sun|Dinner|   2| 22.80501710376283|
|     26.88|3.12|  Male|    No|Sun|Dinner|   4|11.607142857142858|
|     15.04|1.96|  Male|    No|Sun|Dinner|   2|13.031914893617023|
|     14.78|3.23|  Male|    No|Sun|Dinner|   2|21.853856562922868|
|     10.27|1.71|  Male|    No|Sun|Dinner|   2| 16.65043816942551|
|     35.26|   5|Female|    No|Sun|Dinner|   4|14.180374361883155|
|     15.42|1.57|  Male|    No|Sun|Dinner|   2|10.181582360570687|
|     18.43|   3|  Male|    No|Sun|Dinner|   4|16.277807921866522|
|     14.83|3.02|Female|    No|Sun|Dinner|   2|20.364126770060686|
|     21.58|3.92|  Male|    No|Sun|Dinner|   2|18.164967562557923|
|     10.33|1.67|Female|    No|Sun|Dinner|   3| 16.16650532429816|
|     16.29|3.71|  Male|    No|Sun|Dinner|   3|22.774708410067525|
|     16.97| 3.5|Female|    No|Sun|Dinner|   3|20.624631703005306|
|     20.65|3.35|  Male|    No|Sat|Dinner|   3|16.222760290556902|
+----------+----+------+------+---+------+----+------------------+
only showing top 20 rows
"""

# Some transformations
# Get the male/female counts

by_sex = tips.groupBy("sex")
by_sex.count().show()
"""
+------+-----+
|   sex|count|
+------+-----+
|Female|   87|
|  Male|  157|
+------+-----+
"""

# How the male/female tips the servers

by_gender = tips.groupBy("sex")
by_gender.avg("perc_tips").show()
"""
+------+------------------+
|   sex|    avg(perc_tips)|
+------+------------------+
|Female|16.649073632892485|
|  Male|15.765054700429744|
+------+------------------+
"""

# Massaging the existing columns

tips = tips.withColumn("total_bill",tips.total_bill.cast("double"))
tips = tips.withColumn("tip", tips.tip.cast("double"))
tips = tips.withColumn("size", tips.size.cast("integer"))
tips.show()
"""
+----------+----+------+------+---+------+----+------------------+
|total_bill| tip|   sex|smoker|day|  time|size|         perc_tips|
+----------+----+------+------+---+------+----+------------------+
|     16.99|1.01|Female|    No|Sun|Dinner|   2|5.9446733372572105|
|     10.34|1.66|  Male|    No|Sun|Dinner|   3|16.054158607350097|
|     21.01| 3.5|  Male|    No|Sun|Dinner|   3|16.658733936220845|
|     23.68|3.31|  Male|    No|Sun|Dinner|   2| 13.97804054054054|
|     24.59|3.61|Female|    No|Sun|Dinner|   4|14.680764538430255|
|     25.29|4.71|  Male|    No|Sun|Dinner|   4| 18.62396204033215|
|      8.77| 2.0|  Male|    No|Sun|Dinner|   2| 22.80501710376283|
|     26.88|3.12|  Male|    No|Sun|Dinner|   4|11.607142857142858|
|     15.04|1.96|  Male|    No|Sun|Dinner|   2|13.031914893617023|
|     14.78|3.23|  Male|    No|Sun|Dinner|   2|21.853856562922868|
|     10.27|1.71|  Male|    No|Sun|Dinner|   2| 16.65043816942551|
|     35.26| 5.0|Female|    No|Sun|Dinner|   4|14.180374361883155|
|     15.42|1.57|  Male|    No|Sun|Dinner|   2|10.181582360570687|
|     18.43| 3.0|  Male|    No|Sun|Dinner|   4|16.277807921866522|
|     14.83|3.02|Female|    No|Sun|Dinner|   2|20.364126770060686|
|     21.58|3.92|  Male|    No|Sun|Dinner|   2|18.164967562557923|
|     10.33|1.67|Female|    No|Sun|Dinner|   3| 16.16650532429816|
|     16.29|3.71|  Male|    No|Sun|Dinner|   3|22.774708410067525|
|     16.97| 3.5|Female|    No|Sun|Dinner|   3|20.624631703005306|
|     20.65|3.35|  Male|    No|Sat|Dinner|   3|16.222760290556902|
+----------+----+------+------+---+------+----+------------------+
only showing top 20 rows
"""

# More transformations:
# tips grouped-by gender and smoker

by_smoker_sex_table = tips.groupBy("smoker", "sex")
by_smoker_sex_table.avg("tip").show()
"""
+------+------+------------------+
|smoker|   sex|          avg(tip)|
+------+------+------------------+
|    No|Female|2.7735185185185185|
|    No|  Male|  3.11340206185567|
|   Yes|  Male|3.0511666666666666|
|   Yes|Female| 2.931515151515151|
+------+------+------------------+
"""

# Convert the results to a pandas DataFrame
by_smoker_sex_df = by_smoker_sex_table.avg("tip").toPandas()
"""
must be "Female"
"""
by_smoker_sex_df.iloc[0,1]
"""
'Female'
"""

# Slicing the Table for a temporary view
tips_000 = tips.select("total_bill", "tip", "size", "perc_tips")
tips_000.show()
"""
|total_bill| tip|   sex|smoker|day|  time|size|         perc_tips|
+----------+----+----+------------------+
|total_bill| tip|size|         perc_tips|
+----------+----+----+------------------+
|     16.99|1.01|   2|5.9446733372572105|
|     10.34|1.66|   3|16.054158607350097|
|     21.01| 3.5|   3|16.658733936220845|
|     23.68|3.31|   2| 13.97804054054054|
|     24.59|3.61|   4|14.680764538430255|
|     25.29|4.71|   4| 18.62396204033215|
|      8.77| 2.0|   2| 22.80501710376283|
|     26.88|3.12|   4|11.607142857142858|
|     15.04|1.96|   2|13.031914893617023|
|     14.78|3.23|   2|21.853856562922868|
|     10.27|1.71|   2| 16.65043816942551|
|     35.26| 5.0|   4|14.180374361883155|
|     15.42|1.57|   2|10.181582360570687|
|     18.43| 3.0|   4|16.277807921866522|
|     14.83|3.02|   2|20.364126770060686|
|     21.58|3.92|   2|18.164967562557923|
|     10.33|1.67|   3| 16.16650532429816|
|     16.29|3.71|   3|22.774708410067525|
|     16.97| 3.5|   3|20.624631703005306|
|     20.65|3.35|   3|16.222760290556902|
+----------+----+----+------------------+
only showing top 20 rows
"""

# Filtering a Table and transfer to pandas
#### Filtering with a string condition
tips_01 = tips.filter("total_bill>40").toPandas()
tips_01
"""
total_bill	tip	sex	smoker	day	time	size	perc_tips
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
0	48.27	6.73	Male	No	Sat	Dinner	4	13.942407
1	40.17	4.73	Male	Yes	Fri	Dinner	4	11.774956
2	44.30	2.50	Female	Yes	Sat	Dinner	3	5.643341
3	41.19	5.00	Male	No	Thur	Lunch	5	12.138869
4	48.17	5.00	Male	No	Sun	Dinner	6	10.379905
5	50.81	10.00	Male	Yes	Sat	Dinner	3	19.681165
6	45.35	3.50	Male	Yes	Sun	Dinner	3	7.717751
7	40.55	3.00	Male	Yes	Sun	Dinner	2	7.398274
8	43.11	5.00	Female	Yes	Thur	Lunch	4	11.598237
9	48.33	9.00	Male	No	Sat	Dinner	4	18.621974
"""

# Filtering with a Boolean condition

Filter tips with a boolean
tips_02 = tips.filter(tips.total_bill>40).toPandas()
tips_02
"""
total_bill	tip	sex	smoker	day	time	size	perc_tips
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
0	48.27	6.73	Male	No	Sat	Dinner	4	13.942407
1	40.17	4.73	Male	Yes	Fri	Dinner	4	11.774956
2	44.30	2.50	Female	Yes	Sat	Dinner	3	5.643341
3	41.19	5.00	Male	No	Thur	Lunch	5	12.138869
4	48.17	5.00	Male	No	Sun	Dinner	6	10.379905
5	50.81	10.00	Male	Yes	Sat	Dinner	3	19.681165
6	45.35	3.50	Male	Yes	Sun	Dinner	3	7.717751
7	40.55	3.00	Male	Yes	Sun	Dinner	2	7.398274
8	43.11	5.00	Female	Yes	Thur	Lunch	4	11.598237
9	48.33	9.00	Male	No	Sat	Dinner	4	18.621974
We can even apply multiple filtering as follows:
"""

filterA = tips.sex == "Female"
filterB = tips.day == "Sun"
tips_03 = tips.filter(filterA).filter(filterB)
tips_03.show()
"""
+----------+----+------+------+---+------+----+------------------+
|total_bill| tip|   sex|smoker|day|  time|size|         perc_tips|
+----------+----+------+------+---+------+----+------------------+
|     16.99|1.01|Female|    No|Sun|Dinner|   2|5.9446733372572105|
|     24.59|3.61|Female|    No|Sun|Dinner|   4|14.680764538430255|
|     35.26| 5.0|Female|    No|Sun|Dinner|   4|14.180374361883155|
|     14.83|3.02|Female|    No|Sun|Dinner|   2|20.364126770060686|
|     10.33|1.67|Female|    No|Sun|Dinner|   3| 16.16650532429816|
|     16.97| 3.5|Female|    No|Sun|Dinner|   3|20.624631703005306|
|     10.29| 2.6|Female|    No|Sun|Dinner|   2| 25.26724975704568|
|     34.81| 5.2|Female|    No|Sun|Dinner|   4|14.938236139040505|
|     25.71| 4.0|Female|    No|Sun|Dinner|   3|15.558148580318942|
|     17.31| 3.5|Female|    No|Sun|Dinner|   2|20.219526285384173|
|     29.85|5.14|Female|    No|Sun|Dinner|   5|17.219430485762143|
|      25.0|3.75|Female|    No|Sun|Dinner|   4|              15.0|
|     13.39|2.61|Female|    No|Sun|Dinner|   2| 19.49215832710978|
|     16.21| 2.0|Female|    No|Sun|Dinner|   3|12.338062924120912|
|     17.51| 3.0|Female|   Yes|Sun|Dinner|   2|17.133066818960593|
|       9.6| 4.0|Female|   Yes|Sun|Dinner|   2| 41.66666666666667|
|      20.9| 3.5|Female|   Yes|Sun|Dinner|   3| 16.74641148325359|
|     18.15| 3.5|Female|   Yes|Sun|Dinner|   3| 19.28374655647383|
+----------+----+------+------+---+------+----+------------------+
"""
# Removing null values from the table

tips_counts = tips.filter("total_bill is not NULL")
tips_counts.show()
"""
+----------+----+------+------+---+------+----+------------------+
|total_bill| tip|   sex|smoker|day|  time|size|         perc_tips|
+----------+----+------+------+---+------+----+------------------+
|     16.99|1.01|Female|    No|Sun|Dinner|   2|5.9446733372572105|
|     10.34|1.66|  Male|    No|Sun|Dinner|   3|16.054158607350097|
|     21.01| 3.5|  Male|    No|Sun|Dinner|   3|16.658733936220845|
|     23.68|3.31|  Male|    No|Sun|Dinner|   2| 13.97804054054054|
|     24.59|3.61|Female|    No|Sun|Dinner|   4|14.680764538430255|
|     25.29|4.71|  Male|    No|Sun|Dinner|   4| 18.62396204033215|
|      8.77| 2.0|  Male|    No|Sun|Dinner|   2| 22.80501710376283|
|     26.88|3.12|  Male|    No|Sun|Dinner|   4|11.607142857142858|
|     15.04|1.96|  Male|    No|Sun|Dinner|   2|13.031914893617023|
|     14.78|3.23|  Male|    No|Sun|Dinner|   2|21.853856562922868|
|     10.27|1.71|  Male|    No|Sun|Dinner|   2| 16.65043816942551|
|     35.26| 5.0|Female|    No|Sun|Dinner|   4|14.180374361883155|
|     15.42|1.57|  Male|    No|Sun|Dinner|   2|10.181582360570687|
|     18.43| 3.0|  Male|    No|Sun|Dinner|   4|16.277807921866522|
|     14.83|3.02|Female|    No|Sun|Dinner|   2|20.364126770060686|
|     21.58|3.92|  Male|    No|Sun|Dinner|   2|18.164967562557923|
|     10.33|1.67|Female|    No|Sun|Dinner|   3| 16.16650532429816|
|     16.29|3.71|  Male|    No|Sun|Dinner|   3|22.774708410067525|
|     16.97| 3.5|Female|    No|Sun|Dinner|   3|20.624631703005306|
|     20.65|3.35|  Male|    No|Sat|Dinner|   3|16.222760290556902|
+----------+----+------+------+---+------+----+------------------+
only showing top 20 rows
"""

# Aggregation using groupBy of gender and smoker

exercise_table = tips.filter(tips.sex=='Male').filter(tips.smoker=='No').groupBy().avg('perc_tips')
exercise_table_df = exercise_table.toPandas()
exercise_table_df
"""

    avg(perc_tips)
    ~~~~~~~~~~~~~~
0	16.066872
"""

vcf_02 =  int(exercise_table_df.iloc[0,0])
with open('vcf_02.txt', 'w') as f:
    f.write("%s\n" % vcf_02)
    """16"""
