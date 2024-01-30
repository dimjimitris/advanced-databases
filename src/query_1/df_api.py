# query 1

import os
import sys

# hack to import from parent directory
sys.path.append(  # append parent directory to $PATH
    os.path.dirname(  # get parent directory's absolute path
        os.path.dirname(  # get current directory's absolute path
            os.path.abspath(__file__)  # get current file's absolute path
        )
    )
)

import pyspark.sql.functions as f
from pyspark.sql import SparkSession, Window

from common import load_crime_data_from_2010_to_present, parse_args, red

# prepare

explain, explain_only, file, _ = parse_args(sys.argv)

spark = SparkSession.builder.appName("Query 1 (DF)").getOrCreate()

crime_data = (
    load_crime_data_from_2010_to_present(spark)
    # create 'Month' and 'Year' columns
    .withColumn("Year", f.year("DATE OCC"))
    .withColumn("Month", f.month("DATE OCC"))
    # group by 'Year' (firstly) and 'Month' (secondly) and count
    .groupBy(f.col("Year"), f.col("Month"))
    .count()
    .withColumnRenamed("count", "Total Crimes")
    # group by 'Year', sort by 'Total Crimes' and number the rows
    # for each 'Year' value, each 'Month', 'Total Crimes' pair will be numbered
    # from 1 to n, where n is the number of rows for that 'Year' value
    .withColumn(
        "#",
        f.row_number().over(
            Window.partitionBy(f.col("Year")).orderBy(f.col("Total Crimes").desc())
        ),
    )
    # only keep top 3 rows for each 'Year' value
    .filter(f.col("#") <= 3)
    .orderBy(f.col("Year").asc(), f.col("Total Crimes").desc())
)

# execute

Q1 = red("Q1: ")
if explain or explain_only:
    file.write(Q1 + "Query execution plan:\n")
    file.write(
        crime_data._sc._jvm.PythonSQLUtils.explainString(
            crime_data._jdf.queryExecution(), "formatted"
        )
        + "\n"
    )
    file.write("\n")

if not explain_only:
    file.write(Q1 + "Query results:\n")
    # n=25, truncate=False, vertical=False
    file.write(crime_data._jdf.showString(25, 0, False) + "\n")
    file.write("\n")

# cleanup

spark.stop()

file.close()
