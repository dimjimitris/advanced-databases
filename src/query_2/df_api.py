# query 2

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
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

from common import load_crime_data_from_2010_to_present, parse_args, red

# prepare

explain, explain_only, file, _ = parse_args(sys.argv)

spark = SparkSession.builder.appName("Query 2 (DF)").getOrCreate()

crime_data = (
    load_crime_data_from_2010_to_present(spark)
    # only keep crimes that happened on the street
    # and that a time of occurrence is available
    .filter(f.col("Premis Desc") == "STREET")
    .filter(f.col("TIME OCC").isNotNull())
    # split crimes into 4 phases of day
    .withColumn(
        "Phase of Day",
        f.when(f.col("TIME OCC").cast(IntegerType()).between(500, 1159), "Morning")
        .when(f.col("TIME OCC").cast(IntegerType()).between(1200, 1659), "Afternoon")
        .when(f.col("TIME OCC").cast(IntegerType()).between(1700, 2059), "Evening")
        .otherwise("Night"),
    )
    .groupBy(f.col("Phase of Day"))
    .count()
    .withColumnRenamed("count", "Total Crimes")
    .orderBy(f.col("Total Crimes").desc())
)

# execute

Q2 = red("Q2: ")
if explain or explain_only:
    file.write(Q2 + "Query execution plan:\n")
    file.write(
        crime_data._sc._jvm.PythonSQLUtils.explainString(
            crime_data._jdf.queryExecution(), "formatted"
        )
        + "\n"
    )
    file.write("\n")

if not explain_only:
    file.write(Q2 + "Query results:\n")
    # n=25, truncate=False, vertical=False
    file.write(crime_data._jdf.showString(25, 0, False) + "\n")
    file.write("\n")

# cleanup

spark.stop()

file.close()
