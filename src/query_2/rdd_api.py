# query 2

import csv
import io
import operator
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

from pyspark.sql import SparkSession

from common import load_crime_data_from_2010_to_present_as_rdd, parse_args, red

# prepare

explain, explain_only, file, _ = parse_args(sys.argv)

spark = SparkSession.builder.appName("Query 2 (RDD)").getOrCreate()

cols = {"TIME OCC": 3, "Premis Desc": 15}

crime_data = (
    load_crime_data_from_2010_to_present_as_rdd(spark)
    # we need csv.reader to handle values that contain commas
    .map(lambda row: next(csv.reader(io.StringIO(row), dialect="unix")))
    # only keep crimes that happened on the street
    # and that a time of occurrence is available
    .filter(
        lambda row: (row[cols["Premis Desc"]].strip() == "STREET")
        and (row[cols["TIME OCC"]].strip() != "")
    )
    # split crimes into 4 phases of day
    # returns tuples of (phase of day, 1)
    .map(
        lambda row: (
            "Morning"
            if 500 <= int(row[cols["TIME OCC"]]) <= 1159
            else (
                "Afternoon"
                if 1200 <= int(row[cols["TIME OCC"]]) <= 1659
                else (
                    "Evening" if 1700 <= int(row[cols["TIME OCC"]]) <= 2059 else "Night"
                )
            ),
            1,
        )
    )
    # reduce by key to get total crimes for each phase of day
    .reduceByKey(operator.add)
    # sort by total crimes in descending order
    .sortBy(lambda tup: tup[1], ascending=False)
)

# execute

Q2 = red("Q2: ")
if explain or explain_only:
    file.write(Q2 + "Query execution plan:\n")
    file.write((crime_data.toDebugString().decode("utf-8")) + "\n")
    file.write("\n")

if not explain_only:
    file.write(Q2 + "Query results:\n")
    for row in crime_data.take(25):
        file.write(str(row) + "\n")
    file.write("\n")

# cleanup

spark.stop()
