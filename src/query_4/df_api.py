# query 4

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

import geopy.distance
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType

from common import (
    load_crime_data_from_2010_to_present,
    load_lapd_police_stations,
    parse_args,
    red,
)

# prepare

explain, explain_only, file, join_strategy = parse_args(sys.argv)

comma_strategy = (", " + join_strategy) if join_strategy is not None else ""
spark = SparkSession.builder.appName("Query 4 (DF" + comma_strategy + ")").getOrCreate()

# declare a user-defined function to calculate distance between two points
geodesic_udf = f.udf(
    f=lambda lat1, lon1, lat2, lon2: geopy.distance.geodesic(
        (lat1, lon1), (lat2, lon2)
    ).km,
    returnType=DoubleType(),
)

lapd_police_stations = (
    load_lapd_police_stations(spark)
    .withColumnRenamed("X", "Station LON")
    .withColumnRenamed("Y", "Station LAT")
    .withColumnRenamed("DIVISION", "Division")
    .select(
        f.col("Station LON"), f.col("Station LAT"), f.col("Division"), f.col("PREC")
    )
)

crime_data_1 = (
    load_crime_data_from_2010_to_present(spark)
    # filter out crimes that have no location data
    .filter(f.col("LAT") != 0.0)
    .filter(f.col("LON") != 0.0)
    # filter out crimes that are not committed with a firearm
    .filter(f.col("Weapon Used Cd").between(100, 199))
    .withColumn("Year", f.year(f.col("Date Rptd")))
    .withColumnRenamed("AREA", "PREC")
)

if join_strategy is not None:
    lapd_police_stations = lapd_police_stations.hint(join_strategy)
    crime_data_1 = crime_data_1.hint(join_strategy)

crime_data_1 = (
    # join to get police station location
    crime_data_1.join(lapd_police_stations, "PREC")
    .withColumn(
        "Distance to Station",
        geodesic_udf(
            f.col("LAT"),
            f.col("LON"),
            f.col("Station LAT"),
            f.col("Station LON"),
        ),
    )
    .select(
        f.col("Year"),
        f.col("Division"),
        f.col("Distance to Station"),
    )
    .persist()
)

crime_data_1a = (
    crime_data_1.groupBy(f.col("Year"))
    .agg(
        f.avg(f.col("Distance to Station")).alias("Average Distance to Station"),
        f.count(f.expr("*")).alias("#"),
    )
    .orderBy(f.col("Year").asc())
)

crime_data_1b = (
    crime_data_1.groupBy(f.col("Division"))
    .agg(
        f.avg(f.col("Distance to Station")).alias("Average Distance to Station"),
        f.count(f.expr("*")).alias("#"),
    )
    .orderBy(f.col("#").desc())
)

crime_data_2 = (
    load_crime_data_from_2010_to_present(spark)
    .filter(f.col("LAT") != 0.0)
    .filter(f.col("LON") != 0.0)
    .filter(f.col("Weapon Used Cd").between(100, 199))
    .withColumn("Year", f.year(f.col("Date Rptd")))
    .withColumnRenamed("AREA", "PREC")
)

if join_strategy is not None:
    lapd_police_stations = lapd_police_stations.hint(join_strategy)
    crime_data_2 = crime_data_2.hint(join_strategy)

# cross join to get distance to each police station
crime_data_2 = crime_data_2.crossJoin(lapd_police_stations).withColumn(
    "Distance to Station",
    geodesic_udf(
        f.col("LAT"),
        f.col("LON"),
        f.col("Station LAT"),
        f.col("Station LON"),
    ),
)

crime_data_2_tmp = crime_data_2.persist()

# for each crime, find the minimum distance to a police station
min_distances = crime_data_2.groupBy(f.col("DR_NO")).agg(
    f.min(f.col("Distance to Station")).alias("Distance to Station")
)

if join_strategy is not None:
    min_distances = min_distances.hint(join_strategy)
    crime_data_2 = crime_data_2.hint(join_strategy)

crime_data_2 = (
    # for each crime, only keep the row with the minimum distance to a police station
    crime_data_2.join(
        min_distances,
        ["DR_NO", "Distance to Station"],
    )
    .withColumnRenamed("Distance to Station", "Distance to Closest Station")
    .select(
        f.col("Year"),
        f.col("Division"),
        f.col("Distance to Closest Station"),
    )
    .persist()
)

crime_data_2a = (
    crime_data_2.groupBy(f.col("Year"))
    .agg(
        f.avg(f.col("Distance to Closest Station")).alias(
            "Average Distance to Closest Station"
        ),
        f.count(f.expr("*")).alias("#"),
    )
    .orderBy(f.col("Year").asc())
)

crime_data_2b = (
    crime_data_2.groupBy(f.col("Division"))
    .agg(
        f.avg(f.col("Distance to Closest Station")).alias(
            "Average Distance to Closest Station"
        ),
        f.count(f.expr("*")).alias("#"),
    )
    .orderBy(f.col("#").desc())
)

# execute
Q4 = red("Q4: ")
if explain or explain_only:
    for q, df in [
        ("1a", crime_data_1a),
        ("1b", crime_data_1b),
        ("2a", crime_data_2a),
        ("2b", crime_data_2b),
    ]:
        file.write(Q4 + "Query execution plan (" + q + comma_strategy + "):\n")
        file.write(
            df._sc._jvm.PythonSQLUtils.explainString(
                df._jdf.queryExecution(), "formatted"
            )
            + "\n"
        )
        file.write("\n")

if not explain_only:
    for q, df in [
        ("1a", crime_data_1a),
        ("1b", crime_data_1b),
        ("2a", crime_data_2a),
        ("2b", crime_data_2b),
    ]:
        file.write(Q4 + "Query results (" + q + comma_strategy + "):\n")
        # n=25, truncate=False, vertical=False
        file.write(df._jdf.showString(25, 0, False) + "\n")
        file.write("\n")

        if q == "1b":
            crime_data_1.unpersist()
        elif q == "2a":
            crime_data_2_tmp.unpersist()

# cleanup

crime_data_2.unpersist()

spark.stop()

file.close()
