# query 3

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

from common import (
    load_crime_data_from_2010_to_present,
    load_la_income_2015,
    load_revgecoding,
    parse_args,
    red,
)

# prepare
explain, explain_only, file, join_strategy = parse_args(sys.argv)

comma_strategy = (", " + join_strategy) if join_strategy is not None else ""
spark = SparkSession.builder.appName("Query 3 (DF" + comma_strategy + ")").getOrCreate()

# see https://data.lacity.org/Public-Safety/Crime-Data-from-2010-to-2019/63jg-8b9z/about_data
descent_dict = {
    "A": "Other Asian",
    "B": "Black",
    "C": "Chinese",
    "D": "Cambodian",
    "F": "Filipino",
    "G": "Guamanian",
    "H": "Hispanic/Latin/Mexican",
    "I": "American Indian/Alaskan Native",
    "J": "Japanese",
    "K": "Korean",
    "L": "Laotian",
    "O": "Other",
    "P": "Pacific Islander",
    "S": "Samoan",
    "U": "Hawaiian",
    "V": "Vietnamese",
    "W": "White",
    "X": "Unknown",
    "Z": "Asian Indian",
}

rev_geocoding = (
    load_revgecoding(spark)
    .withColumnRenamed("ZIPcode", "Zip Code")
    .dropDuplicates(["LAT", "LON"])
)

crime_data = (
    load_crime_data_from_2010_to_present(spark)
    # only keep crimes that happened in 2015
    # and that have victims
    .filter(f.year(f.col("DATE OCC")) == 2015).filter(f.col("Vict Descent").isNotNull())
)

if join_strategy is not None:
    rev_geocoding = rev_geocoding.hint(join_strategy)
    crime_data = crime_data.hint(join_strategy)

# join to get zip codes
crime_data = crime_data.join(rev_geocoding, ["LAT", "LON"])

la_income = (
    load_la_income_2015(spark)
    .withColumn(
        "Estimated Median Income",
        # income is given in '$56,000' format
        # remove '$' and ',' and cast to int
        f.regexp_replace(f.col("Estimated Median Income"), "[\$\,]", "").cast(
            IntegerType()
        ),
    )
    .select(f.col("Zip Code"), f.col("Estimated Median Income"))
)

zip_codes_with_data = crime_data.select(f.col("Zip Code")).distinct()

if join_strategy is not None:
    la_income = la_income.hint(join_strategy)
    zip_codes_with_data = zip_codes_with_data.hint(join_strategy)

# only keep zip codes for which we have crime data
la_income = la_income.join(zip_codes_with_data, "Zip Code").persist()

zip_codes = (
    la_income.orderBy(f.col("Estimated Median Income").desc())
    .limit(3)
    .union(la_income.orderBy(f.col("Estimated Median Income").asc()).limit(3))
    .select(f.col("Zip Code"))
    .distinct()
)

if join_strategy is not None:
    zip_codes = zip_codes.hint(join_strategy)
    crime_data = crime_data.hint(join_strategy)

crime_data = (
    crime_data.join(zip_codes, "Zip Code")
    .groupBy(f.col("Vict Descent"))
    .count()
    .withColumnRenamed("count", "#")
    .orderBy(f.col("#").desc())
    # replace descent codes with descent names
    .replace(descent_dict, subset=["Vict Descent"])
)

# execute
parenthesis_strategy = (" (" + join_strategy + ")") if join_strategy is not None else ""

Q3 = red("Q3: ")
if explain or explain_only:
    file.write(Q3 + "Query execution plan" + parenthesis_strategy + ":\n")
    file.write(
        crime_data._sc._jvm.PythonSQLUtils.explainString(
            crime_data._jdf.queryExecution(), "formatted"
        )
        + "\n"
    )
    file.write("\n")

if not explain_only:
    file.write(Q3 + "Query results" + parenthesis_strategy + ":\n")
    # n=25, truncate=False, vertical=False
    file.write(crime_data._jdf.showString(25, 0, False) + "\n")
    file.write("\n")

# cleanup

la_income.unpersist()

spark.stop()

file.close()
