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

from pyspark.sql import SparkSession

from common import load_crime_data_from_2010_to_present, parse_args, red

# prepare

explain, explain_only, file, _ = parse_args(sys.argv)

spark = SparkSession.builder.appName("Query 1 (SQL)").getOrCreate()

load_crime_data_from_2010_to_present(spark).createOrReplaceTempView("crime_data")

# same logic as in query_1/df_api.py
crime_data = spark.sql(
    """
    SELECT *
    FROM (SELECT YEAR(`DATE OCC`) AS `Year`,
                 MONTH(`DATE OCC`) AS `Month`,
                 COUNT(*) AS `Total Crimes`,
                 ROW_NUMBER() OVER (PARTITION BY YEAR(`DATE OCC`)
                                    ORDER BY COUNT(*) DESC) AS `#`
          FROM `crime_data`
          GROUP BY `Year`, `Month`)
    WHERE `#` <= 3
    ORDER BY `Year` ASC, `Total Crimes` DESC
    """
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

spark.catalog.dropTempView("crime_data")

spark.stop()
