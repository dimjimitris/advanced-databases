# preprocessing

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

from common import (
    load_crime_data_from_2010_to_2019,
    load_crime_data_from_2020_to_present,
    parse_args,
    red,
    save_crime_data_from_2010_to_present,
)

# prepare

explain, explain_only, file, _ = parse_args(sys.argv)

spark = SparkSession.builder.appName("Preprocessing (DF)").getOrCreate()

crime_data_pt_1 = load_crime_data_from_2010_to_2019(spark)
crime_data_pt_2 = load_crime_data_from_2020_to_present(spark)
crime_data = crime_data_pt_1.union(crime_data_pt_2).dropDuplicates()

# execute

PREPROCESSING = red("PREPROCESSING: ")
if explain or explain_only:
    file.write(PREPROCESSING + "Query execution plan:\n")
    file.write(crime_data._jdf.queryExecution().toString() + "\n")
    file.write("\n")

if not explain_only:
    crime_data_pt_1_count = crime_data_pt_1.count()
    crime_data_pt_2_count = crime_data_pt_2.count()
    crime_data_count = crime_data.count()

    save_crime_data_from_2010_to_present(crime_data)

    file.write(
        PREPROCESSING
        + "Dataset 1 (2010 - 2020) has "
        + str(crime_data_pt_1_count)
        + " rows\n"
    )
    file.write("\n")

    file.write(
        PREPROCESSING
        + "Dataset 2 (2020 - Present) has "
        + str(crime_data_pt_2_count)
        + " rows\n"
    )
    file.write("\n")

    file.write(
        PREPROCESSING
        + "The merged dataset (2010 - Present) has "
        + str(crime_data_count)
        + " rows ("
        + str(crime_data_pt_1_count + crime_data_pt_2_count - crime_data_count)
        + " duplicate rows were dropped)\n"
    )
    file.write("\n")

    file.write(
        PREPROCESSING
        + "The contents of the merged dataset (2010 - Present) "
        + "were successfully written to disk\n"
    )
    file.write("\n")

    file.write(
        PREPROCESSING
        + "The merged dataset (2010 - Present) has the following schema:\n"
    )
    file.write(crime_data._jdf.schema().treeString() + "\n")
    file.write("\n")

# cleanup

spark.stop()

file.close()
