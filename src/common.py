import sys

import paths
import schemas


# ./argv[0] [-e/--explain] [-E/--explain-only] [-o/--output <file>] [-s/--join-strategy <strategy>]
def parse_args(argv):
    explain = False
    explain_only = False
    file = sys.stdout
    join_strategy = None

    i = 1
    while i < len(argv):
        if argv[i] == "-e" or argv[i] == "--explain":
            explain = True
        elif argv[i] == "-E" or argv[i] == "--explain-only":
            explain_only = True
        elif argv[i] == "-o" or argv[i] == "--output":
            i += 1
            file = open(argv[i], "w")
        elif argv[i] == "-s" or argv[i] == "--join-strategy":
            i += 1
            if argv[i] not in [
                "broadcast",
                "merge",
                "shuffle_hash",
                "shuffle_replicate_nl",
            ]:
                raise ValueError(
                    "Invalid join strategy: "
                    + argv[i]
                    + "\n"
                    + "Valid join strategies: "
                    + "broadcast, merge, shuffle_hash, shuffle_replicate_nl"
                )
            join_strategy = argv[i]
        else:
            raise ValueError("Unrecognized argument: " + argv[i])
        i += 1
    return explain, explain_only, file, join_strategy


def red(str):
    return "\033[91m{}\033[00m".format(str)


def load_crime_data_from_2010_to_2019(spark):
    return (
        spark.read.option("header", True)
        .option("ignoreLeadingWhiteSpace", True)
        .option("ignoreTrailingWhiteSpace", True)
        .option("dateFormat", "MM/dd/yyyy hh:mm:ss a")
        .csv(
            path=paths.CRIME_DATA_FROM_2010_TO_2019_PATH,
            schema=schemas.CRIME_DATA_FROM_2010_TO_2019_SCHEMA,
        )
    )


def load_crime_data_from_2020_to_present(spark):
    return (
        spark.read.option("header", True)
        .option("ignoreLeadingWhiteSpace", True)
        .option("ignoreTrailingWhiteSpace", True)
        .option("dateFormat", "MM/dd/yyyy hh:mm:ss a")
        .csv(
            path=paths.CRIME_DATA_FROM_2020_TO_PRESENT_PATH,
            schema=schemas.CRIME_DATA_FROM_2020_TO_PRESENT_SCHEMA,
        )
    )


def load_crime_data_from_2010_to_present(spark):
    return (
        spark.read.option("header", True)
        .option("ignoreLeadingWhiteSpace", True)
        .option("ignoreTrailingWhiteSpace", True)
        .csv(
            path=paths.CRIME_DATA_FROM_2010_TO_PRESENT_PATH,
            schema=schemas.CRIME_DATA_FROM_2010_TO_PRESENT_SCHEMA,
        )
    )


def load_crime_data_from_2010_to_present_as_rdd(spark):
    return spark.sparkContext.textFile(paths.CRIME_DATA_FROM_2010_TO_PRESENT_PATH)


def save_crime_data_from_2010_to_present(df):
    (
        df.write.option("header", True)
        .option("ignoreLeadingWhiteSpace", True)
        .option("ignoreTrailingWhiteSpace", True)
        .csv(path=paths.CRIME_DATA_FROM_2010_TO_PRESENT_PATH, mode="overwrite")
    )


def load_la_income_2015(spark):
    return (
        spark.read.option("header", True)
        .option("ignoreLeadingWhiteSpace", True)
        .option("ignoreTrailingWhiteSpace", True)
        .csv(path=paths.LA_INCOME_2015_PATH, schema=schemas.LA_INCOME_2015_SCHEMA)
    )


def load_lapd_police_stations(spark):
    return (
        spark.read.option("header", True)
        .option("ignoreLeadingWhiteSpace", True)
        .option("ignoreTrailingWhiteSpace", True)
        .csv(
            path=paths.LAPD_POLICE_STATIONS_PATH,
            schema=schemas.LAPD_POLICE_STATIONS_SCHEMA,
        )
    )


def load_revgecoding(spark):
    return (
        spark.read.option("header", True)
        .option("ignoreLeadingWhiteSpace", True)
        .option("ignoreTrailingWhiteSpace", True)
        .csv(path=paths.REVGECODING_PATH, schema=schemas.REVGECODING_SCHEMA)
    )
