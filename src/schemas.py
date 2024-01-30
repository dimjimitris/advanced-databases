from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

CRIME_DATA_FROM_2010_TO_2019_SCHEMA = (
    CRIME_DATA_FROM_2020_TO_PRESENT_SCHEMA
) = CRIME_DATA_FROM_2010_TO_PRESENT_SCHEMA = StructType(
    [
        StructField("DR_NO", IntegerType()),
        StructField("Date Rptd", DateType()),
        StructField("DATE OCC", DateType()),
        StructField("TIME OCC", StringType()),
        StructField("AREA", IntegerType()),
        StructField("AREA NAME", StringType()),
        StructField("Rpt Dist No", IntegerType()),
        StructField("Part 1-2", StringType()),
        StructField("Crm Cd", IntegerType()),
        StructField("Crm Cd Desc", StringType()),
        StructField("Mocodes", StringType()),
        StructField("Vict Age", IntegerType()),
        StructField("Vict Sex", StringType()),
        StructField("Vict Descent", StringType()),
        StructField("Premis Cd", IntegerType()),
        StructField("Premis Desc", StringType()),
        StructField("Weapon Used Cd", IntegerType()),
        StructField("Weapon Desc", StringType()),
        StructField("Status", StringType()),
        StructField("Status Desc", StringType()),
        StructField("Crm Cd 1", IntegerType()),
        StructField("Crm Cd 2", IntegerType()),
        StructField("Crm Cd 3", IntegerType()),
        StructField("Crm Cd 4", IntegerType()),
        StructField("LOCATION", StringType()),
        StructField("Cross Street", StringType()),
        StructField("LAT", DoubleType()),
        StructField("LON", DoubleType()),
    ]
)

LA_INCOME_2015_SCHEMA = StructType(
    [
        StructField("Zip Code", StringType()),
        StructField("Community", StringType()),
        StructField("Estimated Median Income", StringType()),
    ]
)

LAPD_POLICE_STATIONS_SCHEMA = StructType(
    [
        StructField("X", DoubleType()),
        StructField("Y", DoubleType()),
        StructField("FID", IntegerType()),
        StructField("DIVISION", StringType()),
        StructField("LOCATION", StringType()),
        StructField("PREC", IntegerType()),
    ]
)

REVGECODING_SCHEMA = StructType(
    [
        StructField("LAT", DoubleType()),
        StructField("LON", DoubleType()),
        StructField("ZIPcode", StringType()),
    ]
)
