# private fields

_HDFS_NAMENODE = "hdfs://okeanos-master:54310"

_HDFS_HOME = "/user/user"

_DATASETS_PATH = _HDFS_NAMENODE + _HDFS_HOME + "/datasets"

# public fields

CRIME_DATA_FROM_2010_TO_2019_PATH = _DATASETS_PATH + "/Crime_Data_from_2010_to_2019.csv"

CRIME_DATA_FROM_2020_TO_PRESENT_PATH = (
    _DATASETS_PATH + "/Crime_Data_from_2020_to_Present.csv"
)

CRIME_DATA_FROM_2010_TO_PRESENT_PATH = (
    _DATASETS_PATH + "/Crime_Data_from_2010_to_Present.csv"
)

LA_INCOME_2015_PATH = _DATASETS_PATH + "/LA_income_2015.csv"

LAPD_POLICE_STATIONS_PATH = _DATASETS_PATH + "/LAPD_Police_Stations.csv"

REVGECODING_PATH = _DATASETS_PATH + "/revgecoding.csv"
