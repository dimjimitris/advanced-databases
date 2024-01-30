#!/bin/bash

download_path=$HOME/datasets
if [ $# -eq 1 ]
  then
    download_path=$1
fi

mkdir -p $download_path/
cd $download_path/

curl -o Crime_Data_from_2010_to_2019.csv "https://data.lacity.org/api/views/63jg-8b9z/rows.csv?accessType=DOWNLOAD"
curl -o Crime_Data_from_2020_to_Present.csv "https://data.lacity.org/api/views/2nrs-mtv8/rows.csv?accessType=DOWNLOAD"
curl -o LAPD_Police_Stations.csv "https://opendata.arcgis.com/api/v3/datasets/1dd3271db7bd44f28285041058ac4612_0/downloads/data?format=csv&spatialRefId=4326"
curl -o data.tar.gz "http://www.dblab.ece.ntua.gr/files/classes/data.tar.gz"
tar -xvzf data.tar.gz
rm data.tar.gz
mv income/LA_income_2015.csv .
rm -rf income

# fix typo in the header of Crime_Data_from_2010_to_2019.csv
sed -i "1 s/AREA /AREA/" Crime_Data_from_2010_to_2019.csv
