import os


# Define in environment
# export AWS_ENDPOINT_URL=https://s3.mesocentre.uca.fr
# export AWS_ACCESS_KEY_ID=xxxx
# export AWS_SECRET_ACCESS_KEY=yyyy

## Example1 : access to s3 with pandas (read_csv / to_csv)
# pip install pandas numpy s3fs
print("TESTING WITH PANDAS... ", end='')

import pandas as pd
import numpy as np

df = pd.read_csv("s3://datascience/data/98646099999/2020.csv", 
                 index_col='DATE',
                 usecols=['DATE','STATION','NAME','LONGITUDE','LATITUDE','ELEVATION','TMP','DEW','SLP'],
)

df["air"] = df['TMP'].astype('str').str.partition(',')[0]
df.air = df.air.replace('+9999',np.nan).astype('float64')
df.air = df.air/10.

df.to_csv("s3://datascience/dataframe.csv")

print("OK")

## Example2 access to s3 with geopandas
# pip install geopandas pyarrow pyogrio fsspec 'gdal<=3.7.3'
print("TESTING WITH GEOPANDAS... ", end='')

import geopandas as gpd

# However, GeoPandas 1.0 will switch to use pyogrio as the default engine, since pyogrio can provide a significant speedup compared to Fiona. We recommend to already install pyogrio.
gpd.options.io_engine = "pyogrio"

gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.LONGITUDE, df.LATITUDE))
gdf.to_parquet("s3://datascience/geodataframe.parquet")

print("OK")

## Example3 access to an s3 file with DuckDB
# pip install duckdb
print("TESTING WITH DUCKDB... ", end='')

import duckdb
request = duckdb.sql("CREATE SECRET secret (TYPE S3,PROVIDER CREDENTIAL_CHAIN,CHAIN 'env',ENDPOINT 's3.mesocentre.uca.fr');")
request = duckdb.sql("SELECT COUNT(*) FROM read_parquet('s3://datascience/geodataframe.parquet');")

print("OK")
request.show()