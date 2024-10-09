-- CREATE TABLE SILVER

CREATE EXTERNAL TABLE `breweries_silver`(
  `id` string, 
  `name` string, 
  `brewery_type` string, 
  `address_1` string, 
  `address_2` string, 
  `address_3` string, 
  `city` string, 
  `state_province` string, 
  `postal_code` string, 
  `longitude` string, 
  `latitude` string, 
  `phone` string, 
  `website_url` string, 
  `street` string)
PARTITIONED BY ( 
  `country` string, 
  `state` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://imbev-silver/breweries_silver'
TBLPROPERTIES (
  'parquet.compress'='SNAPPY', 
  'transient_lastDdlTime'='1727740033')

-- CREATE TABLE SILVER ANONOMIZADO

CREATE EXTERNAL TABLE `breweries_silver_anom`(
  `id` string, 
  `name` string, 
  `brewery_type` string, 
  `address_1` string, 
  `address_2` string, 
  `address_3` string, 
  `city` string, 
  `state_province` string, 
  `postal_code` string, 
  `longitude` string, 
  `latitude` string, 
  `phone` string, 
  `website_url` string, 
  `street` string)
PARTITIONED BY ( 
  `country` string, 
  `state` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://imbev-silver/breweries_silver_anom'
TBLPROPERTIES (
  'parquet.compress'='SNAPPY', 
  'transient_lastDdlTime'='1727741197')

-- CREATE TABLE GOLD

CREATE EXTERNAL TABLE `breweries_gold`(
  `brewery_type` string, 
  `aggregated` bigint)
PARTITIONED BY ( 
  `country` string, 
  `state` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://imbev-gold/breweries_gold'
TBLPROPERTIES (
  'parquet.compress'='SNAPPY', 
  'transient_lastDdlTime'='1727748500')