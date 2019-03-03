CREATE EXTERNAL TABLE IF NOT EXISTS %s (
  %s
) PARTITIONED BY (
  year int,
  month int,
  day int,
  hour int
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
) LOCATION 's3://haystack-spans-int/sql/%s/'
TBLPROPERTIES ('has_encrypted_data'='false');
