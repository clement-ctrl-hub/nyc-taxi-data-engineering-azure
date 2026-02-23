-- ============================================
-- NYC Green Taxi - Configuration Synapse
-- External Tables (Silver / Gold)
-- ============================================

-- Credential (Managed Identity)
CREATE DATABASE SCOPED CREDENTIAL cred_tp
WITH IDENTITY = 'Managed Identity';


-- External Data Source - Silver
CREATE EXTERNAL DATA SOURCE source_silver
WITH (
    LOCATION = 'https://datalaketpdataeng3.blob.core.windows.net/silver',
    CREDENTIAL = cred_tp
);


-- External Data Source - Gold
CREATE EXTERNAL DATA SOURCE source_gold
WITH (
    LOCATION = 'https://datalaketpdataeng3.blob.core.windows.net/gold',
    CREDENTIAL = cred_tp
);


-- External File Format (Parquet)
CREATE EXTERNAL FILE FORMAT file_parquet
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);


-- External Table (Gold dataset)
CREATE EXTERNAL TABLE gold.tpdataset
WITH (
    LOCATION = 'tpdataset',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = file_parquet
)
AS
SELECT *
FROM gold.dataset;