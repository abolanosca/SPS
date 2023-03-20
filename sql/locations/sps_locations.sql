

CREATE or replace TABLE {{ params.ds_stg }}.sps_channel_locations AS
SELECT DISTINCT s.website_id    AS LocationID
    , s.website_id              AS StoreNumber
    , s.website_nm              AS StoreName
    , CAST(NULL AS string)     AS Address
    , CAST(NULL AS string)     AS City
    , CAST(NULL AS string)     AS State
    , CAST(NULL AS string)     AS PostalCode
    , CAST(NULL AS string)     AS Country
    , CAST(NULL AS string)     AS OpenDate
    , CAST(NULL AS string)     AS CloseDate
    , CAST(NULL AS string)     AS StoreRank
    , CAST(NULL AS string)     AS StoreClassification
    , CAST(NULL AS string)     AS LocationType
    , CAST(NULL AS string)     AS Demographic
    , CAST(NULL AS string)     AS Region
    , CAST(NULL AS string)     AS Climate
    , CAST(NULL AS string)     AS CompStoreIndicator
    , CAST(NULL AS string)     AS StoreSize
    , CAST(NULL AS string)     AS RetailerDivision
    , CAST(NULL AS string)     AS Banner
    , CAST(NULL AS string)     AS GLN
    , CAST(NULL AS string)     AS RetailerLocationAttribute22
    , CAST(NULL AS string)     AS RetailerLocationAttribute23
    , CAST(NULL AS string)     AS RetailerLocationAttribute24
    , CAST(NULL AS string)     AS RetailerLocationAttribute25
    , CAST(NULL AS string)     AS RetailerLocationAttribute26
    , CAST(NULL AS string)     AS RetailerLocationAttribute27
    , CAST(NULL AS string)     AS RetailerLocationAttribute28
    , CAST(NULL AS string)     AS RetailerLocationAttribute29
FROM {{ params.ds_stg }}.sps_chnnl_summary s
WHERE  s.website_id  <> -1
ORDER BY s.website_id