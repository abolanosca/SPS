
MERGE INTO {{ params.ds_stg }}.sps_chnnl_summary target USING
(
    SELECT DISTINCT vendor_id
        , vendor_name
        , brand_name
        , sku
        , website_id
        , website_nm
        , received_quantity
        , received_cost
        , received_retail
        , WeekEndingDate as week_ending_date
    FROM {{ params.ds_stg }}.sps_chnnl_receipts
    WHERE received_quantity IS NOT NULL
        OR received_cost IS NOT NULL
) source
ON (target.sku = source.sku AND target.website_id = source.website_id AND target.vendor_id = source.vendor_id and target.week_ending_date = source.week_ending_date)
WHEN MATCHED THEN
UPDATE
    SET target.received_quantity = source.received_quantity
        , target.received_cost = cast(source.received_cost as float64)
        , target.received_retail = cast(source.received_retail as float64)
WHEN NOT MATCHED THEN
    INSERT
    ( vendor_id
      , vendor_name
      , brand_name
      , sku
      , website_id
      , website_nm
      , received_quantity
      , received_cost
      , received_retail
      , week_ending_date
    )
    VALUES
    ( source.vendor_id
      , source.vendor_name
      , source.brand_name
      , source.sku
      , source.website_id
      , source.website_nm
      , source.received_quantity
      , cast(source.received_cost as float64)
      , cast(source.received_retail as float64)
      , source.week_ending_date
    )
;