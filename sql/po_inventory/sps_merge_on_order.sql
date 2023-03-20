
MERGE INTO {{ params.ds_stg }}.sps_chnnl_summary target USING
(
    SELECT DISTINCT vendor_id
        , vendor_name
        , brand_name
        , sku
        , website_id
        , website_nm
        , on_order_quantity
        ,  on_order_cost
        , on_order_retail
        , WeekEndingDate as week_ending_date
    FROM {{ params.ds_stg }}.sps_chnnl_onorder
    WHERE on_order_quantity IS NOT NULL
        OR on_order_cost IS NOT NULL
) source
ON (target.sku = source.sku AND target.website_id = source.website_id AND target.vendor_id = source.vendor_id AND target.week_ending_date = source.week_ending_date)
WHEN MATCHED THEN
UPDATE
    SET target.on_order_quantity = cast(source.on_order_quantity as float64)
        , target.on_order_cost = cast(source.on_order_cost as float64)
        , target.on_order_retail = cast(source.on_order_retail as float64)
WHEN NOT MATCHED THEN
    INSERT
    ( vendor_id
      , vendor_name
      , brand_name
      , sku
      , website_id
      , website_nm
      , on_order_quantity
      , on_order_cost
      , on_order_retail
      , week_ending_date
    )
    VALUES
    ( source.vendor_id
      , source.vendor_name
      , source.brand_name
      , source.sku
      , source.website_id
      , source.website_nm
      , cast(source.on_order_quantity as float64)
      , cast(source.on_order_cost as float64)
      , cast(source.on_order_retail as float64)
      , source.week_ending_date
    )
;
