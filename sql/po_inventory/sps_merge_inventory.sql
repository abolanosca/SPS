
MERGE INTO {{ params.ds_stg }}.sps_chnnl_summary target USING
(
    SELECT DISTINCT vendor_id
        , vendor_name
        , brand_name
        , sku
        , website_id
        , website_nm
        , inventory_quantity
        , inventory_cost
        , inventory_retail
        , WeekEndingDate as week_ending_date
    FROM {{ params.ds_stg }}.sps_chnnl_inventory
    WHERE inventory_quantity IS NOT NULL
        OR inventory_cost IS NOT NULL
) source
ON (target.sku = source.sku AND target.website_id = source.website_id AND target.vendor_id = source.vendor_id and target.week_ending_date = source.week_ending_date)
WHEN MATCHED THEN
UPDATE
    SET target.inventory_quantity = source.inventory_quantity
        , target.inventory_cost = cast(source.inventory_cost as float64)
        , target.inventory_retail = cast(source.inventory_retail as float64)
WHEN NOT MATCHED THEN
    INSERT
    ( vendor_id
      , vendor_name
      , brand_name
      , sku
      , website_id
      , website_nm
      , inventory_quantity
      , inventory_cost
      , inventory_retail
      , week_ending_date
    )
    VALUES
    ( source.vendor_id
      , source.vendor_name
      , source.brand_name
      , source.sku
      , source.website_id
      , source.website_nm
      , source.inventory_quantity
      , cast(source.inventory_cost as float64)
      , cast(source.inventory_retail as float64)
      , source.week_ending_date
    )
;
