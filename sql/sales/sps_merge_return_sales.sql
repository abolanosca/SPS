
/*sps_chnnl_summary_merge_returns*/
MERGE INTO {{ params.ds_stg }}.sps_chnnl_summary target USING
(
    SELECT DISTINCT vendor_id
        , vendor_name
        , brand_name
        , sku
        , website_id
        , website_nm
        , return_sales
        , return_quantity
        , return_cogs
        , return_gm
        , WeekEndingDate as week_ending_date
    FROM {{ params.ds_stg }}.sps_chnnl_returns
    WHERE return_sales IS NOT NULL
        OR return_quantity IS NOT NULL
        OR return_cogs IS NOT NULL
        OR return_gm IS NOT NULL
) source
ON (target.sku = source.sku AND target.website_id = source.website_id AND target.vendor_id = source.vendor_id and target.week_ending_date = source.week_ending_date)
WHEN MATCHED THEN
UPDATE
    SET target.return_sales = cast(source.return_sales as float64)
        , target.return_quantity = cast(source.return_quantity as float64)
        , target.return_cogs = cast(source.return_cogs as float64)
        , target.return_gm = cast(source.return_gm as float64)
WHEN NOT MATCHED THEN
    INSERT
    ( vendor_id
      , vendor_name
      , brand_name
      , sku
      , website_id
      , website_nm
      , return_sales
      , return_quantity
      , return_cogs
      , return_gm
      , week_ending_date
    )
    VALUES
    ( source.vendor_id
      , source.vendor_name
      , source.brand_name
      , source.sku
      , source.website_id
      , source.website_nm
      , cast(source.return_sales as float64)
      , cast(source.return_quantity as float64)
      , cast(source.return_cogs as float64)
      , cast(source.return_gm as float64)
      , source.week_ending_date
    );