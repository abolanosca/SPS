

/*sps_chnnl_summary_merge_gross_sales*/

MERGE INTO {{ params.ds_stg }}.sps_chnnl_summary target USING
(
    SELECT DISTINCT vendor_id
        , vendor_name
        , brand_name
        , sku
        , website_id
        , website_nm
        , shipped_sales_amount
        , shipped_quantity_amount
        , shipped_cogs_amount
        , shipped_gm
        , shipped_markdown
        , WeekEndingDate as week_ending_date
    FROM {{ params.ds_stg }}.sps_chnnl_gross_sales
    WHERE shipped_sales_amount IS NOT NULL
        OR shipped_quantity_amount IS NOT NULL
        OR shipped_cogs_amount IS NOT NULL
        OR shipped_gm IS NOT NULL
        OR shipped_markdown IS NOT NULL
) source
ON (target.sku = source.sku AND target.website_id = source.website_id AND target.vendor_id = source.vendor_id)
WHEN NOT MATCHED THEN
    INSERT
    ( vendor_id
      , vendor_name
      , brand_name
      , sku
      , website_id
      , website_nm
      , gross_sales
      , gross_quantity
      , gross_cogs
      , gross_gm
      , gross_markdown
      , week_ending_date
    )
    VALUES
    ( source.vendor_id
      , source.vendor_name
      , source.brand_name
      , source.sku
      , source.website_id
      , source.website_nm
      , cast(source.shipped_sales_amount as float64)
      , cast(source.shipped_quantity_amount as float64)
      , cast(source.shipped_cogs_amount as float64)
      , cast(source.shipped_gm as float64)
      , cast(source.shipped_markdown as float64)
      , source.week_ending_date
    )
;