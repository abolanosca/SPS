
create or replace table {{ params.ds_stg }}.sps_chnnl_returns as
SELECT
    replace(cast( cast(dim_dates.fiscal_end_of_wk as date) as string),'-','') as WeekEndingDate,
    dim_vendors.vendor_id as vendor_id,
    dim_vendors.vendor_name as vendor_name,
    dim_products.brandname  AS brand_name,
    nested_sb_customer_transactions_daily__details.sku as sku,
    CASE LOWER(site_channels.website_name)
        WHEN 'backcountry'          THEN 10
        WHEN 'steepandcheap'        THEN 11
        WHEN 'competitive cyclist'  THEN 12
        WHEN 'amazon'               THEN 13
        WHEN 'ebay'                 THEN 14
        WHEN 'motosport'            THEN 16
        ELSE -1
    END                                                               website_id,
    CASE LOWER(site_channels.website_name)
        WHEN 'backcountry'          THEN '10-'||site_channels.website_name
        WHEN 'steepandcheap'        THEN '11-'||site_channels.website_name
        WHEN 'competitive cyclist'  THEN '12-'||site_channels.website_name
        WHEN 'amazon'               THEN '13-'||site_channels.website_name
        WHEN 'ebay'                 THEN '14-'||site_channels.website_name
        WHEN 'motosport'            THEN '16-'||site_channels.website_name
        ELSE '-1-Unknown'
    END                                                               website_nm,
    ABS(COALESCE(SUM(case when Transactions_Nested.transactions.transaction_event_type = 'return' then round(nested_sb_customer_transactions_daily__details.amount,2) else 0 end ), 0)) as return_sales,
    ABS(COALESCE(SUM(case when Transactions_Nested.transactions.transaction_event_type = 'return' then nested_sb_customer_transactions_daily__details.quantity else 0 end), 0)) as return_quantity,
    ABS(COALESCE(SUM(case when Transactions_Nested.transactions.transaction_event_type = 'return' then round(nested_sb_customer_transactions_daily__details.cogs_amount,2) else 0 end),0)) as return_cogs,
    ABS(COALESCE(SUM(case when Transactions_Nested.transactions.transaction_event_type = 'return' then round(nested_sb_customer_transactions_daily__details.amount,2) else 0 end ), 0)) - ABS(COALESCE(SUM(case when Transactions_Nested.transactions.transaction_event_type = 'return' then round(nested_sb_customer_transactions_daily__details.cogs_amount,2) else 0 end),0)) as return_gm


FROM {{ params.ds_elcap }}.vi_nested_sb_customer_transactions AS Transactions_Nested
LEFT JOIN unnest(details) as nested_sb_customer_transactions_daily__details
INNER JOIN {{ params.ds_elcap }}.sb_products AS dim_products
    ON nested_sb_customer_transactions_daily__details.sku = dim_products.sku
inner JOIN {{ params.ds_stg }}.sps_brand_vendor_map dim_vendors
    ON dim_products.brandid = dim_vendors.brand_id
INNER JOIN {{ params.ds_elcap }}.dim_dates dim_dates
    ON DATE(sb_event_date) = date(dim_dates.calendar_date)
INNER JOIN {{ params.ds_stg }}.sps_week_ending_date fd
    ON dim_dates.fiscal_yr_no =fd.fiscal_yr_no and dim_dates.fiscal_wk_no = fd.fiscal_wk_no

WHERE   DATE(sb_event_date) >= "2001-01-01"
and sb_Event_type = 'sales'
and transactions.transaction_event_type = 'return'
and LOWER(Transactions_Nested.catalogs.catalog_extid) IN('amazon','bcs','competitivecyclist','ebay','steepcheap','motosport')

GROUP BY replace(cast( cast(dim_dates.fiscal_end_of_wk as date) as string),'-',''),
dim_vendors.vendor_id,
dim_vendors.vendor_name,
dim_products.brandname,
nested_sb_customer_transactions_daily__details.sku,
CASE LOWER(site_channels.website_name)
        WHEN 'backcountry'          THEN 10
        WHEN 'steepandcheap'        THEN 11
        WHEN 'competitive cyclist'  THEN 12
        WHEN 'amazon'               THEN 13
        WHEN 'ebay'                 THEN 14
        WHEN 'motosport'            THEN 16
        ELSE -1
END,
CASE LOWER(site_channels.website_name)
        WHEN 'backcountry'          THEN '10-'||site_channels.website_name
        WHEN 'steepandcheap'        THEN '11-'||site_channels.website_name
        WHEN 'competitive cyclist'  THEN '12-'||site_channels.website_name
        WHEN 'amazon'               THEN '13-'||site_channels.website_name
        WHEN 'ebay'                 THEN '14-'||site_channels.website_name
        WHEN 'motosport'            THEN '16-'||site_channels.website_name
        ELSE '-1-Unknown'
END;
