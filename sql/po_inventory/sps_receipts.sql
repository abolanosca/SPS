
create or replace table {{ params.ds_stg }}.sps_chnnl_receipts AS
SELECT
  replace(cast( cast(partition_dim_dates.fiscal_end_of_wk as date) as string),'-','') as WeekEndingDate,
  dim_vendors.vendor_id AS vendor_id,
  dim_vendors.vendor_name AS vendor_name,
  dim_products_transactions.brandname AS brand_name,
  dim_products_transactions.sku AS SKU,
  15 as website_id,
  '15-DC/Warehouse' as website_nm,
    COALESCE(SUM(pdt_retail_store_merch_report.po_received_quantity ), 0) AS received_quantity,
    COALESCE(SUM(pdt_retail_store_merch_report.po_received_cost ), 0) AS received_cost,
    COALESCE(SUM(pdt_retail_store_merch_report.po_received_retail_value ), 0) AS received_retail
FROM {{ params.ds_elcap }}.merch_transactions_final AS pdt_retail_store_merch_report
INNER JOIN {{ params.ds_elcap }}.dim_dates  AS partition_dim_dates ON (DATE(pdt_retail_store_merch_report.sb_event_date_date , 'America/Denver')) = (DATE(partition_dim_dates.calendar_date , 'America/Denver'))
INNER JOIN
   {{ params.ds_elcap }}.vi_sb_products_no_loyalty_sku AS dim_products_transactions
ON
  pdt_retail_store_merch_report.sku=dim_products_transactions.sku
inner JOIN  {{ params.ds_stg }}.sps_brand_vendor_map dim_vendors
    ON dim_products_transactions.brandid = dim_vendors.brand_id
WHERE date(pdt_retail_store_merch_report.sb_event_date_date) > date('2001-01-01')

and partition_dim_dates.FISCAL_WK_NO in (select fiscal_wk_no from {{ params.ds_stg }}.sps_week_ending_date)
and  partition_dim_dates.FISCAL_YR_NO in (select fiscal_yr_no from {{ params.ds_stg }}.sps_week_ending_date)

GROUP BY
    1,
    2,
    3,4,5,6,7

  ;