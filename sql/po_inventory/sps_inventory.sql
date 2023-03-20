
CREATE OR REPLACE TABLE
  {{ params.ds_stg }}.sps_chnnl_inventory AS
SELECT
  replace(cast( cast(dates.fiscal_end_of_wk as date) as string),'-','') as WeekEndingDate,
  dim_vendors.vendor_id AS vendor_id,
  dim_vendors.vendor_name AS vendor_name,
  dim_products_transactions.brandname AS brand_name,
  15 website_id,
  '15-DC/Warehouse'website_nm,
  dim_products_transactions.sku AS SKU,
  COALESCE(SUM(CASE
        WHEN (DATE(base.sb_event_date_date, 'America/Denver')) = DATE_SUB(CURRENT_DATE(),INTERVAL 1 day) OR (DATE(base.sb_event_date_date, 'America/Denver')) = (DATE(dates.FISCAL_END_OF_WK, 'America/Denver')) THEN ROUND( base.on_hand_cost,2)
      ELSE
      NULL
    END
      ), 0) AS inventory_cost,
  COALESCE(SUM(CASE
        WHEN (DATE(base.sb_event_date_date, 'America/Denver')) = DATE_SUB(CURRENT_DATE(),INTERVAL 1 day) OR (DATE(base.sb_event_date_date, 'America/Denver')) = (DATE(dates.FISCAL_END_OF_WK, 'America/Denver')) THEN ROUND( base.on_hand_count,2)
      ELSE
      NULL
    END
      ), 0) AS inventory_quantity,
  COALESCE(SUM(CASE
        WHEN (DATE(base.sb_event_date_date, 'America/Denver')) = DATE_SUB(CURRENT_DATE(),INTERVAL 1 day) OR (DATE(base.sb_event_date_date, 'America/Denver')) = (DATE(dates.FISCAL_END_OF_WK, 'America/Denver')) THEN ROUND( base.on_hand_retail_value,2)
      ELSE
      NULL
    END
      ), 0) AS inventory_retail
FROM (
  SELECT
    (TIMESTAMP_TRUNC(Transactions_Nested.sb_event_date, DAY, 'America/Denver')) AS sb_event_date_date,
    nested_sb_customer_transactions_daily__details.sku AS sku,
    nested_sb_customer_transactions_daily__details.transaction_line_estimated_cost AS po_item_original_cost,
    CASE
      WHEN facilities.po_category = 'Closeout' THEN 'Closeout'
      WHEN facilities.po_category = 'Close-Out' THEN 'Closeout'
      WHEN facilities.po_category = 'close-out' THEN 'Closeout'
    ELSE
    'Retail'
  END
    AS po_category,
    CASE
      WHEN facilities.po_category = 'Closeout' THEN 'Closeout'
      WHEN facilities.po_category = 'Close-Out' THEN 'Closeout'
      WHEN facilities.po_category = 'close-out' THEN 'Closeout'
      WHEN facilities.po_category = 'Retail' THEN 'Retail'
    ELSE
    'Unknown'
  END
    AS po_category_org,
    Transactions_Nested.facilities.facility AS facility,
    Transactions_Nested.site_channels.website_name AS website_name,
    CASE
      WHEN Transactions_Nested.transactions.transaction_event_type = 'inventory_in_stock' THEN nested_sb_customer_transactions_daily__details.inventory_owned_flag
  END
    AS in_stock_variant_flag,
    nested_sb_customer_transactions_daily__details.inventory_owned_flag AS inventory_owned_flag,
    Transactions_Nested.sb_event_type AS sb_event_type,
    COALESCE(SUM(ROUND((
            CASE
              WHEN Transactions_Nested.transactions.transaction_event_type = 'inventory' THEN nested_sb_customer_transactions_daily__details.quantity
          END
            ),2) ), 0) AS on_hand_count,
    COALESCE(SUM(ROUND((
            CASE
              WHEN Transactions_Nested.transactions.transaction_event_type = 'inventory' THEN nested_sb_customer_transactions_daily__details.amount
          END
            ),2)), 0) AS on_hand_cost,
    COALESCE(SUM((
          CASE
            WHEN Transactions_Nested.transactions.transaction_event_type = 'inventory' THEN nested_sb_customer_transactions_daily__details.quantity
        END
          )* dim_products.regular_price ), 0) AS on_hand_retail_value,
  FROM
    {{ params.ds_elcap }}.vi_nested_sb_customer_transactions AS Transactions_Nested
  LEFT JOIN
    UNNEST(details) AS nested_sb_customer_transactions_daily__details
  LEFT JOIN
    {{ params.ds_elcap }}.vi_sb_products_no_loyalty_sku AS dim_products
  ON
    nested_sb_customer_transactions_daily__details.sku = dim_products.sku
  LEFT JOIN
    {{ params.ds_elcap }}.dim_dates AS dates
  ON
    (DATE(Transactions_Nested.sb_event_date, 'America/Denver')) = (DATE(dates.calendar_date, 'America/Denver'))
  LEFT JOIN
    {{ params.ds_elcap }}.dim_purchase_order AS dim_purchase_order
  ON
    Transactions_Nested.transactions.id = dim_purchase_order.po_number
    AND nested_sb_customer_transactions_daily__details.sku = dim_purchase_order.sku
    AND nested_sb_customer_transactions_daily__details.transaction_line_item_id = dim_purchase_order.po_purchase_order_item_id
  WHERE
    Transactions_Nested.sb_event_date >= (TIMESTAMP('2021-01-01 00:00:00', 'America/Denver'))
    AND ((nested_sb_customer_transactions_daily__details.sku ) <> 'BCCZ2F1-ONECOL-ONESIZ'
      OR (nested_sb_customer_transactions_daily__details.sku ) IS NULL)
  GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10) AS base
INNER JOIN
  {{ params.ds_elcap }}.dim_dates AS dates
ON
  (DATE(base.sb_event_date_date, 'America/Denver')) = (DATE(dates.calendar_date, 'America/Denver'))
INNER JOIN
  {{ params.ds_elcap }}.vi_sb_products_no_loyalty_sku AS dim_products_transactions
ON
  base.sku=dim_products_transactions.sku
inner JOIN {{ params.ds_stg }}.sps_brand_vendor_map dim_vendors
    ON dim_products_transactions.brandid = dim_vendors.brand_id
WHERE
  DATE(base.sb_event_date_date) IN (
  SELECT
    DISTINCT DATE(END_OF_WEEK)
  FROM
     {{ params.ds_stg }}.sps_week_ending_date)
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6,
  7;