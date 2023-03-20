CREATE or replace TABLE {{ params.ds_stg }}.sps_group_chnnl_activity AS
SELECT  week_ending_date                      AS WeekEndingDate
    , s.sku                                                     AS ProductID
    , case when website_id =-1 then 10 else website_id end      AS LocationID
    , sum(cast(gross_quantity   as float64) )                                          AS GrossSalesUnits
    , sum(cast(gross_cogs  as float64) )                                     AS GrossSalesCost
    , sum(cast(gross_sales  as float64) )                                    AS GrossSalesRetail
    , sum( cast(gross_gm  as float64) )                                        AS GrossSalesMarginValue
    , sum(cast(coalesce(gross_quantity, 0) - coalesce(return_quantity, 0)   as float64))         AS NetSalesUnits
    , sum(cast(coalesce(gross_cogs, 0) - coalesce(return_cogs, 0)  as float64) )       AS NetSalesCost
    , sum(cast(coalesce(gross_sales, 0) - coalesce(return_sales, 0) as float64) )     AS NetSalesRetail
    , sum(cast(coalesce(gross_gm, 0) - coalesce(return_gm, 0)   as float64)  )        AS NetSalesMarginValue
    , sum(cast(return_quantity  as float64)    )                                       AS CustomerReturnUnits
    , sum(cast(return_cogs  as float64)   )                                           AS CustomerReturnCost
    , sum(cast(return_sales  as float64)  )                                           AS CustomerReturnRetail
    , sum(cast(return_gm as float64)     )                                            AS CustomerReturnMarginValue
    , sum(cast(inventory_quantity as float64)  )                                       AS InventoryUnits
    , sum(cast(inventory_cost as float64)   )                                         AS InventoryCost
    , sum(cast(inventory_retail as float64))    AS InventoryRetail
    , sum(cast(on_order_quantity as float64)  )                                       AS OnOrderUnits
    , sum(cast(on_order_cost as float64)   )                                         AS OnOrderCost
    , sum(cast(on_order_retail as float64) )                            AS OnOrderRetail
    , sum(cast(received_quantity as float64)   )                                      AS ReceiptUnits
    , sum(cast(received_retail as float64)  )                                AS ReceiptRetail
    , sum(cast(received_cost as float64)  )                                          AS ReceiptCost
    , CAST(NULL AS string)                                     AS InTransitUnits
    , CAST(NULL AS string)                                     AS InTransitCost
    , CAST(NULL AS string)                                     AS InTransitRetail
    , CAST(NULL AS string)                                     AS ReturnToVendorUnits
    , CAST(NULL AS string)                                     AS ReturnToVendorCost
    , CAST(NULL AS string)                                     AS ReturnToVendorRetail
    , CAST(NULL AS string)                                     AS InventoryAdjustmentUnits
    , CAST(NULL AS string)                                     AS InventoryAdjustmentCost
    , CAST(NULL AS string)                                     AS InventoryAdjustmentRetail
    , CAST(NULL AS string)                                     AS PermanentMarkdownValues
    , CAST(NULL AS string)                                     AS PointOfSaleMarkdownValues
    , sum(cast(gross_markdown as float64) )                                        AS TotalMarkdownValues

FROM {{ params.ds_stg }}.sps_chnnl_summary s
GROUP BY 1,2,3

;


CREATE or replace TABLE {{ params.ds_stg }}.sps_chnnl_activity AS
SELECT WeekEndingDate
    , ProductID
    , LocationID
    , GrossSalesUnits
    , GrossSalesCost
    , GrossSalesRetail
    , GrossSalesMarginValue
    , NetSalesUnits
    , NetSalesCost
    , NetSalesRetail
    , NetSalesMarginValue
    , CustomerReturnUnits
    , CustomerReturnCost
    , CustomerReturnRetail
    , CustomerReturnMarginValue
    , InventoryUnits
    , InventoryCost
    , InventoryRetail
    , OnOrderUnits
    , OnOrderCost
    , OnOrderRetail
    , ReceiptUnits
    , ReceiptRetail
    , ReceiptCost
    , InTransitUnits
    , InTransitCost
    , InTransitRetail
    , ReturnToVendorUnits
    , ReturnToVendorCost
    , ReturnToVendorRetail
    , InventoryAdjustmentUnits
    , InventoryAdjustmentCost
    , InventoryAdjustmentRetail
    , PermanentMarkdownValues
    , PointOfSaleMarkdownValues
    , TotalMarkdownValues
    ,  cast(coalesce(p.wholesale_price_history,p.wholesale_price) as float64)                                       AS UnitCostPrice
    ,  cast(coalesce(p.regular_price_history,p.regular_price) as float64)                                          AS UnitRetailPrice
FROM {{ params.ds_stg }}.sps_group_chnnl_activity s
    LEFT JOIN {{ params.ds_stg }}.sps_chnnl_products_prices p ON s.ProductID = p.sku

;
