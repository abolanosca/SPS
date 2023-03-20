CREATE or replace TABLE {{ params.ds_stg }}.sps_chnnl_products AS
SELECT DISTINCT dpc.sku         AS ProductID
    , 'SKU'                     AS ProductIDType
    ,  s.vendor_id  VendorCode
    ,  s.vendor_name  VendorName
    , dpc.brandid               AS BrandCode
    , dpc.brandname             AS BrandName
    , CAST(NULL AS string)     AS DUNSNumber
    , CAST(NULL AS string)     AS DivisionCode
    , dpc.industry              AS DivisionName
    , dpc.merchdivisionid       AS MajorDepartmentCode
    , dpc.merchdivisiondesc     AS MajorDepartmentName
    , dpc.merchgroupid          AS DepartmentCode
    , dpc.merchgroupdesc        AS DepartmentName
    , dpc.productgroupid        AS SubDepartmentCode
    , dpc.productgroupdesc      AS SubDepartmentName
    , CAST(NULL AS string)     AS MajorClassCode
    , CAST(NULL AS string)     AS MajorClassName
    , CAST(NULL AS string)     AS ClassCode
    , CAST(NULL AS string)     AS ClassName
    , CAST(NULL AS string)     AS SubClassCode
    , CAST(NULL AS string)     AS SubClassName
    , CAST(NULL AS string)     AS CategoryCode
    , CAST(NULL AS string)     AS CategoryName
    , CAST(NULL AS string)     AS ProductGroupCode
    , CAST(NULL AS string)     AS ProductGroupName
    , dpc.gender                AS Gender
    , dpc.styleid               AS StyleNumber
    , dpc.stylename             AS StyleDescription
    , dpc.axis1title            AS ColorCode
    , dpc.axis1value            AS ColorName
    , dpc.axis2title            AS SizeCode
    , dpc.axis2value            AS SizeName
    , case when u.upc='-1'   then CAST(NULL AS string)    else u.upc end            AS UPC
    , dpc.sku                   AS SKU
    , CAST(NULL AS string)     AS VendorStyleNumber
    , CAST(NULL AS string)     AS VendorStyleDescription
    , dpc.skudesc               AS ProductDescription
    , CAST(NULL AS string)     AS CorporateUnitAcquiredCost
    , CAST(NULL AS string)     AS CorporateUnitAdjustedCost
    , CAST(NULL AS string)     AS CorporateUnitOwnedRetailPrice
    , CAST(NULL AS string)     AS ReplenishmentIndicator
    , CAST(NULL AS string)     AS MarkdownIndicator
    , r.firstreceiptdate        AS FirstReceiptDate
    , r.lastreceiptdate         AS LastReceiptDate
    , dpc.season                AS ProductSeason
    , r.Age                     AS Age
    , CAST(NULL AS string)     AS UnitOfMeasure
    , CAST(NULL AS string)     AS FEDASCode
    , CAST(NULL AS string)     AS RetailerProductAttribute49
    , CAST(NULL AS string)     AS RetailerProductAttribute50
    , CAST(NULL AS string)     AS RetailerProductAttribute51
    , CAST(NULL AS string)     AS RetailerProductAttribute52
    , CAST(NULL AS string)     AS RetailerProductAttribute53
    , CAST(NULL AS string)     AS RetailerProductAttribute54
    , CAST(NULL AS string)     AS RetailerProductAttribute55
    , CAST(NULL AS string)     AS RetailerProductAttribute56
    , CAST(NULL AS string)     AS RetailerProductAttribute57
    , CAST(NULL AS string)     AS RetailerProductAttribute58
    , CAST(NULL AS string)     AS RetailerProductAttribute59
    , CAST(NULL AS string)     AS RetailerProductAttribute60
FROM {{ params.ds_stg }}.sps_chnnl_summary s
    LEFT JOIN {{ params.ds_stg }}.sps_chnnl_upc u on s.sku=u.sku
	LEFT  JOIN {{ params.ds_elcap }}.vi_sb_products_no_loyalty_sku AS dpc ON s.sku = dpc.sku
    LEFT JOIN {{ params.ds_stg }}.sps_chnnl_first_last_receipt r ON s.sku = r.sku
ORDER BY sku
;
