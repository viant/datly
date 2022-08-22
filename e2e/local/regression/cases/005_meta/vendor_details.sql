/* {"URI":"vendors/"} */
SELECT vendor.*,
       products.* EXCEPT VENDOR_ID,
       meta.* /* {"Output":{"In":"Header", "Name":"Vendor-Meta", "Case":"lc"}} */
       productMeta.* /* {"Output":{"In":"record", "Name":"ProductMeta", "Case":"lc"}} */

FROM (SELECT t.*, '$View.SQL' AS DEUG_SQL  FROM VENDOR t WHERE 1=1 #if($period) 1 #end  ) vendor
    JOIN (SELECT * FROM PRODUCT t) products ON products.VENDOR_ID = vendor.ID
    JOIN (SELECT 1 + (COUNT(1) / ${parentView.Limit}) AS PAGE_CNT, COUNT(1) AS TOTAL_COUNT
                 FROM ($View.vendor.NonWindowSQL)) AS meta ON 1=1

    JOIN (SELECT 1 + (COUNT(1) / ${parentView.Limit}) AS PAGE_CNT, COUNT(1) AS TOTAL_COUNT
    FROM ($View.products.NonWindowSQL)) AS productMeta ON 1=1
