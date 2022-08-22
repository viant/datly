/* {"URI":"vendors/"} */
SELECT vendor.*, $Meta.PAGE_CNT AS PAGE_COUNT, $Meta.TOTAL_COUNT AS TOTAL_COUNT, --// this become post parameter on vendor view
       products.* EXCEPT VENDOR_ID
FROM (SELECT t.*, $view.SQL AS DEUG_SQL  FROM VENDOR t WHERE 1=1 #if($period) 1 #end  ) vendor
JOIN (SELECT * FROM PRODUCT t) products ON products.VENDOR_ID = vendor.ID
JOIN (SELECT 1 + (COUNT(1) / ${parentView.Limit}) AS PAGE_CNT, COUNT(1) AS TOTAL_COUNT FROM ($parentView.NonWindowSQL)) AS Meta ON 1=1

