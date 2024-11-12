/* {"URI":"meta/vendors/", "CaseFormat":"lc"} */

#set( $_ = $VendorId<[]int>(query/ids).WithPredicate(0,'in','t','id').Optional())
#set( $_ = $VendorName<string>(query/name).WithPredicate(0,'equal','t','id').Optional())
#set( $_ = $Meta<?>(output/summary))
#set( $_ = $Data<?>(output/view))
#set( $_ = $Status<?>(output/status).Embed())

SELECT vendor.*,
       products.* EXCEPT VENDOR_ID
FROM (SELECT t.* FROM VENDOR t WHERE 1=1
                    ${predicate.Builder().CombineOr(
                         $predicate.FilterGroup(0, "AND")
                       ).Build("AND")}

  ) vendor
    JOIN (SELECT ID, NAME AS my_name /* {"IgnoreCaseFormatter":true} */, VENDOR_ID   FROM PRODUCT t) products  ON products.VENDOR_ID = vendor.ID
    JOIN (
        SELECT
            CAST(1 + (COUNT(1) / $View.Limit) AS SIGNED)  AS PAGE_CNT,
            COUNT(1) AS CNT
           FROM ($View.vendor.SQL)  t
    ) AS Meta ON 1=1
