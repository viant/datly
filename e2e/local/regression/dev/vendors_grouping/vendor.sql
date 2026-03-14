SELECT ACCOUNT_ID,
           USER_CREATED,
           SUM(ID) AS TOTAL_ID,
           MAX(ID) AS MAX_ID
    FROM VENDOR t
    WHERE t.ID IN ($criteria.AppendBinding($Unsafe.VendorIDs))
    GROUP BY 1, 2