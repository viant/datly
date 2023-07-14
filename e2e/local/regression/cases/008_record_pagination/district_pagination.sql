/* {"URI":"meta/districts"} */
SELECT districts.*,
       cities.*
FROM (SELECT t.* FROM DISTRICT t WHERE 1 = 1 AND ID IN ($IDs)) districts
JOIN (SELECT * FROM CITY t) cities /* {"Selector": { "Limit": 2 } } */ ON districts.ID = cities.DISTRICT_ID

