/* {"URI":"ado",
   "Cache":{
         "Name": "fs",
         "Location": "/tmp/cache/${view.Name}",
         "TimeToLiveMs": 360000
         }
   } */
SELECT ado.*
FROM (SELECT * FROM CI_AD_ORDER) ado /* {"Cache":{"Ref":"fs"}} */
