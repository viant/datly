SELECT * FROM  (
  SELECT ID,
       0                                                                              AS TYPE,
       LOWER(TRIM(d.DEAL_ID))                                                         AS DEAL_ID,
       COALESCE(PUBLISHER_ID, 0)                                                      AS PUBLISHER_ID,
       COALESCE(IS_MOBILE, 0)                                                         AS IS_MOBILE,
       COALESCE(IS_DESKTOP, 0)                                                        AS IS_DESKTOP,
       COALESCE(IS_TV, 0)                                                             AS IS_TV,
       COALESCE(IS_BANNER, 0)                                                         AS IS_BANNER,
       COALESCE(IS_MRAID, 0)                                                          AS IS_MRAID,
       COALESCE(IS_VAST, 0)                                                           AS IS_VAST,
       COALESCE(IS_NATIVE, 0)                                                         AS IS_NATIVE,
       if(COALESCE(d.ACCOUNT_ID, 0) = 0, true, false)                                 AS IS_ADELPHIC_DEAL,
       (CASE WHEN d.MIN_MARGIN IS NULL THEN COALESCE(m.MARKUP_PCT, 0.0) ELSE 0.0 END) AS MARKUP_PCT,
       COALESCE(m.MARKUP_CPM, 0.0)                                                    AS MARKUP_CPM,
       o.RATE,
       d.MIN_MARGIN,
       (CASE WHEN a.PMP_DEAL_ID IS NULL THEN TRUE ELSE FALSE END)                     AS DEAL_SYNDICATED
FROM CI_PMP_DEAL d
         LEFT JOIN (select PMP_DEAL_ID,
                           MAX(COALESCE(MARKUP_PCT, 0.0)) AS MARKUP_PCT,
                           MAX(COALESCE(MARKUP_CPM, 0.0)) AS MARKUP_CPM
                    FROM CI_PMP_DEAL_MARKUP
                    GROUP BY 1) m ON m.PMP_DEAL_ID = d.ID
         LEFT JOIN (select DEAL_ID, MAX(RATE) as RATE from CI_DEAL_TECH_FEE_OVERRIDE GROUP BY 1) o ON o.DEAL_ID = d.ID
         LEFT JOIN (SELECT DISTINCT PMP_DEAL_ID FROM CI_PMP_DEAL_ADVERTISER) a ON a.PMP_DEAL_ID = d.ID
WHERE d.ID IN (SELECT CAST(r.FEATURE_VALUE AS SIGNED)
               FROM `CI_TARGETING_RULE` r
               WHERE r.FEATURE_TYPE = 'ad.pmp.deal.id')
   UNION ALL
  SELECT d.PARENT_ID                                                                     AS ID,
       0                                                                               AS TYPE,
       LOWER(TRIM(d.DEAL_ID))                                                          AS DEAL_ID,
       COALESCE(d.PUBLISHER_ID, 0)                                                     AS PUBLISHER_ID,
       false                                                                              IS_MOBILE,
       false                                                                              IS_DESKTOP,
       false                                                                              IS_TV,
       false                                                                              IS_BANNER,
       false                                                                              IS_MRAID,
       false                                                                              IS_VAST,
       false                                                                              IS_NATIVE,
       if(COALESCE(cp.ACCOUNT_ID, 0) = 0, true, false)                                 AS IS_ADELPHIC_DEAL,
       (CASE WHEN cp.MIN_MARGIN IS NULL THEN COALESCE(m.MARKUP_PCT, 0.0) ELSE 0.0 END) AS MARKUP_PCT,
       COALESCE(m.MARKUP_CPM, 0.0)                                                     AS MARKUP_CPM,
       NULL                                                                               RATE,
       cp.MIN_MARGIN                                                                      MIN_MARGIN,
       (CASE WHEN a.PMP_DEAL_ID IS NULL THEN TRUE ELSE FALSE END)                      AS DEAL_SYNDICATED
FROM CI_PMP_DEAL_ENTRY d
         LEFT JOIN CI_PMP_DEAL cp ON cp.ID = d.PARENT_ID
         LEFT JOIN (select PMP_DEAL_ID,
                           MAX(COALESCE(MARKUP_PCT, 0.0)) AS MARKUP_PCT,
                           MAX(COALESCE(MARKUP_CPM, 0.0)) AS MARKUP_CPM
                    FROM CI_PMP_DEAL_MARKUP
                    GROUP BY 1) m ON TRIM(m.PMP_DEAL_ID) = cp.ID
         LEFT JOIN (SELECT DISTINCT PMP_DEAL_ID FROM CI_PMP_DEAL_ADVERTISER) a ON a.PMP_DEAL_ID = d.PARENT_ID
WHERE d.PARENT_ID IN (SELECT CAST(r.FEATURE_VALUE AS SIGNED)
                      FROM `CI_TARGETING_RULE` r
                      WHERE r.FEATURE_TYPE = 'ad.pmp.deal.id')
)  deal