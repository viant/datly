SELECT  Campaign.* /* { "Cardinality": "One", "Field":"Entity" } */,
        Advertiser.*,
        Flights.*
FROM (SELECT * FROM CI_CAMPAIGN) Campaign
JOIN (SELECT * FROM CI_ADVERTISER) Advertiser  ON Advertiser.ID = Campaign.ADVERTISER_ID AND 1=1
JOIN (SELECT * FROM CI_CAMPAIGN_FLIGHT) Flights  ON Campaign.ID = Flights.CAMPAIGN_ID
