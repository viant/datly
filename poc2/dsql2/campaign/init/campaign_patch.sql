SELECT  Campaign.* /* { "Cardinality": "One", "Field":"Entity" } */,
        Advertiser.*,
        Audience.*,
        CampaignCreative.*,
        BidOverride.*,
        Creative.*,
        Acl.*,
        Features.*
FROM (SELECT * FROM CI_CAMPAIGN) Campaign
LEFT JOIN (SELECT av.ID,
                           av.CURRENCY_ID,
                           DEFAULT_CHANNELS,
                           AGENCY_ID,
                           (SELECT ctz.IANA_TIMEZONE_STR FROM CI_TIME_ZONE ctz WHERE av.TIME_ZONE_ID = ctz.ID) AS IANA_TIMEZONE
                    FROM (CI_ADVERTISER) av
) Advertiser  ON Advertiser.ID = Campaign.ADVERTISER_ID AND 1=1
         LEFT JOIN (SELECT * FROM CI_CAMPAIGN_FLIGHT) Flights  ON Campaign.ID = Flights.CAMPAIGN_ID
         LEFT JOIN (SELECT * FROM CI_CAMPAIGN_CREATIVE) CampaignCreative ON CampaignCreative.CAMPAIGN_ID = Campaign.ID
         LEFT JOIN (SELECT ID, ADVERTISER_ID, CAMPAIGN_ID FROM (CI_CREATIVE) c) Creative ON Creative.CAMPAIGN_ID = Campaign.ID
         LEFT JOIN (SELECT au.ID, au.TARGET, au.EXCLUSION, ad.CAMPAIGN_ID  FROM CI_AUDIENCE au
          JOIN CI_AD_ORDER ad ON au.AD_ORDER_ID = ad.ID) Audience ON Audience.CAMPAIGN_ID = Campaign.ID

         LEFT JOIN (SELECT * FROM CI_CAMPAIGN_BID_MULTIPLIER) BidOverride ON BidOverride.CAMPAIGN_ID = Campaign.ID
         LEFT JOIN (SELECT * FROM (CI_EVENT) t) Event ON Event.CAMPAIGN_ID = Campaign.ID AND 1=1
         LEFT JOIN (
    SELECT ID USER_ID,
           ACCOUNT_ID,
           HasUserRole(ID, 'ROLE_READ_ONLY') AS IS_READ_ONLY,
           HasUserRole(ID, 'ROLE_BUSINESS_OWNER') AS IS_BUSINESS_OWNER,
           HasUserRole(ID, 'ROLE_ADMIN_CREATION') AS IS_ADMIN_CREATION,
           HasUserRole(ID, 'ROLE_ROLES_MANAGEMENT') AS IS_ROLES_MANAGEMENT,
           HasUserRole(ID, 'EXPOSE_COMMISSION') AS CAN_EXPOSE_COMMISSION,
           HasUserRole(ID, 'ROLE_ADELPHIC_INTERNAL') AS ROLE_ADELPHIC_INTERNAL,
           HasAgencyRole(1, ID,  'AGENCY_OWNER') IS_AGENCY_OWNER,
           HasAgencyRole(1, ID,  'AGENCY_CAMPAIGN_MEMBER') IS_AGENCY_CAMPAIGN_MEMBER,
           HasAdvertiserRole(1, ID,'ADVERTISER_OWNER') IS_ADVERTISER_OWNER,
           HasAdvertiserRole(1, ID,'CAMPAIGN_MEMBER') IS_CAMPAIGN_MEMBER
    FROM (CI_CONTACTS)
) Acl ON Acl.USER_ID = Campaign.CREATED_USER AND 1=1
         LEFT JOIN (SELECT
                        ID USER_ID,
                        HasAccountFeatureEnabled(ACCOUNT_ID, 'EXPOSE_DUAL_STATUS') AS DUAL_STATUS,
                        HasAccountFeatureEnabled(ACCOUNT_ID, 'EXPOSE_HOUSEHOLD_IDENTIFIER') AS HOUSEHOLD_IDENTIFIER,
                        HasAccountFeatureEnabled(ACCOUNT_ID, 'EXPOSE_CAMPAIGN_FLIGHTING') AS CAMPAIGN_FLIGHTING,
                        HasAccountFeatureEnabled(ACCOUNT_ID, 'EXPOSE_CHANNELS_V2') AS EXPOSE_CHANNELS_V2,
                        HasAccountFeatureEnabled(ACCOUNT_ID, 'EXPOSE_XDEVICE_FREQUENCY_CAPPING') AS XDEVICE_FREQUENCY_CAPPING
                    FROM (CI_CONTACTS)
) Features ON Features.USER_ID = Campaign.CREATED_USER AND 1=1




