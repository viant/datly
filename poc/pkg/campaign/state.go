package campaign

type State struct {
	Campaign *Campaign `datly:"kind=body,in=Entity"`

	/*
	   ? SELECT ARRAY_AGG(Id) AS Values FROM  `/` LIMIT 1
	*/
	CampaignId *struct{ Values []int } `datly:"kind=param,in=Campaign"`

	/*
	    ? SELECT * FROM CI_CAMPAIGN
	   WHERE $criteria.In("ID", $CampaignId.Values)
	*/
	CurCampaign *Campaign `datly:"kind=data_view,in=CurCampaign"`

	/*
	   ? SELECT ARRAY_AGG(Id) AS Values FROM  `/Advertiser/` LIMIT 1
	*/
	AdvertiserId *struct{ Values []int } `datly:"kind=param,in=Advertiser"`

	/*
	    ? SELECT av.ID,
	                              av.CURRENCY_ID,
	                              DEFAULT_CHANNELS,
	                              AGENCY_ID,
	                              (SELECT ctz.IANA_TIMEZONE_STR FROM CI_TIME_ZONE ctz WHERE av.TIME_ZONE_ID = ctz.ID) AS IANA_TIMEZONE
	                       FROM CI_ADVERTISER av
	   WHERE $criteria.In("ID", $AdvertiserId.Values)
	*/
	CurAdvertiser *Advertiser `datly:"kind=data_view,in=CurAdvertiser"`

	/*
	   ? SELECT ARRAY_AGG(Id) AS Values FROM  `/Flights/` LIMIT 1
	*/
	FlightsId *struct{ Values []int } `datly:"kind=param,in=Flights"`

	/*
	    ? SELECT * FROM CI_CAMPAIGN_FLIGHT
	   WHERE $criteria.In("ID", $FlightsId.Values)
	*/
	CurFlights []*Flights `datly:"kind=data_view,in=CurFlights"`

	/*
	   ? SELECT ARRAY_AGG(Id) AS Values FROM  `/CampaignCreative/` LIMIT 1
	*/
	CampaignCreativeId *struct{ Values []int } `datly:"kind=param,in=CampaignCreative"`

	/*
	    ? SELECT * FROM CI_CAMPAIGN_CREATIVE
	   WHERE $criteria.In("ID", $CampaignCreativeId.Values)
	*/
	CurCampaignCreative []*CampaignCreative `datly:"kind=data_view,in=CurCampaignCreative"`

	/*
	   ? SELECT ARRAY_AGG(Id) AS Values FROM  `/Creative/` LIMIT 1
	*/
	CreativeId *struct{ Values []int } `datly:"kind=param,in=Creative"`

	/*
	    ? SELECT ID, ADVERTISER_ID, CAMPAIGN_ID FROM CI_CREATIVE
	   WHERE $criteria.In("ID", $CreativeId.Values)
	*/
	CurCreative []*Creative `datly:"kind=data_view,in=CurCreative"`

	/*
	   ? SELECT ARRAY_AGG(Id) AS Values FROM  `/Audience/` LIMIT 1
	*/
	AudienceId *struct{ Values []int } `datly:"kind=param,in=Audience"`

	/*
	    ? SELECT au.ID, au.TARGET, au.EXCLUSION, ad.CAMPAIGN_ID  FROM CI_AUDIENCE au
	             JOIN CI_AD_ORDER ad ON au.AD_ORDER_ID = ad.ID
	   WHERE $criteria.In("ID", $AudienceId.Values)
	*/
	CurAudience []*Audience `datly:"kind=data_view,in=CurAudience"`

	/*
	   ? SELECT ARRAY_AGG(Id) AS Values FROM  `/BidOverride/` LIMIT 1
	*/
	BidOverrideId *struct{ Values []int } `datly:"kind=param,in=BidOverride"`

	/*
	    ? SELECT * FROM CI_CAMPAIGN_BID_MULTIPLIER
	   WHERE $criteria.In("ID", $BidOverrideId.Values)
	*/
	CurBidOverride []*BidOverride `datly:"kind=data_view,in=CurBidOverride"`

	/*
	   ? SELECT ARRAY_AGG(Id) AS Values FROM  `/Event/` LIMIT 1
	*/
	EventId *struct{ Values []int } `datly:"kind=param,in=Event"`

	/*
	    ? SELECT * FROM CI_EVENT
	   WHERE $criteria.In("ID", $EventId.Values)
	*/
	CurEvent *Event `datly:"kind=data_view,in=CurEvent"`

	/*
	   ? SELECT ID USER_ID,
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
	*/
	CurAcl *Acl `datly:"kind=data_view,in=CurAcl"`

	/*
	   ? SELECT
	                          ID USER_ID,
	                          HasAccountFeatureEnabled(ACCOUNT_ID, 'EXPOSE_DUAL_STATUS') AS DUAL_STATUS,
	                          HasAccountFeatureEnabled(ACCOUNT_ID, 'EXPOSE_HOUSEHOLD_IDENTIFIER') AS HOUSEHOLD_IDENTIFIER,
	                          HasAccountFeatureEnabled(ACCOUNT_ID, 'EXPOSE_CAMPAIGN_FLIGHTING') AS CAMPAIGN_FLIGHTING,
	                          HasAccountFeatureEnabled(ACCOUNT_ID, 'EXPOSE_CHANNELS_V2') AS EXPOSE_CHANNELS_V2,
	                          HasAccountFeatureEnabled(ACCOUNT_ID, 'EXPOSE_XDEVICE_FREQUENCY_CAPPING') AS XDEVICE_FREQUENCY_CAPPING
	                      FROM (CI_CONTACTS)
	*/
	CurFeatures *Features `datly:"kind=data_view,in=CurFeatures"`
}
