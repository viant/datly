/* {"Method":"patch","ResponseBody":{"From":"Entity"}} */


import (
	"campaign.Campaign"
	"campaign.Entity"
	"campaign.Advertiser"
	"campaign.Flights"
	"campaign.CampaignCreative"
	"campaign.Creative"
	"campaign.Audience"
	"campaign.BidOverride"
	"campaign.Event"
	"campaign.Acl"
	"campaign.Features"
	)


#set($_ = $Campaign<*Campaign>(body/Entity))
	#set($_ = $CurCampaignID<?>(param/Campaign) /*
? SELECT ARRAY_AGG(Id) AS Values FROM  `/` LIMIT 1
*/
)
	#set($_ = $CurCampaign<*Campaign>(data_view/CurCampaign) /*
? SELECT * FROM CI_CAMPAIGN
WHERE $criteria.In("ID", $CurCampaignID.Values)
*/
)
	#set($_ = $CurCampaignAdvertiserID<?>(param/Campaign) /*
? SELECT ARRAY_AGG(Id) AS Values FROM  `/Advertiser` LIMIT 1
*/
)
	#set($_ = $CurAdvertiser<*Advertiser>(data_view/CurAdvertiser) /*
? SELECT av.ID,
                           av.CURRENCY_ID,
                           DEFAULT_CHANNELS,
                           AGENCY_ID,
                           (SELECT ctz.IANA_TIMEZONE_STR FROM CI_TIME_ZONE ctz WHERE av.TIME_ZONE_ID = ctz.ID) AS IANA_TIMEZONE
                    FROM CI_ADVERTISER av

WHERE $criteria.In("ID", $CurCampaignAdvertiserID.Values)
*/
)
	#set($_ = $CurCampaignFlightsID<?>(param/Campaign) /*
? SELECT ARRAY_AGG(Id) AS Values FROM  `/Flights` LIMIT 1
*/
)
	#set($_ = $CurFlights<[]*Flights>(data_view/CurFlights) /*
? SELECT * FROM CI_CAMPAIGN_FLIGHT
WHERE $criteria.In("ID", $CurCampaignFlightsID.Values)
*/
)
	#set($_ = $CurCampaignCampaignCreativeID<?>(param/Campaign) /*
? SELECT ARRAY_AGG(Id) AS Values FROM  `/CampaignCreative` LIMIT 1
*/
)
	#set($_ = $CurCampaignCreative<[]*CampaignCreative>(data_view/CurCampaignCreative) /*
? SELECT * FROM CI_CAMPAIGN_CREATIVE
WHERE $criteria.In("ID", $CurCampaignCampaignCreativeID.Values)
*/
)
	#set($_ = $CurCampaignCreativeID<?>(param/Campaign) /*
? SELECT ARRAY_AGG(Id) AS Values FROM  `/Creative` LIMIT 1
*/
)
	#set($_ = $CurCreative<[]*Creative>(data_view/CurCreative) /*
? SELECT ID, ADVERTISER_ID, CAMPAIGN_ID FROM CI_CREATIVE c
WHERE $criteria.In("ID", $CurCampaignCreativeID.Values)
*/
)
	#set($_ = $CurCampaignAudienceID<?>(param/Campaign) /*
? SELECT ARRAY_AGG(Id) AS Values FROM  `/Audience` LIMIT 1
*/
)
	#set($_ = $CurAudience<[]*Audience>(data_view/CurAudience) /*
? SELECT au.ID, au.TARGET, au.EXCLUSION, ad.CAMPAIGN_ID  FROM CI_AUDIENCE au
          JOIN CI_AD_ORDER ad ON au.AD_ORDER_ID = ad.ID
WHERE $criteria.In("ID", $CurCampaignAudienceID.Values)
*/
)
	#set($_ = $CurCampaignBidOverrideID<?>(param/Campaign) /*
? SELECT ARRAY_AGG(Id) AS Values FROM  `/BidOverride` LIMIT 1
*/
)
	#set($_ = $CurBidOverride<[]*BidOverride>(data_view/CurBidOverride) /*
? SELECT * FROM CI_CAMPAIGN_BID_MULTIPLIER
WHERE $criteria.In("ID", $CurCampaignBidOverrideID.Values)
*/
)
	#set($_ = $CurCampaignEventID<?>(param/Campaign) /*
? SELECT ARRAY_AGG(Id) AS Values FROM  `/Event` LIMIT 1
*/
)
	#set($_ = $CurEvent<*Event>(data_view/CurEvent) /*
? SELECT * FROM CI_EVENT t
WHERE $criteria.In("ID", $CurCampaignEventID.Values)
*/
)
	#set($_ = $CurAcl<*Acl>(data_view/CurAcl) /*
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
    FROM CI_CONTACTS
*/
)
	#set($_ = $CurFeatures<*Features>(data_view/CurFeatures) /*
? SELECT
                        ID USER_ID,
                        HasAccountFeatureEnabled(ACCOUNT_ID, 'EXPOSE_DUAL_STATUS') AS DUAL_STATUS,
                        HasAccountFeatureEnabled(ACCOUNT_ID, 'EXPOSE_HOUSEHOLD_IDENTIFIER') AS HOUSEHOLD_IDENTIFIER,
                        HasAccountFeatureEnabled(ACCOUNT_ID, 'EXPOSE_CAMPAIGN_FLIGHTING') AS CAMPAIGN_FLIGHTING,
                        HasAccountFeatureEnabled(ACCOUNT_ID, 'EXPOSE_CHANNELS_V2') AS EXPOSE_CHANNELS_V2,
                        HasAccountFeatureEnabled(ACCOUNT_ID, 'EXPOSE_XDEVICE_FREQUENCY_CAPPING') AS XDEVICE_FREQUENCY_CAPPING
                    FROM CI_CONTACTS
*/
)


$sequencer.Allocate("CI_CAMPAIGN", $Campaign, "Id")
$sequencer.Allocate("CI_CAMPAIGN_FLIGHT", $Campaign, "Flights/Id")
$sequencer.Allocate("CI_CAMPAIGN_CREATIVE", $Campaign, "CampaignCreative/Id")
$sequencer.Allocate("CI_AUDIENCE", $Campaign, "Audience/Id")
$sequencer.Allocate("CI_CAMPAIGN_BID_MULTIPLIER", $Campaign, "BidOverride/Id")

#set($CurCampaignById = $CurCampaign.IndexBy("Id"))
#set($CurFlightsById = $CurFlights.IndexBy("Id"))
#set($CurCampaignCreativeById = $CurCampaignCreative.IndexBy("Id"))
#set($CurAudienceById = $CurAudience.IndexBy("Id"))
#set($CurBidOverrideById = $CurBidOverride.IndexBy("Id"))

#if($Unsafe.Campaign)
  #if($CurCampaignById.HasKey($Unsafe.Campaign.Id) == true)
    $sql.Update($Unsafe.Campaign, "CI_CAMPAIGN");
  #else
    $sql.Insert($Unsafe.Campaign, "CI_CAMPAIGN");
  #end
  
  #foreach($RecFlights in $Unsafe.Campaign.Flights)
    #set($RecFlights.CampaignId = $Unsafe.Campaign.Id)
    #if($CurFlightsById.HasKey($RecFlights.Id) == true)
      $sql.Update($RecFlights, "CI_CAMPAIGN_FLIGHT");
    #else
      $sql.Insert($RecFlights, "CI_CAMPAIGN_FLIGHT");
    #end
  #end
  
  #foreach($RecCampaignCreative in $Unsafe.Campaign.CampaignCreative)
    #set($RecCampaignCreative.CampaignId = $Unsafe.Campaign.Id)
    #if($CurCampaignCreativeById.HasKey($RecCampaignCreative.Id) == true)
      $sql.Update($RecCampaignCreative, "CI_CAMPAIGN_CREATIVE");
    #else
      $sql.Insert($RecCampaignCreative, "CI_CAMPAIGN_CREATIVE");
    #end
  #end
  
  #foreach($RecAudience in $Unsafe.Campaign.Audience)
    #if($CurAudienceById.HasKey($RecAudience.Id) == true)
      $sql.Update($RecAudience, "CI_AUDIENCE");
    #else
      $sql.Insert($RecAudience, "CI_AUDIENCE");
    #end
  #end
  
  #foreach($RecBidOverride in $Unsafe.Campaign.BidOverride)
    #set($RecBidOverride.CampaignId = $Unsafe.Campaign.Id)
    #if($CurBidOverrideById.HasKey($RecBidOverride.Id) == true)
      $sql.Update($RecBidOverride, "CI_CAMPAIGN_BID_MULTIPLIER");
    #else
      $sql.Insert($RecBidOverride, "CI_CAMPAIGN_BID_MULTIPLIER");
    #end
  #end
#end