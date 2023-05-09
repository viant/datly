/* {"Method":"PATCH","ResponseBody":{"From":"Campaign"}} */

import (
	"/Users/awitas/go/src/github.com/viant/datly/poc/xxx/.Campaign"
	"/Users/awitas/go/src/github.com/viant/datly/poc/xxx/.Entity"
)


#set($_ = $Campaign<*Campaign>(body/Entity))
    
#set($_ = $prevCampaign<[]*Campaign> /*
  SELECT * FROM CI_CAMPAIGN  WHERE  ID = $Campaign.Id
*/)
  
#set($_ = $advertiserIds /*
  SELECT ARRAY_AGG(Id) AS Values FROM `Campaign:/Advertiser/`
*/)   

#set($_ = $prevAdvertiser /* 
  SELECT * FROM CI_ADVERTISER
  WHERE  ID IN ( $advertiserIds.Values )
*/)
    
    
$sequencer.Allocate("CI_CAMPAIGN", $Campaign, "Id")
$sequencer.Allocate("CI_ADVERTISER", $Campaign, "Advertiser/Id")

    
#set($campaignById = $prevCampaign.IndexBy("Id"))
#set($advertiserById = $prevAdvertiser.IndexBy("Id"))

#if($Unsafe.Campaign)
  #if(($campaignById.HasKey($Unsafe.Campaign.Id) == true))
    $sql.Update($Campaign, "CI_CAMPAIGN");
  #else
    $sql.Insert($Campaign, "CI_CAMPAIGN");
  #end
#end