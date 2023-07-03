package campaign

type CampaignSlice []*Campaign
type IndexedCampaign map[int]*Campaign

func (c CampaignSlice) IndexById() IndexedCampaign {
	var result = IndexedCampaign{}
	for i, item := range c {
		if item != nil {
			result[item.Id] = c[i]
		}
	}
	return result
}

type AdvertiserSlice []*Advertiser
type IndexedAdvertiser map[int]*Advertiser

func (c AdvertiserSlice) IndexById() IndexedAdvertiser {
	var result = IndexedAdvertiser{}
	for i, item := range c {
		if item != nil {
			result[item.Id] = c[i]
		}
	}
	return result
}

type FlightsSlice []*Flights
type IndexedFlights map[int]*Flights

func (c FlightsSlice) IndexById() IndexedFlights {
	var result = IndexedFlights{}
	for i, item := range c {
		if item != nil {
			result[item.Id] = c[i]
		}
	}
	return result
}

type CampaignCreativeSlice []*CampaignCreative
type IndexedCampaignCreative map[int]*CampaignCreative

func (c CampaignCreativeSlice) IndexById() IndexedCampaignCreative {
	var result = IndexedCampaignCreative{}
	for i, item := range c {
		if item != nil {
			result[item.Id] = c[i]
		}
	}
	return result
}

type CreativeSlice []*Creative
type IndexedCreative map[int]*Creative

func (c CreativeSlice) IndexById() IndexedCreative {
	var result = IndexedCreative{}
	for i, item := range c {
		if item != nil {
			result[item.Id] = c[i]
		}
	}
	return result
}

type AudienceSlice []*Audience
type IndexedAudience map[int]*Audience

func (c AudienceSlice) IndexById() IndexedAudience {
	var result = IndexedAudience{}
	for i, item := range c {
		if item != nil {
			result[item.Id] = c[i]
		}
	}
	return result
}

type BidOverrideSlice []*BidOverride
type IndexedBidOverride map[int]*BidOverride

func (c BidOverrideSlice) IndexById() IndexedBidOverride {
	var result = IndexedBidOverride{}
	for i, item := range c {
		if item != nil {
			result[item.Id] = c[i]
		}
	}
	return result
}

type EventSlice []*Event
type IndexedEvent map[int]*Event

func (c EventSlice) IndexById() IndexedEvent {
	var result = IndexedEvent{}
	for i, item := range c {
		if item != nil {
			result[item.Id] = c[i]
		}
	}
	return result
}

type AclSlice []*Acl
type IndexedAcl map[int]*Acl

func (c AclSlice) IndexByUserId() IndexedAcl {
	var result = IndexedAcl{}
	for i, item := range c {
		if item != nil {
			result[item.UserId] = c[i]
		}
	}
	return result
}

type FeaturesSlice []*Features
type IndexedFeatures map[int]*Features

func (c FeaturesSlice) IndexByUserId() IndexedFeatures {
	var result = IndexedFeatures{}
	for i, item := range c {
		if item != nil {
			result[item.UserId] = c[i]
		}
	}
	return result
}

func (c IndexedCampaign) Has(key int) bool {
	_, ok := c[key]
	return ok
}
func (c IndexedAdvertiser) Has(key int) bool {
	_, ok := c[key]
	return ok
}
func (c IndexedFlights) Has(key int) bool {
	_, ok := c[key]
	return ok
}
func (c IndexedCampaignCreative) Has(key int) bool {
	_, ok := c[key]
	return ok
}
func (c IndexedCreative) Has(key int) bool {
	_, ok := c[key]
	return ok
}
func (c IndexedAudience) Has(key int) bool {
	_, ok := c[key]
	return ok
}
func (c IndexedBidOverride) Has(key int) bool {
	_, ok := c[key]
	return ok
}
func (c IndexedEvent) Has(key int) bool {
	_, ok := c[key]
	return ok
}
func (c IndexedAcl) Has(key int) bool {
	_, ok := c[key]
	return ok
}
func (c IndexedFeatures) Has(key int) bool {
	_, ok := c[key]
	return ok
}
