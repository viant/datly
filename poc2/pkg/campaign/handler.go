package campaign

import (
	"context"
	"github.com/viant/xdatly/handler"
)

type Handler struct{}

func (h *Handler) Exec(ctx context.Context, sess handler.Session) (interface{}, error) {
	state := &State{}
	if err := sess.Stater().Into(ctx, state); err != nil {
		return nil, err
	}

	sql, err := sess.Db()
	if err != nil {
		return nil, err
	}
	sequencer := sql

	campaign := state.Campaign
	curAdvertiser := state.CurAdvertiser
	curFlights := state.CurFlights
	curCampaignCreative := state.CurCampaignCreative
	curCreative := state.CurCreative
	curAudience := state.CurAudience
	curBidOverride := state.CurBidOverride
	curEvent := state.CurEvent
	curAcl := state.CurAcl
	curFeatures := state.CurFeatures
	curCampaign := state.CurCampaign

	sequencer.Allocate(ctx, "CI_CAMPAIGN", campaign, "Id")
	sequencer.Allocate(ctx, "CI_ADVERTISER", campaign, "Advertiser/Id")
	sequencer.Allocate(ctx, "CI_CAMPAIGN_FLIGHT", campaign, "Flights/Id")
	sequencer.Allocate(ctx, "CI_CAMPAIGN_CREATIVE", campaign, "CampaignCreative/Id")
	sequencer.Allocate(ctx, "CI_CREATIVE", campaign, "Creative/Id")
	sequencer.Allocate(ctx, "CI_AUDIENCE", campaign, "Audience/Id")
	sequencer.Allocate(ctx, "CI_CAMPAIGN_BID_MULTIPLIER", campaign, "BidOverride/Id")
	sequencer.Allocate(ctx, "CI_EVENT", campaign, "Event/Id")
	sequencer.Allocate(ctx, "CI_CONTACTS", campaign, "Acl/UserId")
	sequencer.Allocate(ctx, "CI_CONTACTS", campaign, "Features/UserId")

	curCampaignById := CampaignSlice([]*Campaign{curCampaign}).IndexById()
	curAdvertiserById := AdvertiserSlice([]*Advertiser{curAdvertiser}).IndexById()
	curFlightsById := FlightsSlice(curFlights).IndexById()
	curCampaignCreativeById := CampaignCreativeSlice(curCampaignCreative).IndexById()
	curCreativeById := CreativeSlice(curCreative).IndexById()
	curAudienceById := AudienceSlice(curAudience).IndexById()
	curBidOverrideById := BidOverrideSlice(curBidOverride).IndexById()
	curEventById := EventSlice([]*Event{curEvent}).IndexById()
	curAclByUserId := AclSlice([]*Acl{curAcl}).IndexByUserId()
	curFeaturesByUserId := FeaturesSlice([]*Features{curFeatures}).IndexByUserId()

	if campaign != nil {

		if curCampaignById.Has(campaign.Id) == true {

			sql.Update("CI_CAMPAIGN", campaign)
		} else {

			sql.Insert("CI_CAMPAIGN", campaign)
		}

		if campaign.Advertiser != nil {

			campaign.Advertiser.Id = campaign.AdvertiserId
			if curAdvertiserById.Has(campaign.Advertiser.Id) == true {

				sql.Update("CI_ADVERTISER", campaign.Advertiser)
			} else {

				sql.Insert("CI_ADVERTISER", campaign.Advertiser)
			}
		}

		for _, recFlights := range campaign.Flights {

			recFlights.CampaignId = campaign.Id
			if curFlightsById.Has(recFlights.Id) == true {

				sql.Update("CI_CAMPAIGN_FLIGHT", recFlights)
			} else {

				sql.Insert("CI_CAMPAIGN_FLIGHT", recFlights)
			}
		}

		for _, recCampaignCreative := range campaign.CampaignCreative {

			recCampaignCreative.CampaignId = campaign.Id
			if curCampaignCreativeById.Has(recCampaignCreative.Id) == true {

				sql.Update("CI_CAMPAIGN_CREATIVE", recCampaignCreative)
			} else {

				sql.Insert("CI_CAMPAIGN_CREATIVE", recCampaignCreative)
			}
		}

		for _, recCreative := range campaign.Creative {

			recCreative.CampaignId = &campaign.Id
			if curCreativeById.Has(recCreative.Id) == true {

				sql.Update("CI_CREATIVE", recCreative)
			} else {

				sql.Insert("CI_CREATIVE", recCreative)
			}
		}

		for _, recAudience := range campaign.Audience {

			recAudience.CampaignId = &campaign.Id
			if curAudienceById.Has(recAudience.Id) == true {

				sql.Update("CI_AUDIENCE", recAudience)
			} else {

				sql.Insert("CI_AUDIENCE", recAudience)
			}
		}

		for _, recBidOverride := range campaign.BidOverride {

			recBidOverride.CampaignId = campaign.Id
			if curBidOverrideById.Has(recBidOverride.Id) == true {

				sql.Update("CI_CAMPAIGN_BID_MULTIPLIER", recBidOverride)
			} else {

				sql.Insert("CI_CAMPAIGN_BID_MULTIPLIER", recBidOverride)
			}
		}

		if campaign.Event != nil {

			campaign.Event.CampaignId = &campaign.Id
			if curEventById.Has(campaign.Event.Id) == true {

				sql.Update("CI_EVENT", campaign.Event)
			} else {

				sql.Insert("CI_EVENT", campaign.Event)
			}
		}

		if campaign.Acl != nil {

			campaign.Acl.UserId = *campaign.CreatedUser
			if curAclByUserId.Has(campaign.Acl.UserId) == true {

				sql.Update("CI_CONTACTS", campaign.Acl)
			} else {

				sql.Insert("CI_CONTACTS", campaign.Acl)
			}
		}

		if campaign.Features != nil {

			campaign.Features.UserId = *campaign.CreatedUser
			if curFeaturesByUserId.Has(campaign.Features.UserId) == true {

				sql.Update("CI_CONTACTS", campaign.Features)
			} else {

				sql.Insert("CI_CONTACTS", campaign.Features)
			}
		}
	}

	return state.Campaign, nil
}
