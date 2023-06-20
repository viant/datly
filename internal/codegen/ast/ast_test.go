package ast

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestBlock_Stringify(t *testing.T) {

	var testCases = []struct {
		description string
		block       Block
		options     Options
		indent      string
		expect      string
	}{
		{
			description: "assign",
			options:     Options{Lang: "dsql"},
			indent:      "  ",
			block: Block{
				&Assign{Holder: &Ident{"inited"}, Expression: &CallExpr{Holder: Ident{Name: "Campaign"}, Name: "Init", Args: []Expression{
					Ident{Name: "CurCampaign"},
				}}},
			},
			expect: `#set($inited = $Campaign.Init($CurCampaign))`,
		},
		{
			description: "for each ",
			options:     Options{Lang: "dsql"},
			indent:      "  ",
			block: Block{
				&Foreach{Set: &Ident{"Sets"},
					Value: &Ident{"Item"},
					Body: Block{
						&Assign{Holder: &Ident{"tested"}, Expression: &CallExpr{Holder: Ident{Name: "Campaign"}, Name: "Test", Args: []Expression{
							Ident{Name: "Item"},
						}}}}}},
			expect: `#foreach($Item in $Sets)
  #set($tested = $Campaign.Test($Item))
#end`,
		},

		{
			description: "if condition",
			options:     Options{Lang: "dsql"},
			indent:      "  ",
			block: Block{
				&Condition{
					If: &BinaryExpr{X: &Ident{"Campaign.Id"}, Op: ">", Y: &LiteralExpr{Literal: "1"}},
					IFBlock: Block{
						&Assign{Holder: &Ident{"inited"}, Expression: &CallExpr{Holder: Ident{Name: "Campaign"}, Name: "Init", Args: []Expression{
							Ident{Name: "CurCampaign"},
						}}},
					},
					ElseIfBlocks: []*ConditionalBlock{{
						If: &BinaryExpr{X: &Ident{"Campaign.Name"}, Op: "==", Y: &LiteralExpr{Literal: `"Foo"`}},
						Block: Block{
							&Assign{Holder: &Ident{"fooed"}, Expression: &CallExpr{Holder: Ident{Name: "Campaign"}, Name: "Foo", Args: []Expression{
								Ident{Name: "CurCampaign"},
							}}},
						},
					},
					},
				},
			},
			expect: `#if($Campaign.Id > 1)
  #set($inited = $Campaign.Init($CurCampaign))
#elseif($Campaign.Name == "Foo")
  #set($fooed = $Campaign.Foo($CurCampaign))
#end`,
		},
	}

	for _, testCase := range testCases {
		builder := NewBuilder(testCase.options)
		err := testCase.block.Generate(builder)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}
		actual := builder.String()
		assert.EqualValues(t, testCase.expect, strings.TrimSpace(actual))
	}

	/*
		#set($inited = $Campaign.Init($CurCampaign, $Acl, $Features))

		#set($info = $Campaign.Validate($CurCampaign, $CurAdvertiser, $NewCreatives, $Acl, $Features, $session))
		#if($info.HasError ==  true)
		$response.StatusCode(401)
		$response.Failf("%v",$info.Error)
		#end




		$sequencer.Allocate("CI_CAMPAIGN", $Campaign, "Id")
		$sequencer.Allocate("CI_CAMPAIGN_FLIGHT", $Campaign, "Flights/Id")
		$sequencer.Allocate("CI_CAMPAIGN_CREATIVE", $Campaign, "CampaignCreative/Id")
		$sequencer.Allocate("CI_CAMPAIGN_BID_MULTIPLIER", $Campaign, "BidOverride/Id")



		#set($capModified = $Campaign.HasCapChange($CurCampaign, $Features))
		#if($capModified == true)
		#set($msg = $messageBus.Message("aws/topic/us-west-1/datly-e2e-campaign", $Campaign.Id))
		#set($confirmation = $messageBus.Push($msg))
		$logger.Printf("confirmation:%v", $confirmation.MessageID)
		#end

		#set($FlightsById = $CurFlights.IndexBy("Id"))
		#set($CampaignCreativeById = $CurCampaignCreative.IndexBy("Id"))
		#set($BidOverrideById = $CurBidOverride.IndexBy("Id"))


		#if($Campaign.Has.Id == true)
		$sql.Update($Campaign, "CI_CAMPAIGN");
		#else
		$sql.Insert($Campaign, "CI_CAMPAIGN");
		#end

		#foreach($recFlight in $Unsafe.Campaign.Flights)
		#if(($FlightsById.HasKey($recFlight.Id) == true))
		$sql.Update($recFlight, "CI_CAMPAIGN_FLIGHT");
		#else
	*/

}
