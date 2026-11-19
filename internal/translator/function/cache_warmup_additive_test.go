package function

import (
	"testing"

	"github.com/viant/datly/view"
)

func TestCacheWarmupApplyIsAdditive(t *testing.T) {
	aView := &view.View{Cache: view.NewRefCache("aerospike")}
	subject := &cacheWarmup{}

	if err := subject.Apply([]string{"advertiser_id", "IndexParameter=AdvertiserIds", "Period=today,yesterday"}, nil, &view.Resource{}, aView); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := subject.Apply([]string{"campaign_id", "IndexParameter=CampaignIds", "Name=campaign", "Priority=2", "Period=today"}, nil, &view.Resource{}, aView); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := subject.Apply([]string{"order_id", "IndexParameter=OrderIds", "CaseRefs=recent,long_range"}, nil, &view.Resource{}, aView); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	//the first declaration keeps populating the singular warmup unchanged
	if aView.Cache.Warmup == nil || aView.Cache.Warmup.IndexColumn != "advertiser_id" {
		t.Fatalf("expected singular warmup for advertiser_id, got %#v", aView.Cache.Warmup)
	}
	if len(aView.Cache.Warmup.Cases) != 1 || len(aView.Cache.Warmup.Cases[0].Set) != 1 || len(aView.Cache.Warmup.Cases[0].Set[0].Values) != 2 {
		t.Fatalf("unexpected singular warmup cases: %#v", aView.Cache.Warmup.Cases)
	}

	//later declarations append to the plural collection without overwriting
	if len(aView.Cache.Warmups) != 2 {
		t.Fatalf("expected 2 plural warmups, got %v", len(aView.Cache.Warmups))
	}
	campaign := aView.Cache.Warmups[0]
	if campaign.IndexColumn != "campaign_id" || campaign.IndexParameter != "CampaignIds" {
		t.Fatalf("unexpected campaign warmup: %#v", campaign)
	}
	if campaign.Name != "campaign" || campaign.Priority != 2 {
		t.Fatalf("unexpected campaign name/priority: %#v", campaign)
	}
	if len(campaign.Cases) != 1 {
		t.Fatalf("unexpected campaign cases: %#v", campaign.Cases)
	}
	order := aView.Cache.Warmups[1]
	if order.IndexColumn != "order_id" {
		t.Fatalf("unexpected order warmup: %#v", order)
	}
	if len(order.CaseRefs) != 2 || order.CaseRefs[0] != "recent" || order.CaseRefs[1] != "long_range" {
		t.Fatalf("unexpected order caseRefs: %#v", order.CaseRefs)
	}

	//canonical accessor keeps singular first, then plural in declaration order
	effective := aView.Cache.EffectiveWarmups()
	if len(effective) != 3 || effective[0] != aView.Cache.Warmup || effective[1] != campaign || effective[2] != order {
		t.Fatalf("unexpected effective warmups order: %#v", effective)
	}
}

func TestCacheWarmupApplyRejectsInvalidPriority(t *testing.T) {
	aView := &view.View{Cache: view.NewRefCache("aerospike")}
	if err := (&cacheWarmup{}).Apply([]string{"order_id", "Priority=high"}, nil, &view.Resource{}, aView); err == nil {
		t.Fatalf("expected error")
	}
}

func TestCacheWarmupApplyRejectsEmptyName(t *testing.T) {
	aView := &view.View{Cache: view.NewRefCache("aerospike")}
	if err := (&cacheWarmup{}).Apply([]string{"order_id", "Name="}, nil, &view.Resource{}, aView); err == nil {
		t.Fatalf("expected error")
	}
}
