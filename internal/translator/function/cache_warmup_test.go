package function

import (
	"testing"

	"github.com/viant/datly/view"
)

func TestCacheWarmupApply(t *testing.T) {
	aView := &view.View{Cache: view.NewRefCache("aerospike")}
	subject := &cacheWarmup{}

	err := subject.Apply([]string{
		"order_id",
		"Connector=bq_metrics_prewarm",
		"IndexParameter=OrderId",
		"Period=today,yesterday",
		"Granularity=hour,day",
	}, nil, &view.Resource{}, aView)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if aView.Cache.Warmup == nil {
		t.Fatalf("expected warmup to be set")
	}
	if aView.Cache.Warmup.IndexColumn != "order_id" {
		t.Fatalf("unexpected index column: %v", aView.Cache.Warmup.IndexColumn)
	}
	if aView.Cache.Warmup.IndexParameter != "OrderId" {
		t.Fatalf("unexpected index parameter: %v", aView.Cache.Warmup.IndexParameter)
	}
	if len(aView.Cache.Warmup.Cases) != 1 {
		t.Fatalf("unexpected cases count: %v", len(aView.Cache.Warmup.Cases))
	}
	if len(aView.Cache.Warmup.Cases[0].Set) != 2 {
		t.Fatalf("unexpected warmup parameter count: %v", len(aView.Cache.Warmup.Cases[0].Set))
	}
	if aView.Cache.Warmup.Connector == nil || aView.Cache.Warmup.Connector.Ref != "bq_metrics_prewarm" {
		t.Fatalf("unexpected warmup connector: %#v", aView.Cache.Warmup.Connector)
	}
}

func TestCacheWarmupApplyRequiresCache(t *testing.T) {
	err := (&cacheWarmup{}).Apply([]string{"order_id"}, nil, &view.Resource{}, &view.View{})
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestCacheWarmupApplyRejectsEmptyConnector(t *testing.T) {
	aView := &view.View{Cache: view.NewRefCache("aerospike")}
	err := (&cacheWarmup{}).Apply([]string{"order_id", "Connector="}, nil, &view.Resource{}, aView)
	if err == nil {
		t.Fatalf("expected error")
	}
}

func TestCacheWarmupApplyRejectsEmptyIndexParameter(t *testing.T) {
	aView := &view.View{Cache: view.NewRefCache("aerospike")}
	err := (&cacheWarmup{}).Apply([]string{"order_id", "IndexParameter="}, nil, &view.Resource{}, aView)
	if err == nil {
		t.Fatalf("expected error")
	}
}
