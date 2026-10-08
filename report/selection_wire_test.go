package report

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	"github.com/viant/datly/typecatalog"
)

func TestDynamicCubeSelectionNamesIgnoreReaderSerialization(t *testing.T) {
	source := reportSource(t, &spec.ReportSettings{Enabled: true})
	source.Component.RootView.Relations = nil
	source.Component.RootView.Columns = []*spec.Column{
		{Name: "EventDate", Source: "event_date", Groupable: boolPointer(true), Tag: `json:"EventDate,omitempty"`},
		{Name: "TotalSpend", Source: "total_spend", Tag: `json:"reader_total"`},
		{Name: "Hidden", Source: "hidden", Groupable: boolPointer(true), Tag: `json:"-"`},
	}
	project, err := NewProjectCompiler(ProjectConfig{Types: typecatalog.NewCatalog()}).Compile([]Source{source})
	if err != nil {
		t.Fatal(err)
	}
	cube := project.Derived()[0]
	for _, test := range []struct{ section, field, wire string }{{"Dimensions", "EventDate", "eventDate"}, {"Measures", "TotalSpend", "totalSpend"}} {
		section, _ := cube.InputType.FieldByName(test.section)
		field, ok := section.Type.FieldByName(test.field)
		if !ok || strings.Split(field.Tag.Get("json"), ",")[0] != test.wire {
			t.Fatalf("cube discovery used reader serialization for %s: %s", test.field, field.Tag)
		}
	}
	dimensions, _ := cube.InputType.FieldByName("Dimensions")
	if _, found := dimensions.Type.FieldByName("Hidden"); found {
		t.Fatal("json:- field exposed as cube selection")
	}
	input := reflect.New(cube.InputType)
	if err := json.Unmarshal([]byte(`{"dimensions":{"eventDate":true},"measures":{"totalSpend":true}}`), input.Interface()); err != nil {
		t.Fatal(err)
	}
	fields, err := cube.Plan.selectedFields(input.Elem())
	if err != nil || !reflect.DeepEqual(fields, []string{"EventDate", "TotalSpend"}) {
		t.Fatalf("lower-camel selections did not bind: %v %v", fields, err)
	}
}

func TestCubeSelectionNamesNormalizeSQLAndLinkedFieldIdentities(t *testing.T) {
	for _, name := range []string{"campaign_id", "CampaignId"} {
		t.Run(name, func(t *testing.T) {
			source := reportSource(t, &spec.ReportSettings{Enabled: true})
			source.Component.RootView.Relations = nil
			source.Component.RootView.Columns = []*spec.Column{{Name: name, Source: "campaign_id", Groupable: boolPointer(true), Tag: `json:"ReaderCampaign"`}}
			contract, _ := source.Input.ForRoute(spec.RouteRef{Method: "GET", Path: "/spend"})
			metadata, err := compileMetadata(source.Component, contract)
			if err != nil {
				t.Fatal(err)
			}
			if metadata.dimensions[0].sqlName != "campaign_id" || metadata.dimensions[0].wireName != "campaignId" {
				t.Fatalf("SQL and cube identities mixed: %+v", metadata.dimensions[0])
			}
			project, err := NewProjectCompiler(ProjectConfig{Types: typecatalog.NewCatalog()}).Compile([]Source{source})
			if err != nil {
				t.Fatal(err)
			}
			cube := project.Derived()[0]
			input := reflect.New(cube.InputType)
			if err := json.Unmarshal([]byte(`{"dimensions":{"campaignId":true}}`), input.Interface()); err != nil {
				t.Fatal(err)
			}
			if !input.Elem().FieldByName("Dimensions").FieldByName("CampaignId").Bool() {
				t.Fatal("normalized selection did not bind")
			}
		})
	}
}
