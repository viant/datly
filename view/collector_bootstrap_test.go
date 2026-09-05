package view

import (
	"reflect"
	"testing"

	"github.com/viant/datly/view/state"
	"github.com/viant/xdatly/handler"
	"github.com/viant/xunsafe"
)

type bootstrapAudience struct {
	ID           int
	SignalValues []*bootstrapSignalValue
}

type bootstrapSignalValue struct {
	AudienceID        int
	FeatureType       string
	FeatureValue      string
	SignalPerformance *bootstrapSignalPerformance
}

type bootstrapSignalPerformance struct {
	FeatureType string
	Value       string
}

func TestCollector_BootstrapFromParentHolder_SeedsNestedCompositeRelation(t *testing.T) {
	parentDest := []*bootstrapAudience{
		{
			ID: 1,
			SignalValues: []*bootstrapSignalValue{
				{AudienceID: 1, FeatureType: "ias.brand.safety", FeatureValue: "4001"},
				{AudienceID: 1, FeatureType: "ias.fraud", FeatureValue: "402"},
			},
		},
	}

	parentView := &View{
		Name:   "audience",
		Schema: state.NewSchema(reflect.TypeOf(&bootstrapAudience{})),
	}
	parentCollector := &Collector{
		destValue:              reflect.ValueOf(&parentDest),
		slice:                  xunsafe.NewSlice(reflect.TypeOf(parentDest)),
		view:                   parentView,
		dataSync:               handler.NewDataSync(),
		valuePosition:          map[string]map[string]map[interface{}][]int{},
		compositeValuePosition: map[string]map[compositeKey][]int{},
		types:                  map[string]*xunsafe.Type{},
		values:                 map[string]*[]interface{}{},
	}

	perfView := &View{
		Name:     "signalPerformance",
		Schema:   state.NewSchema(reflect.TypeOf(&bootstrapSignalPerformance{})),
		Template: &Template{},
	}
	perfRelation := &Relation{
		Name:        "SignalPerformance",
		Cardinality: state.One,
		Holder:      "SignalPerformance",
		On: Links{
			{Field: "FeatureType", Column: "FEATURE_TYPE", xField: xunsafe.FieldByName(reflect.TypeOf(bootstrapSignalValue{}), "FeatureType")},
			{Field: "FeatureValue", Column: "FEATURE_VALUE", xField: xunsafe.FieldByName(reflect.TypeOf(bootstrapSignalValue{}), "FeatureValue")},
		},
		Of: &ReferenceView{
			View: *perfView,
			On: Links{
				{Column: "FeatureType"},
				{Column: "Value"},
			},
		},
		holderField: xunsafe.FieldByName(reflect.TypeOf(bootstrapSignalValue{}), "SignalPerformance"),
	}

	signalView := &View{
		Name:     "signalValues",
		Schema:   state.NewSchema(reflect.TypeOf(&bootstrapSignalValue{})),
		Template: &Template{},
		With:     []*Relation{perfRelation},
	}
	signalDest := make([]*bootstrapSignalValue, 0)
	signalCollector := &Collector{
		parent:                 parentCollector,
		destValue:              reflect.ValueOf(&signalDest),
		appender:               xunsafe.NewSlice(reflect.TypeOf(signalDest)).Appender(xunsafe.AsPointer(&signalDest)),
		slice:                  xunsafe.NewSlice(reflect.TypeOf(signalDest)),
		view:                   signalView,
		relation:               &Relation{Holder: "SignalValues", holderField: xunsafe.FieldByName(reflect.TypeOf(bootstrapAudience{}), "SignalValues")},
		dataSync:               handler.NewDataSync(),
		valuePosition:          map[string]map[string]map[interface{}][]int{},
		compositeValuePosition: map[string]map[compositeKey][]int{},
		types:                  map[string]*xunsafe.Type{},
		values:                 map[string]*[]interface{}{},
	}

	children, err := signalCollector.Relations(nil)
	if err != nil {
		t.Fatalf("relations: %v", err)
	}
	if len(children) != 1 {
		t.Fatalf("expected one nested relation collector, got %d", len(children))
	}
	perfCollector := children[0]

	if !signalCollector.BootstrapFromParentHolder() {
		t.Fatalf("expected bootstrap from parent holder")
	}
	if signalCollector.Len() != 2 {
		t.Fatalf("bootstrap length = %d, want 2", signalCollector.Len())
	}

	_, composite, columns := perfCollector.ParentPlaceholders()
	if len(composite) != 2 {
		t.Fatalf("composite placeholder rows = %d, want 2", len(composite))
	}
	if !reflect.DeepEqual(columns, []string{"FeatureType", "Value"}) {
		t.Fatalf("columns = %v, want %v", columns, []string{"FeatureType", "Value"})
	}

	expected := map[compositeKey]bool{
		buildCompositeKey([]interface{}{"ias.brand.safety", "4001"}): true,
		buildCompositeKey([]interface{}{"ias.fraud", "402"}):         true,
	}
	actual := map[compositeKey]bool{}
	for _, row := range composite {
		actual[buildCompositeKey(row)] = true
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("composite rows = %v, want %v", actual, expected)
	}

	signature := relationCompositeSignature(perfRelation.On)
	indexed := signalCollector.compositeValuePosition[signature]
	if len(indexed) != 2 {
		t.Fatalf("composite index size = %d, want 2", len(indexed))
	}
}
