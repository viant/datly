package readerbuilder

import (
	"context"
	"strings"
	"testing"
)

func TestSetColumnContractPreservesUnrelatedMetadata(t *testing.T) {
	service := New(Config{})
	source := `#package('example.com/readers')
#setting($_ = $route('/records','GET'))
#define($_ = $Records<[]*Record>(output/view))
SELECT records.*, type(records,'Record'), tag(records.name,'groupable:"true" json:"legacy"'), CAST(records.name AS string)
FROM (SELECT name FROM records) records`
	cast := "float64"
	response := service.Apply(context.Background(), Request{DQL: source, Operation: Operation{
		Type: OperationSetColumnContract,
		Column: &ColumnContractMutation{
			View: "records", Column: "name", CastType: &cast,
			Tags: map[string]string{"internal": "true", "json": "displayName"}, RemoveTags: []string{"groupable"},
		},
	}})
	if !response.Applied {
		t.Fatalf("set column contract: %+v", response.Diagnostics)
	}
	for _, want := range []string{`cast(records.name AS float64)`, `internal:"true"`, `json:"displayName"`} {
		if !strings.Contains(response.DQL, want) {
			t.Fatalf("column contract missing %q:\n%s", want, response.DQL)
		}
	}
	if strings.Contains(response.DQL, `groupable:`) || strings.Contains(response.DQL, `json:"legacy"`) {
		t.Fatalf("column contract retained replaced metadata:\n%s", response.DQL)
	}
}

func TestSetColumnContractCanRemoveCastAndTags(t *testing.T) {
	service := New(Config{})
	source := `#package('example.com/readers')
#setting($_ = $route('/records','GET'))
#define($_ = $Records<[]*Record>(output/view))
SELECT records.*, type(records,'Record'), tag(records.name,'internal:"true"'), CAST(records.name AS string)
FROM (SELECT name FROM records) records`
	empty := ""
	response := service.Apply(context.Background(), Request{DQL: source, Operation: Operation{
		Type:   OperationSetColumnContract,
		Column: &ColumnContractMutation{View: "records", Column: "name", CastType: &empty, RemoveTags: []string{"internal"}},
	}})
	if !response.Applied {
		t.Fatalf("remove column contract: %+v", response.Diagnostics)
	}
	if strings.Contains(strings.ToLower(response.DQL), "cast(records.name") || strings.Contains(strings.ToLower(response.DQL), "tag(records.name") {
		t.Fatalf("column contract functions were not removed:\n%s", response.DQL)
	}
}

func TestInspectNormalizesColumnContracts(t *testing.T) {
	service := New(Config{})
	source := `#package('example.com/readers')
#setting($_ = $route('/records','GET'))
#define($_ = $Records<[]*Record>(output/view))
SELECT records.*, type(records,'Record'), tag(records.name,'internal:"true" format:"name=DisplayName"'), CAST(records.name AS string)
FROM (SELECT name FROM records) records`
	response := service.Apply(context.Background(), Request{DQL: source, Operation: Operation{Type: OperationInspect}})
	if !response.Applied || response.Structure == nil || len(response.Structure.ColumnContracts) != 1 {
		t.Fatalf("column contracts=%+v diagnostics=%+v", response.Structure, response.Diagnostics)
	}
	contract := response.Structure.ColumnContracts[0]
	if contract.View != "records" || contract.Column != "name" || contract.CastType != "string" || contract.Tags["internal"] != "true" || contract.Tags["format"] != "name=DisplayName" {
		t.Fatalf("column contract=%+v", contract)
	}
}
