package report

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/output"
	"github.com/viant/datly/spec"
)

type civilReportRow struct {
	Day    *time.Time `json:"day" sqlx:"day" format:"dateFormat=YYYY-MM-DD" groupable:"true"`
	Raw    *time.Time `json:"raw" sqlx:"raw" groupable:"true"`
	Amount *float64   `json:"amount" sqlx:"amount" groupable:"false"`
}
type civilReportOutput struct {
	Data []*civilReportRow `parameter:",kind=output,in=view" json:"data"`
}

func TestDerivedCubePreservesSourceCivilDatePresentation(t *testing.T) {
	on := true
	component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Scope: "example.com/readings", Name: "Readings"}, Settings: &spec.Settings{CaseFormat: "lc", Report: &spec.ReportSettings{Enabled: true}}, Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}}, Routes: []*spec.Route{{Method: "GET", Path: "/readings"}}, RootView: &spec.View{Name: "readings", Groupable: &on, AllowNulls: &on, Selector: &spec.Selector{AllowFields: true}, Source: &spec.ViewSource{SQL: "SELECT day, raw, SUM(amount) AS amount FROM readings GROUP BY day, raw"}}}
	compilation, e := NewProjectCompiler(ProjectConfig{}).CompileArtifacts([]bootstrap.ArtifactInput{{Component: component, InputType: reflect.TypeFor[struct{}](), OutputType: reflect.TypeFor[civilReportOutput](), DirectViewField: "Data"}})
	if e != nil {
		t.Fatal(e)
	}
	sourceCount, cubeCount := 0, 0
	for _, artifact := range compilation.Artifacts() {
		if artifact.IsReport() {
			cubeCount++
		} else {
			sourceCount++
		}

		plan, e := (output.Compiler{}).Compile(output.CompileInput{Type: artifact.OutputType(), Component: artifact.Component(), DataField: "Data"})
		if e != nil {
			t.Fatal(e)
		}
		document, e := plan.JSONSchema()
		if e != nil {
			t.Fatal(e)
		}
		rawSchema, _ := json.Marshal(document)
		var schema struct {
			Properties map[string]struct {
				Items struct {
					Properties map[string]struct {
						Format string `json:"format"`
					} `json:"properties"`
				} `json:"items"`
			} `json:"properties"`
		}
		if e = json.Unmarshal(rawSchema, &schema); e != nil {
			t.Fatal(e)
		}
		if schema.Properties["data"].Items.Properties["day"].Format != "date" || schema.Properties["data"].Items.Properties["raw"].Format != "date-time" {
			t.Errorf("report=%v lost source date schema: %s", artifact.IsReport(), rawSchema)
		}
		// Native formatting must not shift a civil source value into another UTC
		// day. Raw time serialization remains RFC3339 with its original offset.
		day := time.Date(2026, 9, 1, 23, 30, 0, 0, time.FixedZone("source", -7*3600))
		encoded, e := plan.Encode(context.Background(), "json", &civilReportOutput{Data: []*civilReportRow{{Day: &day, Raw: &day}, {}}})
		if e != nil {
			t.Fatal(e)
		}
		var wire struct {
			Data []struct {
				Day *string `json:"day"`
				Raw *string `json:"raw"`
			} `json:"data"`
		}
		if e = json.Unmarshal(encoded.Data, &wire); e != nil {
			t.Fatal(e)
		}
		if len(wire.Data) != 2 || wire.Data[0].Day == nil || *wire.Data[0].Day != "2026-09-01" || wire.Data[0].Raw == nil || *wire.Data[0].Raw != "2026-09-01T23:30:00-07:00" || wire.Data[1].Day != nil {
			t.Errorf("report=%v changed civil/raw/null wire: %s", artifact.IsReport(), encoded.Data)
		}
	}
	if sourceCount != 1 || cubeCount != 1 {
		t.Fatalf("expected one source and one derived cube artifact, got source=%d cube=%d", sourceCount, cubeCount)
	}
}
