package load

import (
	"context"
	"fmt"

	"github.com/viant/datly/repository/shape"
	dqlplan "github.com/viant/datly/repository/shape/dql/plan"
	shapeplan "github.com/viant/datly/repository/shape/plan"
	"github.com/viant/datly/repository/shape/scan"
)

// Artifact carries canonical representation for parity checks.
type Artifact struct {
	Canonical map[string]any
}

func FromPlan(result *dqlplan.Result) *Artifact {
	if result == nil {
		return nil
	}
	return &Artifact{Canonical: result.Canonical}
}

// FromHolderStruct builds a canonical shape artifact directly from a tagged holder struct.
func FromHolderStruct(ctx context.Context, holder any) (*Artifact, error) {
	if holder == nil {
		return nil, fmt.Errorf("dql load: holder was nil")
	}
	scanned, err := scan.New().Scan(ctx, &shape.Source{Struct: holder})
	if err != nil {
		return nil, err
	}
	planned, err := shapeplan.New().Plan(ctx, scanned)
	if err != nil {
		return nil, err
	}
	shapeResult, ok := shapeplan.ResultFrom(planned)
	if !ok {
		return nil, fmt.Errorf("dql load: unsupported shape plan kind %q", planned.Plan.ShapeSpecKind())
	}
	views := make([]any, 0, len(shapeResult.Views))
	for _, item := range shapeResult.Views {
		if item == nil {
			continue
		}
		entry := map[string]any{
			"Name":         item.Name,
			"Table":        item.Table,
			"ConnectorRef": item.Connector,
			"Holder":       item.Holder,
			"Cardinality":  item.Cardinality,
		}
		if item.Partitioner != "" {
			entry["Partitioner"] = item.Partitioner
		}
		if item.PartitionedConcurrency > 0 {
			entry["PartitionedConcurrency"] = item.PartitionedConcurrency
		}
		if item.RelationalConcurrency > 0 {
			entry["RelationalConcurrency"] = item.RelationalConcurrency
		}
		if item.Ref != "" {
			entry["Ref"] = item.Ref
		}
		if item.SQLURI != "" {
			entry["SourceURL"] = item.SQLURI
		}
		if item.SQL != "" {
			entry["SQL"] = item.SQL
		}
		if len(item.Links) > 0 {
			entry["Links"] = append([]string(nil), item.Links...)
		}
		views = append(views, entry)
	}
	return &Artifact{
		Canonical: map[string]any{
			"Resource": map[string]any{
				"Views": views,
			},
		},
	}, nil
}
