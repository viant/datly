package xgen

import (
	"fmt"
	"strings"

	"github.com/viant/datly/repository/shape/dql/shape"
	"github.com/viant/datly/repository/shape/typectx"
	"github.com/viant/datly/view"
)

// GenerateFromResource produces Go structs directly from an in-memory view.Resource
// without YAML roundtrip. Uses real columns from DB discovery when available.
func GenerateFromResource(resource *view.Resource, typeCtx *typectx.Context, cfg *Config) (*Result, error) {
	if resource == nil {
		return nil, fmt.Errorf("shape xgen: nil resource")
	}
	doc := resourceToShapeDocument(resource, typeCtx)
	return GenerateFromDQLShape(doc, cfg)
}

// resourceToShapeDocument converts an in-memory view.Resource into a shape.Document
// that xgen can process. This avoids the YAML marshal/unmarshal roundtrip.
func resourceToShapeDocument(resource *view.Resource, typeCtx *typectx.Context) *shape.Document {
	root := map[string]any{}

	// Build Resource.Views from in-memory views
	var views []any
	for _, aView := range resource.Views {
		if aView == nil {
			continue
		}
		viewMap := map[string]any{
			"Name":  aView.Name,
			"Table": aView.Table,
			"Mode":  string(aView.Mode),
		}
		if aView.Module != "" {
			viewMap["Module"] = aView.Module
		}
		// Schema
		if aView.Schema != nil {
			schema := map[string]any{}
			if aView.Schema.Name != "" {
				schema["Name"] = aView.Schema.Name
			}
			if aView.Schema.DataType != "" {
				schema["DataType"] = aView.Schema.DataType
			}
			if aView.Schema.Cardinality != "" {
				schema["Cardinality"] = string(aView.Schema.Cardinality)
			}
			viewMap["Schema"] = schema
		}
		// Columns — this is the key: real columns from DB discovery
		if len(aView.Columns) > 0 {
			var columns []any
			for _, col := range aView.Columns {
				if col == nil {
					continue
				}
				colMap := map[string]any{
					"Name":     col.Name,
					"DataType": col.DataType,
				}
				if col.Tag != "" {
					colMap["Tag"] = col.Tag
				}
				if col.Nullable {
					colMap["Nullable"] = true
				}
				columns = append(columns, colMap)
			}
			viewMap["Columns"] = columns
		}
		views = append(views, viewMap)
	}
	root["Resource"] = map[string]any{"Views": views}
	return &shape.Document{
		Root:        root,
		TypeContext: typeCtx,
	}
}

// columnDataType returns Go type name for a view.Column.
func columnDataType(col *view.Column) string {
	if col.DataType != "" {
		return col.DataType
	}
	rType := col.ColumnType()
	if rType == nil {
		return "string"
	}
	return strings.TrimPrefix(rType.String(), "*")
}
