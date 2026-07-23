package shape_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCompareMetadataParity(t *testing.T) {
	trueValue := true
	falseValue := false

	legacyMeta := &resourceMetaIR{ColumnsDiscovery: &trueValue}
	shapeMeta := &resourceMetaIR{ColumnsDiscovery: &trueValue}

	legacyViews := []viewMetaIR{
		{
			Name:              "items",
			Mode:              "SQLQuery",
			Module:            "platform/items",
			AllowNulls:        &trueValue,
			SelectorNamespace: "item",
			SelectorNoLimit:   &falseValue,
			SchemaCardinality: "Many",
			SchemaType:        "*ItemView",
			HasSummary:        &trueValue,
		},
	}
	shapeViews := []viewMetaIR{
		{
			Name:              "items",
			Mode:              "SQLQuery",
			Module:            "platform/items",
			AllowNulls:        &trueValue,
			SelectorNamespace: "item",
			SelectorNoLimit:   &falseValue,
			SchemaCardinality: "Many",
			SchemaType:        "*ItemView",
			HasSummary:        &trueValue,
		},
	}

	assert.Empty(t, compareMetadataParity(legacyMeta, shapeMeta, legacyViews, shapeViews))
}

func TestCompareMetadataParity_DetectsMismatches(t *testing.T) {
	trueValue := true
	falseValue := false

	legacyMeta := &resourceMetaIR{ColumnsDiscovery: &trueValue}
	shapeMeta := &resourceMetaIR{ColumnsDiscovery: &falseValue}

	legacyViews := []viewMetaIR{{
		Name:            "items",
		Mode:            "SQLQuery",
		Module:          "platform/items",
		AllowNulls:      &trueValue,
		SelectorNoLimit: &trueValue,
		SchemaType:      "*ItemView",
	}}
	shapeViews := []viewMetaIR{{
		Name:            "items",
		Mode:            "SQLExec",
		Module:          "platform/items2",
		AllowNulls:      &falseValue,
		SelectorNoLimit: &falseValue,
		SchemaType:      "*OtherView",
	}}

	mismatches := compareMetadataParity(legacyMeta, shapeMeta, legacyViews, shapeViews)
	assert.Contains(t, mismatches, "resource columnsDiscovery mismatch")
	assert.Contains(t, mismatches, "view mode mismatch: items")
	assert.Contains(t, mismatches, "view module mismatch: items")
	assert.Contains(t, mismatches, "view allowNulls mismatch: items")
	assert.Contains(t, mismatches, "view selector noLimit mismatch: items")
	assert.Contains(t, mismatches, "view schema type mismatch: items")
}
