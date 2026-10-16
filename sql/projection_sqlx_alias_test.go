package sql

import (
	"errors"
	"testing"

	"github.com/viant/datly/data"
)

func projectionForSQLXAliases(source string, fields ...data.SelectorField) SelectorProjection {
	return SelectorProjection{SQL: source, View: &data.View{SelectorFields: fields}}
}

func requireProjectionSelection(t *testing.T, projection SelectorProjection, selector, wantOutput string) {
	t.Helper()
	columns, err := projection.Columns([]string{selector})
	if err != nil {
		t.Fatalf("Columns(%q): %v", selector, err)
	}
	if len(columns) != 1 || columns[0].OutputName() != wantOutput {
		t.Fatalf("Columns(%q) outputs = %#v, want only %q", selector, columns, wantOutput)
	}
}

func requireProjectionSelectionNotFound(t *testing.T, projection SelectorProjection, selector string) {
	t.Helper()
	_, err := projection.Columns([]string{selector})
	var missing *UnknownProjectionColumnError
	if !errors.As(err, &missing) {
		t.Fatalf("Columns(%q) error = %T (%v), want UnknownProjectionColumnError", selector, err, err)
	}
}

func TestSelectorProjectionKeepsPrimaryAndResolvesOnlyUniqueVerifiedSecondary(t *testing.T) {
	fields := []data.SelectorField{
		{
			GoName: "Primary", PublicName: "primary", Column: "physical_id",
			ColumnAliases:         []string{"shared_result", "primary_fallback"},
			VerifiedColumnAliases: []string{"shared_result", "primary_fallback"},
		},
		{
			GoName: "Other", PublicName: "other", Column: "other_id",
			ColumnAliases: []string{"shared_result", "other_fallback"},
			// The native SQLX matcher assigns a duplicate alias to the first
			// matching field. Full aliases still retain the second claim below.
			VerifiedColumnAliases: []string{"other_fallback"},
		},
	}

	t.Run("primary remains authoritative when secondary is also projected", func(t *testing.T) {
		projection := projectionForSQLXAliases(
			"SELECT physical_id, physical_id AS primary_fallback, other_id AS shared_result FROM records",
			fields...,
		)
		requireProjectionSelection(t, projection, "primary", "physical_id")
		// The shared secondary is claimed by two visible fields, so neither
		// field may acquire it through fallback.
		requireProjectionSelectionNotFound(t, projection, "other")
	})

	t.Run("unique secondary resolves when the shared alternative is absent", func(t *testing.T) {
		projection := projectionForSQLXAliases(
			"SELECT physical_id AS primary_fallback, other_id FROM records",
			fields...,
		)
		requireProjectionSelection(t, projection, "primary", "primary_fallback")
	})

	t.Run("shared secondary plus a unique alternative withholds fallback", func(t *testing.T) {
		projection := projectionForSQLXAliases(
			"SELECT physical_id AS shared_result, physical_id AS primary_fallback, other_id FROM records",
			fields...,
		)
		// Full alias targets are considered before matcher verification. Even
		// though primary_fallback has one destination, shared_result is claimed
		// by two fields and both outputs are projected, so fallback is withheld.
		requireProjectionSelectionNotFound(t, projection, "primary")
	})
}

func TestSelectorProjectionWithholdsFallbackForTwoDistinctSecondariesAndWrongOutput(t *testing.T) {
	t.Run("two distinct secondary targets", func(t *testing.T) {
		projection := projectionForSQLXAliases(
			"SELECT first_value AS secondary_one, second_value AS secondary_two FROM records",
			data.SelectorField{
				GoName: "Value", PublicName: "value", Column: "physical_value",
				ColumnAliases:         []string{"secondary_one", "secondary_two"},
				VerifiedColumnAliases: []string{"secondary_one", "secondary_two"},
			},
		)
		requireProjectionSelectionNotFound(t, projection, "value")
	})

	t.Run("tagged secondary differs from actual SQL output spelling", func(t *testing.T) {
		projection := projectionForSQLXAliases(
			"SELECT source_value AS actual_output FROM records",
			data.SelectorField{
				GoName: "Value", PublicName: "value", Column: "physical_value",
				ColumnAliases:         []string{"tagged_result"},
				VerifiedColumnAliases: []string{"tagged_result"},
			},
		)
		requireProjectionSelectionNotFound(t, projection, "value")
	})
}

func TestSelectorProjectionApplyMappingsWithholdsUnverifiedSecondaryTargets(t *testing.T) {
	t.Run("ambiguous original alias plus unique alternative", func(t *testing.T) {
		projection := SelectorProjection{View: &data.View{SelectorFields: []data.SelectorField{
			{
				GoName: "Primary", PublicName: "primary", Column: "physical_id",
				ColumnAliases:         []string{"shared_result", "unique_result"},
				VerifiedColumnAliases: []string{"shared_result", "unique_result"},
			},
			{
				GoName: "Other", PublicName: "other", Column: "other_id",
				ColumnAliases:         []string{"shared_result"},
				VerifiedColumnAliases: []string{"shared_result"},
			},
		}}}
		columns := []ProjectionColumn{
			{names: ProjectionNames{"output_a", "shared_result"}, output: "output_a"},
			{names: ProjectionNames{"shared_result", "unique_result"}, output: "unique_result"},
		}
		projection.applyMappings(columns)
		if columns[0].Matches("primary") || columns[1].Matches("primary") {
			t.Fatalf("ambiguous shared target was admitted through unique alternative: %#v", columns)
		}
		if !columns[1].Matches("unique_result") {
			t.Fatalf("unique raw output name was not retained: %#v", columns[1])
		}
	})

	t.Run("two distinct secondary targets", func(t *testing.T) {
		projection := SelectorProjection{View: &data.View{SelectorFields: []data.SelectorField{{
			GoName: "Value", PublicName: "value", Column: "physical_value",
			ColumnAliases:         []string{"secondary_one", "secondary_two"},
			VerifiedColumnAliases: []string{"secondary_one", "secondary_two"},
		}}}}
		columns := []ProjectionColumn{
			{names: ProjectionNames{"output_one", "secondary_one"}, output: "output_one"},
			{names: ProjectionNames{"output_two", "secondary_two"}, output: "output_two"},
		}
		projection.applyMappings(columns)
		for _, column := range columns {
			if column.Matches("value") {
				t.Fatalf("field was admitted through two projected secondary targets: %#v", columns)
			}
		}
	})

	t.Run("source alias is not actual output spelling", func(t *testing.T) {
		projection := SelectorProjection{View: &data.View{SelectorFields: []data.SelectorField{{
			GoName: "Value", PublicName: "value", Column: "physical_value",
			ColumnAliases:         []string{"source_value"},
			VerifiedColumnAliases: []string{"source_value"},
		}}}}
		columns := []ProjectionColumn{{
			names:  ProjectionNames{"source_value", "actual_output"},
			output: "actual_output",
		}}
		projection.applyMappings(columns)
		if columns[0].Matches("value") {
			t.Fatalf("source alias admitted fallback for a different actual output name: %#v", columns[0])
		}
		if !columns[0].Matches("actual_output") {
			t.Fatalf("actual output spelling was lost: %#v", columns[0])
		}
	})
}
