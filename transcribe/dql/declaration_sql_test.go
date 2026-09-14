package dql

import "testing"

func TestAnalyze_ArrayAggHelper(t *testing.T) {
	actual, err := AnalyzeDeclarationSQL(`? SELECT ARRAY_AGG(Id) AS IDs FROM / LIMIT 1`)
	if err != nil {
		t.Fatalf("unexpected analyze error: %v", err)
	}
	if actual == nil {
		t.Fatalf("expected analysis")
	}
	if len(actual.Projection) != 1 || actual.Projection[0] != (DeclarationProjection{Name: "IDs", Source: "Id", Aggregate: true}) {
		t.Fatalf("unexpected aggregate projection %+v", actual.Projection)
	}
	if !actual.ProjectionComplete {
		t.Fatal("aggregate projection must be complete")
	}
	if actual.NestedChildField() != "" {
		t.Fatalf("expected no nested child, got %q", actual.NestedChildField())
	}
}

func TestAnalyze_UnsupportedHelperProjectionIsIncomplete(t *testing.T) {
	for _, sqlText := range []string{
		`SELECT COUNT(Id) AS Count FROM /`,
		`SELECT ARRAY_AGG(Id) AS IDs, Name FROM /`,
		`SELECT * FROM /`,
	} {
		actual, err := AnalyzeDeclarationSQL(sqlText)
		if err != nil {
			t.Fatalf("AnalyzeDeclarationSQL(%q) error = %v", sqlText, err)
		}
		if actual == nil || actual.ProjectionComplete || len(actual.Projection) != 0 {
			t.Fatalf("AnalyzeDeclarationSQL(%q) projection = %+v complete=%v", sqlText, actual.Projection, actual.ProjectionComplete)
		}
	}
}

func TestAnalyze_RowHelperFromChildPath(t *testing.T) {
	actual, err := AnalyzeDeclarationSQL(`SELECT Price AS CurrentPrice, Timestamp FROM /EventsPerformance`)
	if err != nil {
		t.Fatalf("unexpected analyze error: %v", err)
	}
	if len(actual.Projection) != 2 || actual.Projection[0] != (DeclarationProjection{Name: "CurrentPrice", Source: "Price"}) ||
		actual.Projection[1] != (DeclarationProjection{Name: "Timestamp", Source: "Timestamp"}) {
		t.Fatalf("unexpected row projection %+v", actual.Projection)
	}
	if actual.NestedChildField() != "EventsPerformance" {
		t.Fatalf("expected child path EventsPerformance, got %q", actual.NestedChildField())
	}
}

func TestAnalyze_CriteriaInCurrentState(t *testing.T) {
	actual, err := AnalyzeDeclarationSQL(`SELECT * FROM EVENTS WHERE $criteria.In("ID", $CurEventsId.Values)`)
	if err != nil {
		t.Fatalf("unexpected analyze error: %v", err)
	}
	if actual.DeclarationCriteriaIn == nil {
		t.Fatalf("expected criteria-in binding")
	}
	if actual.DeclarationCriteriaIn.Column != "ID" || actual.DeclarationCriteriaIn.HelperField != "CurEventsId" {
		t.Fatalf("unexpected criteria-in binding %+v", actual.DeclarationCriteriaIn)
	}
	if actual.Table != "EVENTS" {
		t.Fatalf("expected table EVENTS, got %q", actual.Table)
	}
}

func TestAnalyze_ScalarPropertyAndDataTypeHint(t *testing.T) {
	actual, err := AnalyzeDeclarationSQL(`!!403
SELECT Authorized /* {"DataType":"bool"} */
FROM user_acl
WHERE user_id = $Event.UserID`)
	if err != nil {
		t.Fatalf("unexpected analyze error: %v", err)
	}
	if actual.DeclarationScalarProperty == nil {
		t.Fatalf("expected scalar-property binding")
	}
	if actual.DeclarationScalarProperty.Column != "user_id" || actual.DeclarationScalarProperty.SourceField != "Event" || actual.DeclarationScalarProperty.Property != "UserID" {
		t.Fatalf("unexpected scalar-property binding %+v", actual.DeclarationScalarProperty)
	}
	if actual.DataType != "bool" {
		t.Fatalf("expected bool data type, got %q", actual.DataType)
	}
}
