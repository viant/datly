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
		`SELECT ARRAY_AGG(#if($enabled)Id#else Name#end) AS IDs FROM /`,
		`SELECT ARRAY_AGG(1) AS IDs FROM /`,
		`SELECT ARRAY_AGG(Id + 1) AS IDs FROM /`,
		`SELECT ARRAY_AGG($Id) AS IDs FROM /`,
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

func TestAnalyzeQuotedChildPath(t *testing.T) {
	for _, path := range []string{"/", "/Items", "/Items/Details"} {
		actual, err := AnalyzeDeclarationSQL("SELECT ChildKey AS ID FROM `" + path + "`")
		if err != nil {
			t.Fatal(err)
		}
		want := path[1:]
		if actual.NestedChildField() != want {
			t.Fatalf("path %s resolved as %q", path, actual.NestedChildField())
		}
	}
}

func TestAnalyze_TemplateOperands(t *testing.T) {
	template := `#foreach($id in $CurEventsId.IDs)$id#if($foreach.HasNext),#end#end`
	for _, sql := range []string{
		`SELECT ID, NAME FROM EVENTS WHERE ID IN (` + template + `)`,
		`SELECT ID, NAME FROM EVENTS WHERE ID IN (0, ` + template + `, 2) ORDER BY ID`,
		`SELECT ID, NAME FROM EVENTS WHERE ID IN (#if($enabled)$Event.ID#else 0#end)`,
		`WITH current AS (SELECT ID, NAME FROM EVENTS WHERE ID IN (` + template + `)) SELECT ID, NAME FROM current`,
		`SELECT e.ID, e.NAME FROM (SELECT ID, NAME FROM EVENTS WHERE ID IN (` + template + `)) e`,
		"SELECT ID, NAME FROM EVENTS -- opaque authored comment\nWHERE ID IN (" + template + ")",
	} {
		t.Run(sql, func(t *testing.T) {
			actual, err := AnalyzeDeclarationSQL(sql)
			if err != nil {
				t.Fatal(err)
			}
			if actual.SQL != sql {
				t.Fatalf("source changed: %q", actual.SQL)
			}
			if !actual.ProjectionComplete || len(actual.Projection) != 2 || actual.Projection[0].Source != "ID" {
				t.Fatalf("projection lost: %+v", actual)
			}
		})
	}
}

func TestAnalyze_TemplatePreservesHintsAndHelperRules(t *testing.T) {
	actual, err := AnalyzeDeclarationSQL(`!!403 {"DataType":"boolean"} SELECT Authorized FROM acl WHERE ID IN (#foreach($id in $IDs)$id#end)`)
	if err != nil {
		t.Fatal(err)
	}
	if actual.DataType != "bool" || actual.Table != "acl" {
		t.Fatalf("hints lost: %+v", actual)
	}
	actual, err = AnalyzeDeclarationSQL(`SELECT #if($flag)Id#else Name#end AS Value FROM /`)
	if err != nil {
		t.Fatal(err)
	}
	if actual.ProjectionComplete || len(actual.Projection) != 0 {
		t.Fatalf("template is not a proven helper field: %+v", actual)
	}
	actual, err = AnalyzeDeclarationSQL(`SELECT ARRAY_AGG(Id) AS IDs FROM / WHERE Id IN (#foreach($id in $IDs)$id#end)`)
	if err != nil {
		t.Fatal(err)
	}
	if !actual.ProjectionComplete || len(actual.Projection) != 1 || !actual.Projection[0].Aggregate {
		t.Fatalf("helper lost: %+v", actual)
	}
}

func TestAnalyze_TemplateRejectsMalformedSyntax(t *testing.T) {
	for _, sql := range []string{
		`SELECT ID FROM EVENTS WHERE ID IN (#foreach($id in $IDs)$id)`,
		`SELECT ID FROM EVENTS WHERE ID IN (#foreach($id $IDs)$id#end)`,
		`SELECT ID FROM EVENTS WHERE ID IN (#foreach($id in $IDs)#if()$id#end#end)`,
		`SELECT ID FROM EVENTS WHERE ID IN (#unknown($IDs))`,
		`SELECT ID FROM EVENTS WHERE ID IN (#end)`,
		`SELECT ID FROM EVENTS WHERE ID IN (#if($ok)$id#end,)`,
		`SELECT ID FROM EVENTS WHERE ID IN (#if($ok)$id#end 2)`,
		`SELECT ID FROM EVENTS WHERE ID IN (#if($ok)$id#end) AND ID IN (1 +)`,
		`SELECT ID FROM EVENTS WHERE ID IN (#if($ok)$id#end`,
		`SELECT f(1 +) FROM EVENTS`,
		`SELECT ID FROM EVENTS WHERE ID IN (1,)`,
	} {
		t.Run(sql, func(t *testing.T) {
			if _, err := AnalyzeDeclarationSQL(sql); err == nil {
				t.Fatal("expected syntax error")
			}
		})
	}
}

func TestAnalyze_QuotedTemplateText(t *testing.T) {
	sql := `SELECT ID FROM EVENTS WHERE NAME IN ('#foreach(invalid)', '#end') /* #if(invalid) */`
	actual, err := AnalyzeDeclarationSQL(sql)
	if err != nil {
		t.Fatal(err)
	}
	if actual.SQL != sql {
		t.Fatal("quoted template text changed")
	}
}
