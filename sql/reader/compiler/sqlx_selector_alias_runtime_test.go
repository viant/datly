package compiler

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/sql/reader"
	xresponse "github.com/viant/xdatly/response"
)

type sqlxAliasItem struct {
	ID     int     `sqlx:"id" json:"id"`
	ItemID *string `sqlx:"physical_id|result_id|not_projected" json:"itemId"`
	Name   string  `sqlx:"name" json:"name"`
}

type sqlxAliasInput struct {
	Fields []string
}

type sqlxAliasOutput struct {
	Data []*sqlxAliasItem `json:"data"`
}

type sqlxAliasNegativeItem struct {
	ID          int    `sqlx:"id" json:"id"`
	Unprojected string `sqlx:"missing_physical|missing_result" json:"unprojected"`
	Hidden      string `sqlx:"secret" json:"-"`
}

type sqlxAliasNegativeOutput struct {
	Data []*sqlxAliasNegativeItem `json:"data"`
}

type sqlxAliasAmbiguousItem struct {
	ID      int    `sqlx:"id" json:"id"`
	Primary string `sqlx:"physical_id|shared_result" json:"primary"`
	Shadow  string `sqlx:"other_id|shared_result" json:"shadow"`
}

type sqlxAliasAmbiguousOutput struct {
	Data []*sqlxAliasAmbiguousItem `json:"data"`
}

type sqlxAliasHiddenOwnerItem struct {
	ID      int     `sqlx:"id" json:"id"`
	Visible *string `sqlx:"visible_id|result_id" json:"visible"`
	Hidden  *string `sqlx:"hidden_id|result_id" json:"-"`
}

type sqlxAliasHiddenOwnerOutput struct {
	Data []*sqlxAliasHiddenOwnerItem `json:"data"`
}

type sqlxAliasHiddenFirstItem struct {
	ID      int     `sqlx:"id" json:"id"`
	Hidden  *string `sqlx:"hidden_id|result_id" json:"-"`
	Visible *string `sqlx:"visible_id|result_id" json:"visible"`
}

type sqlxAliasHiddenFirstOutput struct {
	Data []*sqlxAliasHiddenFirstItem `json:"data"`
}

type sqlxAliasTransientItem struct {
	ID       int     `sqlx:"id" json:"id"`
	Visible  *string `sqlx:"visible_id|result_id" json:"visible"`
	ResultID string  `sqlx:"-" json:"-"`
}

type sqlxAliasTransientOutput struct {
	Data []*sqlxAliasTransientItem `json:"data"`
}

func newSQLXAliasExecution(t *testing.T, h *sqlite.Harness, rowType, outputType reflect.Type, sql string) *reader.Execution {
	t.Helper()
	component := &spec.Component{
		Name: "Items",
		RootView: &spec.View{
			Name:     "items",
			Source:   &spec.ViewSource{SQL: sql},
			Selector: &spec.Selector{AllowFields: true},
		},
		Parameters: []*spec.Parameter{{
			Name: "Fields", Source: spec.BindSource{Kind: "query", Name: "fields"},
			QuerySelector: &spec.QuerySelectorBinding{View: "items", Property: spec.SelectorPropertyFields},
		}},
	}
	inputType := reflect.TypeOf(sqlxAliasInput{})
	artifact, err := BuildArtifact(ArtifactInput{
		Component: component, InputType: inputType, OutputType: outputType, DirectViewField: "Data",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(artifact.Reader.Root.View.SelectorFields) == 0 {
		t.Fatal("compiled reader omitted row selector fields")
	}
	execution, err := reader.NewExecution(reader.Config{
		Component: component, InputType: inputType,
		OutputType: outputType, Plan: artifact.Reader,
		SQL: &dsql.SQLComponent{DB: h.DB},
	})
	if err != nil {
		t.Fatal(err)
	}
	return execution
}

func requireSelectorResponseError(t *testing.T, err error, contains string) {
	t.Helper()
	if err == nil {
		t.Fatal("expected selector rejection")
	}
	var responseErr *xresponse.Error
	if !errors.As(err, &responseErr) {
		t.Fatalf("selector rejection = %T (%v), want *response.Error", err, err)
	}
	if responseErr.StatusCode() != 400 {
		t.Fatalf("selector status = %d, want 400: %v", responseErr.StatusCode(), err)
	}
	if !strings.Contains(err.Error(), contains) {
		t.Fatalf("selector error %q does not contain %q", err.Error(), contains)
	}
}

func TestSQLXSelectorAcceptsSecondaryProjectedAliasSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx,
		"CREATE TABLE items(id INTEGER PRIMARY KEY, name TEXT)",
		"CREATE TABLE children(item_id INTEGER, physical_id TEXT)",
		"INSERT INTO items(id,name) VALUES(1,'one'),(2,'two')",
		"INSERT INTO children(item_id,physical_id) VALUES(1,'item-1')",
	); err != nil {
		t.Fatal(err)
	}

	for _, selectorName := range []string{"itemId", "ItemID"} {
		t.Run(selectorName, func(t *testing.T) {
			execution := newSQLXAliasExecution(t, h, reflect.TypeOf(sqlxAliasItem{}), reflect.TypeOf(sqlxAliasOutput{}), "SELECT i.id, c.physical_id AS result_id, i.name FROM items i LEFT JOIN children c ON c.item_id = i.id ORDER BY i.id")
			got, err := execution.Read(ctx, &sqlxAliasInput{Fields: []string{"id", selectorName}}, nil, nil)
			if err != nil {
				t.Fatalf("selecting %s backed by result_id failed: %v", selectorName, err)
			}
			data, ok := got.(*sqlxAliasOutput)
			if !ok {
				t.Fatalf("reader returned %T, want *sqlxAliasOutput", got)
			}
			if len(data.Data) != 2 || data.Data[0].ID != 1 || data.Data[0].ItemID == nil || *data.Data[0].ItemID != "item-1" || data.Data[1].ID != 2 || data.Data[1].ItemID != nil {
				var rows []struct {
					ID     int
					ItemID *string
				}
				for _, row := range data.Data {
					if row == nil {
						rows = append(rows, struct {
							ID     int
							ItemID *string
						}{})
						continue
					}
					rows = append(rows, struct {
						ID     int
						ItemID *string
					}{row.ID, row.ItemID})
				}
				t.Fatalf("selected rows (ID, nullable ItemID) = %#v", rows)
			}
		})
	}
}

func TestSQLXSelectorKeepsUniquePrimaryWhenSecondaryAlsoProjectsSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx,
		"CREATE TABLE items(id INTEGER PRIMARY KEY, physical_id TEXT, name TEXT)",
		"CREATE TABLE children(item_id INTEGER, physical_id TEXT)",
		"INSERT INTO items(id,physical_id,name) VALUES(1,'primary-1','one'),(2,'primary-2','two')",
		"INSERT INTO children(item_id,physical_id) VALUES(1,'secondary-1'),(2,'secondary-2')",
	); err != nil {
		t.Fatal(err)
	}
	sql := "SELECT i.id, i.physical_id AS physical_id, c.physical_id AS result_id, i.name FROM items i JOIN children c ON c.item_id = i.id ORDER BY i.id"
	execution := newSQLXAliasExecution(t, h, reflect.TypeOf(sqlxAliasItem{}), reflect.TypeOf(sqlxAliasOutput{}), sql)
	got, err := execution.Read(ctx, &sqlxAliasInput{Fields: []string{"id", "itemId"}}, nil, nil)
	if err != nil {
		t.Fatalf("unique primary mapping should remain authoritative when a secondary alias also projects: %v", err)
	}
	rows := got.(*sqlxAliasOutput).Data
	if len(rows) != 2 || rows[0].ID != 1 || rows[0].ItemID == nil || *rows[0].ItemID != "primary-1" || rows[1].ID != 2 || rows[1].ItemID == nil || *rows[1].ItemID != "primary-2" {
		t.Fatalf("primary-over-secondary rows = %#v", rows)
	}
}

func TestSQLXSelectorRejectsMissingHiddenAndAmbiguousAliasesSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	if err := h.ExecStatements(ctx,
		"CREATE TABLE items(id INTEGER PRIMARY KEY, physical_id TEXT, other_id TEXT, name TEXT, secret TEXT)",
		"CREATE TABLE children(item_id INTEGER, physical_id TEXT)",
		"INSERT INTO items(id,physical_id,other_id,name,secret) VALUES(1,'item-1','other-1','one','private'),(2,'item-2','other-2','two','private')",
		"INSERT INTO children(item_id,physical_id) VALUES(1,'item-1')",
	); err != nil {
		t.Fatal(err)
	}
	const joinedSQL = "SELECT i.id, c.physical_id AS result_id, i.name FROM items i LEFT JOIN children c ON c.item_id = i.id ORDER BY i.id"
	t.Run("valid base joined alias control", func(t *testing.T) {
		execution := newSQLXAliasExecution(t, h, reflect.TypeOf(sqlxAliasItem{}), reflect.TypeOf(sqlxAliasOutput{}), joinedSQL)
		got, err := execution.Read(ctx, &sqlxAliasInput{Fields: []string{"id", "name"}}, nil, nil)
		if err != nil {
			t.Fatalf("known-good joined query rejected: %v", err)
		}
		data := got.(*sqlxAliasOutput)
		if len(data.Data) != 2 || data.Data[0].ID != 1 || data.Data[0].Name != "one" || data.Data[1].ID != 2 || data.Data[1].Name != "two" {
			t.Fatalf("known-good joined rows = %#v", data.Data)
		}
	})
	t.Run("unprojected alias", func(t *testing.T) {
		execution := newSQLXAliasExecution(t, h, reflect.TypeOf(sqlxAliasNegativeItem{}), reflect.TypeOf(sqlxAliasNegativeOutput{}), "SELECT i.id FROM items i LEFT JOIN children c ON c.item_id = i.id")
		_, err := execution.Read(ctx, &sqlxAliasInput{Fields: []string{"unprojected"}}, nil, nil)
		requireSelectorResponseError(t, err, "not found column unprojected")
	})
	t.Run("hidden field", func(t *testing.T) {
		execution := newSQLXAliasExecution(t, h, reflect.TypeOf(sqlxAliasNegativeItem{}), reflect.TypeOf(sqlxAliasNegativeOutput{}), "SELECT i.id, i.secret FROM items i LEFT JOIN children c ON c.item_id = i.id")
		_, err := execution.Read(ctx, &sqlxAliasInput{Fields: []string{"hidden"}}, nil, nil)
		requireSelectorResponseError(t, err, "not found column hidden")
	})
	t.Run("raw source alias is not a projected selector", func(t *testing.T) {
		execution := newSQLXAliasExecution(t, h, reflect.TypeOf(sqlxAliasItem{}), reflect.TypeOf(sqlxAliasOutput{}), joinedSQL)
		_, err := execution.Read(ctx, &sqlxAliasInput{Fields: []string{"physical_id"}}, nil, nil)
		requireSelectorResponseError(t, err, "not found column physical_id")
	})
	t.Run("raw projected result alias remains selectable", func(t *testing.T) {
		execution := newSQLXAliasExecution(t, h, reflect.TypeOf(sqlxAliasItem{}), reflect.TypeOf(sqlxAliasOutput{}), joinedSQL)
		got, err := execution.Read(ctx, &sqlxAliasInput{Fields: []string{"id", "result_id"}}, nil, nil)
		if err != nil {
			t.Fatalf("raw SQL output alias result_id should remain a valid projected selector: %v", err)
		}
		rows := got.(*sqlxAliasOutput).Data
		if len(rows) != 2 || rows[0].ID != 1 || rows[0].ItemID == nil || *rows[0].ItemID != "item-1" || rows[1].ID != 2 || rows[1].ItemID != nil {
			t.Fatalf("raw alias selected rows = %#v", rows)
		}
	})
	t.Run("qualified selector is not a public field name", func(t *testing.T) {
		execution := newSQLXAliasExecution(t, h, reflect.TypeOf(sqlxAliasItem{}), reflect.TypeOf(sqlxAliasOutput{}), joinedSQL)
		_, err := execution.Read(ctx, &sqlxAliasInput{Fields: []string{"items.itemId"}}, nil, nil)
		requireSelectorResponseError(t, err, "not found column items.itemid")
	})
	t.Run("transitive unprojected alternative", func(t *testing.T) {
		execution := newSQLXAliasExecution(t, h, reflect.TypeOf(sqlxAliasItem{}), reflect.TypeOf(sqlxAliasOutput{}), joinedSQL)
		_, err := execution.Read(ctx, &sqlxAliasInput{Fields: []string{"notProjected"}}, nil, nil)
		requireSelectorResponseError(t, err, "not found column notprojected")
	})
	t.Run("ambiguous secondary alias", func(t *testing.T) {
		execution := newSQLXAliasExecution(t, h, reflect.TypeOf(sqlxAliasAmbiguousItem{}), reflect.TypeOf(sqlxAliasAmbiguousOutput{}), "SELECT id, physical_id AS shared_result, other_id AS shared_result FROM items")
		_, err := execution.Read(ctx, &sqlxAliasInput{Fields: []string{"id", "primary"}}, nil, nil)
		if err == nil || !strings.Contains(err.Error(), `duplicate output column "shared_result"`) {
			t.Fatalf("duplicate result alias error = %v", err)
		}
	})
	t.Run("two fields share one projected secondary alias", func(t *testing.T) {
		execution := newSQLXAliasExecution(t, h, reflect.TypeOf(sqlxAliasAmbiguousItem{}), reflect.TypeOf(sqlxAliasAmbiguousOutput{}), "SELECT id, physical_id AS shared_result FROM items")
		for _, name := range []string{"primary", "shadow"} {
			t.Run(name, func(t *testing.T) {
				_, err := execution.Read(ctx, &sqlxAliasInput{Fields: []string{"id", name}}, nil, nil)
				requireSelectorResponseError(t, err, "not found column "+name)
			})
		}
	})
	t.Run("one field alias set maps to multiple projected outputs", func(t *testing.T) {
		// The primary physical_id target is absent. Both secondary alternatives
		// result_id and not_projected are present, so fallback has no unique output.
		execution := newSQLXAliasExecution(t, h, reflect.TypeOf(sqlxAliasItem{}), reflect.TypeOf(sqlxAliasOutput{}), "SELECT i.id, c.physical_id AS result_id, i.name AS not_projected FROM items i LEFT JOIN children c ON c.item_id = i.id")
		_, err := execution.Read(ctx, &sqlxAliasInput{Fields: []string{"itemId"}}, nil, nil)
		requireSelectorResponseError(t, err, "not found column itemid")
	})
	t.Run("hidden SQLX destination cannot steal visible secondary alias", func(t *testing.T) {
		execution := newSQLXAliasExecution(t, h, reflect.TypeOf(sqlxAliasHiddenOwnerItem{}), reflect.TypeOf(sqlxAliasHiddenOwnerOutput{}), "SELECT i.id, c.physical_id AS result_id FROM items i LEFT JOIN children c ON c.item_id = i.id ORDER BY i.id")
		got, err := execution.Read(ctx, &sqlxAliasInput{Fields: []string{"id", "visible"}}, nil, nil)
		if err != nil {
			requireSelectorResponseError(t, err, "not found column visible")
			return
		}
		rows := got.(*sqlxAliasHiddenOwnerOutput).Data
		var observed []struct {
			ID      int
			Visible *string
			Hidden  *string
		}
		for _, row := range rows {
			observed = append(observed, struct {
				ID      int
				Visible *string
				Hidden  *string
			}{row.ID, row.Visible, row.Hidden})
		}
		var visibleValues []string
		for _, row := range rows {
			if row.Visible == nil {
				visibleValues = append(visibleValues, "<nil>")
			} else {
				visibleValues = append(visibleValues, *row.Visible)
			}
		}
		t.Logf("visible/hidden destinations for one SQLX alias = %#v; visible values=%v", observed, visibleValues)
		if len(rows) != 2 || rows[0].Visible == nil || *rows[0].Visible != "item-1" || rows[0].Hidden != nil || rows[1].ID != 2 || rows[1].Visible != nil || rows[1].Hidden != nil {
			t.Fatalf("visible field was not uniquely hydrated: %#v", observed)
		}
	})
	t.Run("hidden SQLX destination before visible destination cannot steal alias", func(t *testing.T) {
		execution := newSQLXAliasExecution(t, h, reflect.TypeOf(sqlxAliasHiddenFirstItem{}), reflect.TypeOf(sqlxAliasHiddenFirstOutput{}), "SELECT i.id, c.physical_id AS result_id FROM items i LEFT JOIN children c ON c.item_id = i.id ORDER BY i.id")
		got, err := execution.Read(ctx, &sqlxAliasInput{Fields: []string{"id", "visible"}}, nil, nil)
		if err != nil {
			requireSelectorResponseError(t, err, "not found column visible")
			return
		}
		rows := got.(*sqlxAliasHiddenFirstOutput).Data
		var observed []struct {
			ID      int
			Visible *string
			Hidden  *string
		}
		for _, row := range rows {
			observed = append(observed, struct {
				ID      int
				Visible *string
				Hidden  *string
			}{row.ID, row.Visible, row.Hidden})
		}
		if len(rows) != 2 || rows[0].Visible == nil || *rows[0].Visible != "item-1" || rows[0].Hidden != nil || rows[1].Visible != nil || rows[1].Hidden != nil {
			t.Fatalf("visible selector hydrated a different destination: %#v", observed)
		}
	})
	t.Run("sqlx-excluded transient does not steal visible secondary alias", func(t *testing.T) {
		execution := newSQLXAliasExecution(t, h, reflect.TypeOf(sqlxAliasTransientItem{}), reflect.TypeOf(sqlxAliasTransientOutput{}), "SELECT i.id, c.physical_id AS result_id FROM items i LEFT JOIN children c ON c.item_id = i.id ORDER BY i.id")
		got, err := execution.Read(ctx, &sqlxAliasInput{Fields: []string{"id", "visible"}}, nil, nil)
		if err != nil {
			t.Fatalf("sqlx-excluded field must not own result_id: %v", err)
		}
		rows := got.(*sqlxAliasTransientOutput).Data
		if len(rows) != 2 || rows[0].ID != 1 || rows[0].Visible == nil || *rows[0].Visible != "item-1" || rows[1].ID != 2 || rows[1].Visible != nil {
			t.Fatalf("transient-collision rows = %#v", rows)
		}
	})
}
