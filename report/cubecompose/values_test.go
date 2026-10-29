package cubecompose

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness/sqlite"
	"reflect"
	"testing"
)

type scalarControls struct {
	Threshold int64  `json:"threshold"`
	Label     string `json:"label" validate:"choice(alpha,beta)" choice:"alpha" choice:"beta"`
	Unused    *int   `json:"unused"`
}

func TestScalarValuesOrderAndAdmission(t *testing.T) {
	r := require.New(t)
	catalog, err := NewCatalog(Field{Name: "key", Role: Dimension, Type: reflect.TypeFor[string]()}, Field{Name: "amount", Role: Measure, Type: reflect.TypeFor[int64]()})
	r.NoError(err)
	values, err := NewValuesContract(reflect.TypeFor[scalarControls]())
	r.NoError(err)
	sql := `SELECT $Values.label AS label,t1.amount FROM $CubeSQL1 AS t1 WHERE t1.amount >= $Values.threshold ORDER BY $Values.threshold`
	p, err := CompileWithValues(sql, catalog, 1, 10, nil, values)
	r.NoError(err)
	bound, err := p.BindValues(context.Background(), json.RawMessage(`{"label":"alpha","threshold":9007199254740993}`))
	r.NoError(err)
	rendered, args, err := bound.Render([]Frame{{SQL: "SELECT ? AS amount", Args: []interface{}{int64(7)}}})
	r.NoError(err)
	r.Contains(rendered, "WHERE t1.amount >= ?")
	r.Equal([]interface{}{"alpha", int64(7), int64(9007199254740993), int64(9007199254740993)}, args)
	_, _, err = p.Render([]Frame{{SQL: "SELECT 7 AS amount"}})
	r.Error(err)
	for _, raw := range []string{`{}`, `null`, `[]`, `{"label":"alpha","threshold":null}`, `{"label":"alpha","threshold":"3"}`, `{"label":"alpha","threshold":3.5}`, `{"label":"alpha","threshold":3,"bad":1}`, `{"label":"gamma","threshold":3}`, `{"label":"alpha","threshold":3,"unused":null}`} {
		_, err = p.BindValues(context.Background(), json.RawMessage(raw))
		r.Error(err, raw)
	}
	_, err = Compile(sql, catalog, 1, 10)
	r.Error(err)
	for _, sql := range []string{`SELECT t1.amount FROM $CubeSQL1 AS t1 WHERE t1.amount > $Values.unknown`, `SELECT t1.amount FROM $CubeSQL1 AS t1 ORDER BY $Values.label DESC; DROP TABLE x`, `SELECT t1.amount FROM $Values.label AS t1`, `SELECT t1.$Values.label FROM $CubeSQL1 AS t1`} {
		_, err = CompileWithValues(sql, catalog, 1, 10, nil, values)
		r.Error(err, sql)
	}
}

func TestScalarValuesClausesAndUnionOrder(t *testing.T) {
	r := require.New(t)
	catalog, err := NewCatalog(Field{Name: "key", Role: Dimension, Type: reflect.TypeFor[string]()}, Field{Name: "amount", Role: Measure, Type: reflect.TypeFor[int64]()})
	r.NoError(err)
	values, err := NewValuesContract(reflect.TypeFor[scalarControls]())
	r.NoError(err)
	for _, tc := range []struct {
		sql    string
		frames []Frame
		args   []interface{}
	}{
		{`SELECT $Values.label AS label,SUM(t1.amount) AS total FROM $CubeSQL1 AS t1 WHERE t1.amount > $Values.threshold GROUP BY $Values.label HAVING total > $Values.threshold ORDER BY $Values.label`, []Frame{{SQL: "SELECT ? AS amount", Args: []interface{}{int64(600)}}}, []interface{}{"alpha", int64(600), int64(1000), "alpha", int64(1000), "alpha"}},
		{`SELECT t1.key AS key,$Values.label AS label FROM $CubeSQL1 AS t1 UNION ALL SELECT t2.key AS key,$Values.label AS label FROM $CubeSQL2 AS t2 ORDER BY key LIMIT 5`, []Frame{{SQL: "SELECT ? AS key", Args: []interface{}{"a"}}, {SQL: "SELECT ? AS key", Args: []interface{}{"b"}}}, []interface{}{"alpha", "a", "alpha", "b"}},
	} {
		p, err := CompileWithValues(tc.sql, catalog, len(tc.frames), 10, nil, values)
		r.NoError(err)
		bound, err := p.BindValues(context.Background(), json.RawMessage(`{"label":"alpha","threshold":1000}`))
		r.NoError(err)
		_, args, err := bound.Render(tc.frames)
		r.NoError(err)
		r.Equal(tc.args, args)
	}
	// Omitted declarations unused by this authored SQL are not required or validated.
	p, err := CompileWithValues(`SELECT t1.amount FROM $CubeSQL1 AS t1 WHERE t1.amount > $Values.threshold`, catalog, 1, 10, nil, values)
	r.NoError(err)
	_, err = p.BindValues(context.Background(), json.RawMessage(`{"threshold":0}`))
	r.NoError(err)
}

func TestScalarValuesRequestIsolation(t *testing.T) {
	catalog, err := NewCatalog(Field{Name: "amount", Role: Measure, Type: reflect.TypeFor[int64]()})
	require.NoError(t, err)
	values, err := NewValuesContract(reflect.TypeFor[scalarControls]())
	require.NoError(t, err)
	plan, err := CompileWithValues(`SELECT t1.amount FROM $CubeSQL1 AS t1 WHERE t1.amount >= $Values.threshold`, catalog, 1, 10, nil, values)
	require.NoError(t, err)
	done := make(chan error, 16)
	for i := 0; i < 16; i++ {
		go func(i int) {
			raw, _ := json.Marshal(map[string]int{"threshold": i})
			bound, err := plan.BindValues(context.Background(), raw)
			if err != nil {
				done <- err
				return
			}
			_, args, err := bound.Render([]Frame{{SQL: "SELECT 1200 AS amount"}})
			if err == nil && (!reflect.DeepEqual(args, []interface{}{int64(i)})) {
				err = fmt.Errorf("request value crossed invocation: %v", args)
			}
			done <- err
		}(i)
	}
	for i := 0; i < 16; i++ {
		require.NoError(t, <-done)
	}
}

func TestScalarValuesDeclarations(t *testing.T) {
	for _, typ := range []reflect.Type{
		nil, reflect.TypeFor[int](), reflect.TypeFor[struct{ Nested struct{ N int } }](), reflect.TypeFor[struct{ Items []int }](), reflect.TypeFor[struct{ N **int }](),
		reflect.TypeFor[struct {
			A int `json:"same"`
			B int `json:"same"`
		}](), reflect.TypeFor[struct {
			A int `json:"unsafe-name"`
		}](),
	} {
		_, err := NewValuesContract(typ)
		require.Error(t, err, "%v", typ)
	}
}

func TestScalarValuesSQLiteExactIntegerAndLiteralSafety(t *testing.T) {
	ctx := context.Background()
	r := require.New(t)
	h := sqlite.New(t)
	type controls struct {
		Exact   int64  `json:"exact"`
		Text    string `json:"text"`
		Enabled bool   `json:"enabled"`
	}
	catalog, err := NewCatalog(Field{Name: "amount", Role: Measure, Type: reflect.TypeFor[int64]()})
	r.NoError(err)
	values, err := NewValuesContract(reflect.TypeFor[controls]())
	r.NoError(err)
	p, err := CompileWithValues(`SELECT t1.amount,$Values.exact AS exact,$Values.text AS text,$Values.enabled AS enabled FROM $CubeSQL1 AS t1`, catalog, 1, 10, nil, values)
	r.NoError(err)
	raw := json.RawMessage(`{"exact":9007199254740993,"text":"x'); DROP TABLE facts; --","enabled":false}`)
	bound, err := p.BindValues(ctx, raw)
	r.NoError(err)
	query, args, err := bound.Render([]Frame{{SQL: "SELECT 1200 AS amount"}})
	r.NoError(err)
	r.NotContains(query, "DROP TABLE")
	h.AssertQuery(t, ctx, sqlite.Query{SQL: query, Args: args}, []struct {
		Amount  int64  `sqlx:"amount"`
		Exact   int64  `sqlx:"exact"`
		Text    string `sqlx:"text"`
		Enabled bool   `sqlx:"enabled"`
	}{{1200, 9007199254740993, "x'); DROP TABLE facts; --", false}})
}

func TestScalarValuesPreserveAuthoredJSONTags(t *testing.T) {
	type quoted struct {
		Exact int64 `json:"exact,string"`
	}
	r := require.New(t)
	catalog, err := NewCatalog(Field{Name: "amount", Role: Measure, Type: reflect.TypeFor[int64]()})
	r.NoError(err)
	values, err := NewValuesContract(reflect.TypeFor[quoted]())
	r.NoError(err)
	p, err := CompileWithValues(`SELECT t1.amount FROM $CubeSQL1 AS t1 WHERE t1.amount > $Values.exact`, catalog, 1, 10, nil, values)
	r.NoError(err)
	bound, err := p.BindValues(context.Background(), json.RawMessage(`{"exact":"9007199254740993"}`))
	r.NoError(err)
	_, args, err := bound.Render([]Frame{{SQL: "SELECT 1200 AS amount"}})
	r.NoError(err)
	r.Equal([]interface{}{int64(9007199254740993)}, args)
	_, err = p.BindValues(context.Background(), json.RawMessage(`{"exact":9007199254740993}`))
	r.Error(err)
}
