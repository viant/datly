package compile

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/viant/datly/internal/translator/parser"
	"github.com/viant/datly/repository/shape"
	dqlparse "github.com/viant/datly/repository/shape/dql/parse"
	"github.com/viant/datly/repository/shape/plan"
	"github.com/viant/sqlparser"
)

// DQLCompiler compiles raw DQL into a shape plan that can be materialized by shape/load.
type DQLCompiler struct{}

// New returns a DQL compiler implementation.
func New() *DQLCompiler {
	return &DQLCompiler{}
}

// Compile implements shape.DQLCompiler.
func (c *DQLCompiler) Compile(_ context.Context, source *shape.Source, _ ...shape.CompileOption) (*shape.PlanResult, error) {
	if source == nil {
		return nil, shape.ErrNilSource
	}
	dql := strings.TrimSpace(source.DQL)
	if dql == "" {
		return nil, shape.ErrNilDQL
	}

	name, table, err := inferRoot(dql, source.Name)
	if err != nil {
		return nil, err
	}

	result := &plan.Result{
		Views: []*plan.View{
			{
				Path:        name,
				Holder:      name,
				Name:        name,
				Table:       table,
				SQL:         dql,
				Cardinality: "many",
				FieldType:   reflect.TypeOf([]map[string]interface{}{}),
				ElementType: reflect.TypeOf(map[string]interface{}{}),
			},
		},
		ViewsByName: map[string]*plan.View{},
		ByPath:      map[string]*plan.Field{},
	}
	if parsed, parseErr := dqlparse.New().Parse(dql); parseErr == nil && parsed != nil && parsed.TypeContext != nil {
		result.TypeContext = parsed.TypeContext
	}
	result.ViewsByName[name] = result.Views[0]
	return &shape.PlanResult{Source: source, Plan: result}, nil
}

func inferRoot(dql string, fallback string) (string, string, error) {
	query, err := sqlparser.ParseQuery(dql, parser.OnVeltyExpression())
	if err != nil {
		name := sanitizeName(fallback)
		if name == "" {
			name = "DQLView"
		}
		return name, "", nil
	}

	name := sanitizeName(query.From.Alias)
	if name == "" {
		name = sanitizeName(fallback)
	}
	if name == "" {
		name = "DQLView"
	}

	table := ""
	if query != nil && query.From.X != nil {
		table = strings.TrimSpace(sqlparser.Stringify(query.From.X))
	}
	if table == "" || strings.HasPrefix(table, "(") {
		table = name
	}
	if name == "" {
		return "", "", fmt.Errorf("shape compile: failed to infer view name")
	}
	return name, table, nil
}

var nonWord = regexp.MustCompile(`[^a-zA-Z0-9_]+`)

func sanitizeName(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	value = nonWord.ReplaceAllString(value, "_")
	value = strings.Trim(value, "_")
	if value == "" {
		return ""
	}
	if value[0] >= '0' && value[0] <= '9' {
		value = "V_" + value
	}
	return value
}
