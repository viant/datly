package transcribe

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly/resource"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/datly/transcribe/column"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/typecatalog"
	sqlxread "github.com/viant/sqlx/io/read"
)

// The application's outer aliases are Go-field names. SQLX keeps the physical
// names first, including for separately executed embedded site/publisher views.
func TestSiteListOuterAliasesRetainPhysicalMapping(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	require.NoError(t, db.ExecStatements(ctx,
		"CREATE TABLE SITE_LIST(ID INTEGER PRIMARY KEY,NAME TEXT)",
		"CREATE TABLE SITE_LIST_MATCH(SITE_ID INTEGER,SITE_LIST_ID INTEGER,MATCH_RULES_LIST TEXT,SITE_LIST_MATCH_RULE_COUNT INTEGER)",
		"CREATE TABLE SITE_LIST_INCLUSION(SITE_ID INTEGER,SITE_LIST_ID INTEGER)",
		"CREATE TABLE SITE_LIST_MATCH_ID(SITE_ID INTEGER,SITE_LIST_ID INTEGER)",
		"CREATE TABLE SITE_LIST_EXCLUSION(SITE_ID INTEGER,SITE_LIST_ID INTEGER)",
		"CREATE TABLE CI_SITE(ID INTEGER PRIMARY KEY,NAME TEXT,PUBLISHER_ID INTEGER)",
		"CREATE TABLE CI_PUBLISHER(ID INTEGER PRIMARY KEY,NAME TEXT)"))
	root := t.TempDir()
	testharness.WriteGeneratedGoMod(t, root)
	sourceDir := filepath.Join(root, "source")
	require.NoError(t, os.MkdirAll(filepath.Join(sourceDir, "meta"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(sourceDir, "meta/ci_site.sql"), []byte("SELECT ID,NAME,PUBLISHER_ID FROM CI_SITE"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(sourceDir, "meta/ci_publisher.sql"), []byte("SELECT ID,NAME FROM CI_PUBLISHER"), 0644))
	text := `#package('records')
#setting($_ = $route('/sitelist/match','GET'))
#set($_ = $Data<?>(output/view))
SELECT site_list_match.*,site.NAME AS SITE_NAME,publisher.ID AS PUBLISHER_ID,publisher.NAME AS PUBLISHER_NAME,
use_connector(match,'bq_sitemgmt_match'),use_connector(site,'ci_ads'),use_connector(publisher,'ci_ads'),use_connector(site_list_match,'sitemgmt'),
cardinality(site_list_match,'One'),tag(exclusion,'json:"exclusion,omitempty"'),set_limit(match,0)
FROM (SELECT sl.ID,sl.NAME FROM SITE_LIST sl) site_list_match
JOIN (SELECT SITE_ID,SITE_LIST_ID,MAX(MATCH_RULES_LIST) AS MATCH_RULES_LIST,MAX(SITE_LIST_MATCH_RULE_COUNT) AS SITE_LIST_MATCH_RULE_COUNT
FROM (SELECT SITE_ID,SITE_LIST_ID,MATCH_RULES_LIST,SITE_LIST_MATCH_RULE_COUNT FROM SITE_LIST_MATCH
UNION ALL SELECT SITE_ID,SITE_LIST_ID,'' AS MATCH_RULES_LIST,0 AS SITE_LIST_MATCH_RULE_COUNT FROM SITE_LIST_INCLUSION
UNION ALL SELECT SITE_ID,SITE_LIST_ID,'' AS MATCH_RULES_LIST,0 AS SITE_LIST_MATCH_RULE_COUNT FROM SITE_LIST_MATCH_ID) m
GROUP BY 1,2) match ON match.SITE_LIST_ID=site_list_match.ID
JOIN (SELECT * FROM SITE_LIST_EXCLUSION) exclusion ON exclusion.SITE_LIST_ID=site_list_match.ID
JOIN (${embed:meta/ci_site.sql}) site ON site.ID=match.SITE_ID AND 1=1
JOIN (${embed:meta/ci_publisher.sql}) publisher ON publisher.ID=site.PUBLISHER_ID AND 1=1`
	resources, err := resource.New().WithDefault(os.DirFS(sourceDir))
	require.NoError(t, err)
	compiled, err := NewCompiler().Compile(ctx, &Source{Name: "SiteListMatch", Path: filepath.Join(sourceDir, "sitelist.dql"), Text: text, Resources: resources, Types: typecatalog.NewCatalog(), Connector: "sitemgmt", ColumnRefiner: column.New(column.Connections{"sitemgmt": db.DB, "bq_sitemgmt_match": db.DB, "ci_ads": db.DB})})
	require.NoError(t, err)
	input, _, err := generationInput(root, "records", compiled)
	require.NoError(t, err)
	vendorViews := map[string]*spec.View{}
	var inspect func(*spec.View)
	inspect = func(view *spec.View) {
		if view == nil {
			return
		}
		if view.Namespace == "site" || view.Namespace == "publisher" {
			vendorViews[view.Namespace] = view
			require.NotContains(t, view.Source.SQL, "AS SITE_NAME")
			require.NotContains(t, view.Source.SQL, "AS PUBLISHER_NAME")
			require.NotContains(t, view.Source.SQL, "AS PUBLISHER_ID")
			t.Logf("%s SQL: %s", view.Namespace, view.Source.SQL)
		}
		for _, relation := range view.Relations {
			inspect(relation.View)
		}
	}
	inspect(compiled.Component.RootView)
	plan, err := gen.New(input).Plan()
	require.NoError(t, err)
	type expected struct {
		field, physical, alias, namespace string
		value                             any
	}
	require.NoError(t, db.ExecStatements(ctx, "INSERT INTO CI_SITE VALUES(5,'site-alpha',7)", "INSERT INTO CI_PUBLISHER VALUES(7,'publisher-alpha')"))
	for _, want := range []expected{
		{"SiteName", "NAME", "SITE_NAME", "site", "site-alpha"},
		{"PublisherId", "ID", "PUBLISHER_ID", "publisher", 7},
		{"PublisherName", "NAME", "PUBLISHER_NAME", "publisher", "publisher-alpha"},
	} {
		found := false
		for _, view := range plan.Views {
			for _, field := range view.Fields {
				if field.Name != want.field || reflect.StructTag(field.Tag).Get("internal") == "true" {
					continue
				}
				found = true
				raw := reflect.StructTag(field.Tag).Get("sqlx")
				mappings := strings.Split(strings.Split(raw, ",")[0], "|")
				require.Equal(t, want.physical, mappings[0], "%s: %s", field.Name, field.Tag)
				require.Equal(t, []string{want.physical}, mappings, "outer configuration alias must not become a SQL result alias")
				t.Logf("%s: %s", field.Name, field.Tag)
				// Execute the actual compiler-produced vendor query, including its
				// backing columns and embedded inner SQL. No artificial AS is introduced.
				source := vendorViews[want.namespace].Source.Clone()
				require.NoError(t, dsql.ResolveSource(want.namespace, source, resources))
				var fields []reflect.StructField
				for _, generated := range view.Fields {
					if generated.RelationHolder {
						continue
					}
					base := strings.TrimPrefix(generated.Type, "*")
					var typ reflect.Type
					switch base {
					case "int":
						typ = reflect.TypeOf(int(0))
					case "int64":
						typ = reflect.TypeOf(int64(0))
					case "string":
						typ = reflect.TypeOf("")
					default:
						t.Fatalf("unexpected fixture type %s", generated.Type)
					}
					if strings.HasPrefix(generated.Type, "*") {
						typ = reflect.PointerTo(typ)
					}
					fields = append(fields, reflect.StructField{Name: generated.Name, Type: typ, Tag: reflect.StructTag(generated.Tag)})
				}
				rowType := reflect.StructOf(fields)
				reader, err := sqlxread.New(ctx, db.DB, source.SQL, func() any { return reflect.New(rowType).Interface() })
				require.NoError(t, err)
				count := 0
				err = reader.QueryAll(ctx, func(value any) error {
					count++
					actual := reflect.ValueOf(value).Elem().FieldByName(field.Name)
					if actual.Kind() == reflect.Pointer {
						require.False(t, actual.IsNil())
						actual = actual.Elem()
					}
					require.EqualValues(t, want.value, actual.Interface())
					return nil
				})
				if reader.Stmt() != nil {
					require.NoError(t, reader.Stmt().Close())
				}
				require.NoError(t, err)
				require.Equal(t, 1, count)

			}
		}
		require.True(t, found, "missing generated %s", want.field)
	}
}

func TestSiteNameWithPhysicalMappingOnly(t *testing.T) {
	ctx := context.Background()
	db := testharness.NewSQLiteHarness(t)
	require.NoError(t, db.ExecStatements(ctx, "CREATE TABLE CI_SITE(NAME TEXT)", "INSERT INTO CI_SITE VALUES('site-alpha')"))
	type row struct {
		SiteName string `sqlx:"NAME"`
	}
	for _, tc := range []struct {
		name, query string
		alias       bool
	}{
		{"original column", "SELECT NAME FROM CI_SITE", false},
		{"aliased result needs alternative mapping", "SELECT NAME AS SITE_NAME FROM CI_SITE", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader, err := sqlxread.New(ctx, db.DB, tc.query, func() any { return &row{} })
			require.NoError(t, err)
			var actual []row
			err = reader.QueryAll(ctx, func(value any) error { actual = append(actual, *value.(*row)); return nil })
			if reader.Stmt() != nil {
				require.NoError(t, reader.Stmt().Close())
			}
			if tc.alias {
				require.ErrorContains(t, err, "failed to match columns: [SITE_NAME]")
				require.Empty(t, actual)
			} else {
				require.NoError(t, err)
				require.Equal(t, []row{{"site-alpha"}}, actual)
			}
		})
	}
}
