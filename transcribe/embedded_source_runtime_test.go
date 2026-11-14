package transcribe

import (
	"database/sql"
	"encoding/json"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/viant/bindly/resource"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	sqltemplate "github.com/viant/datly/sql/template"
	"github.com/viant/datly/transcribe/column"
	tcompile "github.com/viant/datly/transcribe/compile"
	"github.com/viant/datly/typecatalog"
	"github.com/viant/sqlx/metadata/database"
	"github.com/viant/sqlx/metadata/info"
	_ "github.com/viant/sqlx/metadata/product/mysql"
)

func TestEmbeddedLockingDeclarationOwnership(t *testing.T) {
	const query = "SELECT id FROM records ORDER BY id\n#if($Ids) ${View.ForUpdate()} #end"
	const authored = "${embed:sql/locks.sql}"
	resources := resource.New()
	require.NoError(t, resources.Register("", fstest.MapFS{"sql/locks.sql": {Data: []byte(query)}}))
	component := &spec.Component{Key: spec.Key{Scope: "example.com/embedded", Name: "Records"}, Parameters: []*spec.Parameter{{Name: "Locks", Source: spec.BindSource{Kind: "view"}, DeclarationSQL: authored}}}
	compiled, err := newDeclarationCompiler(component, nil, resources).compile()
	require.NoError(t, err)
	require.Len(t, compiled.views, 1)
	source := compiled.views[0].Source
	require.Equal(t, authored, source.SQL, "the resource retains its terminal fragment")
	require.Len(t, source.Embeds, 1)
	resolved := source.Clone()
	require.NoError(t, dsql.ResolveSource("Locks", resolved, resources))
	require.Equal(t, query, resolved.SQL)
	stored, err := resources.ReadFile("sql/locks.sql")
	require.NoError(t, err)
	require.Equal(t, query, string(stored))
	require.Equal(t, authored, component.Parameters[0].DeclarationSQL)
}

// This DSN must designate a disposable schema owned exclusively by these tests.
func embeddedMySQL(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("DATLY_EMBEDDED_MYSQL_DSN")
	if dsn == "" {
		t.Skip("requires DATLY_EMBEDDED_MYSQL_DSN with an owned disposable schema")
	}
	cfg, err := mysql.ParseDSN(dsn)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(cfg.DBName, "datly_embedded_"), "requires owned embedded-source schema")
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	require.NoError(t, db.PingContext(t.Context()))
	return db
}

func TestEmbeddedLockingSourceMySQL(t *testing.T) {
	db := embeddedMySQL(t)
	_, err := db.Exec("CREATE TABLE embedded_locks(id INT PRIMARY KEY) ENGINE=InnoDB")
	require.NoError(t, err)
	t.Cleanup(func() { db.Exec("DROP TABLE embedded_locks") })
	_, err = db.Exec("INSERT INTO embedded_locks VALUES(1)")
	require.NoError(t, err)
	const query = `SELECT id FROM embedded_locks
WHERE $criteria.In("id", $Ids)
ORDER BY id
#if($Ids) ${View.ForUpdate()} #end`
	for _, name := range []string{"direct", "embedded", "embedded auxiliary"} {
		t.Run(name, func(t *testing.T) {
			embedded := name != "direct"
			query := query
			if name == "embedded auxiliary" {
				query = strings.Replace(query, "FROM embedded_locks", "FROM (embedded_locks)", 1)
			}
			resources := resource.New()
			require.NoError(t, resources.Register("", fstest.MapFS{"sql/locks.sql": {Data: []byte(query)}}))
			body := query
			if embedded {
				body = "${embed:sql/locks.sql}"
			}
			var view *spec.View
			if embedded {
				text := `#setting($_ = $route('/locks','GET'))
#define($_ = $Ids<[]int>(query/ids))
#define($_ = $Locks<?>(view/Locks).Cardinality('Many').WithTypeName('LockRow').WithConnector('main').WithColumnType('id','int') /*
${embed:sql/locks.sql}
*/)
SELECT id FROM embedded_locks`
				source := &Source{Name: "Records", Scope: "example.com/embedded", Connector: "main", Text: text, Resources: resources, ColumnRefiner: column.New(column.Connections{"main": db})}
				compiled, err := NewCompiler().Compile(t.Context(), source)
				require.NoError(t, err)
				require.Len(t, compiled.Component.Views, 1)
				view = compiled.Component.Views[0]
				require.Equal(t, text, source.Text)
			} else {
				var err error
				view, err = tcompile.NewReader().Compile(tcompile.ReadInput{View: &spec.View{Name: "Locks", Source: &spec.ViewSource{SQL: body}}, SQL: body, Resources: resources})
				require.NoError(t, err)
			}
			resolved := view.Source.Clone()
			if name == "embedded auxiliary" {
				require.True(t, view.Auxiliary)
				require.NotContains(t, resolved.SQL, "(embedded_locks)")
			}
			require.NoError(t, dsql.ResolveSource(view.Name, resolved, resources))
			require.Equal(t, 1, strings.Count(resolved.SQL, "${View.ForUpdate()}"), resolved.SQL)
			type input struct{ Ids []int }
			program, err := (sqltemplate.Compiler{Source: resolved.SQL, InputType: reflect.TypeFor[input](), Variables: []sqltemplate.Variable{{Name: "Ids", FieldIndex: []int{0}}}}).Compile()
			require.NoError(t, err)
			for _, ids := range [][]int{nil, {1}} {
				tx, err := db.BeginTx(t.Context(), nil)
				require.NoError(t, err)
				func() {
					defer tx.Rollback()
					result, err := program.Evaluate(t.Context(), sqltemplate.Invocation{Input: reflect.ValueOf(input{Ids: ids}), View: sqltemplate.ViewInput{Dialect: &info.Dialect{Product: database.Product{Name: "MySQL"}}, TransactionActive: true}})
					require.NoError(t, err)
					want := 0
					if len(ids) != 0 {
						want = 1
					}
					require.Equal(t, want, strings.Count(strings.ToUpper(result.SQL), "FOR UPDATE"), result.SQL)
					rows, err := tx.QueryContext(t.Context(), result.SQL, result.Args...)
					require.NoError(t, err, result.SQL)
					defer rows.Close()
					if len(ids) == 0 {
						require.False(t, rows.Next(), "empty membership selects no rows")
					} else {
						require.True(t, rows.Next())
						var id int
						require.NoError(t, rows.Scan(&id))
						require.Equal(t, 1, id)
					}
					require.NoError(t, rows.Err())
				}()
			}
		})
	}
}

func TestEmbeddedAuxiliaryWriterSQLite(t *testing.T) { runEmbeddedAuxiliaryWriter(t, false) }
func TestEmbeddedAuxiliaryWriterMySQL(t *testing.T)  { runEmbeddedAuxiliaryWriter(t, true) }

func runEmbeddedAuxiliaryWriter(t *testing.T, mysqlDialect bool) {
	for _, embedded := range []bool{false, true} {
		t.Run(map[bool]string{false: "direct", true: "embedded"}[embedded], func(t *testing.T) {
			db := sqlite.New(t)
			schema := auxiliarySchema
			if mysqlDialect {
				db = &sqlite.Harness{DB: embeddedMySQL(t)}
				schema = []string{
					"CREATE TABLE ORDERS(ID BIGINT PRIMARY KEY AUTO_INCREMENT, NAME VARCHAR(64) NOT NULL, WINDOW_START BIGINT, WINDOW_END BIGINT) ENGINE=InnoDB",
					"CREATE TABLE AUXILIARY(ID BIGINT PRIMARY KEY AUTO_INCREMENT, ORDER_ID BIGINT NOT NULL, NAME VARCHAR(64) NOT NULL) ENGINE=InnoDB",
					"INSERT INTO ORDERS VALUES(1,'before',3,9)", "INSERT INTO AUXILIARY VALUES(7,1,'lookup')",
				}
				for _, action := range []string{"INSERT", "UPDATE", "DELETE"} {
					schema = append(schema, "CREATE TRIGGER no_aux_"+strings.ToLower(action)+" BEFORE "+action+" ON AUXILIARY FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='auxiliary write forbidden'")
				}
				t.Cleanup(func() { db.DB.Exec("DROP TABLE AUXILIARY"); db.DB.Exec("DROP TABLE ORDERS") })
			}
			require.NoError(t, db.ExecStatements(t.Context(), schema...))
			resources := resource.New()
			const dependency = "SELECT a.* FROM (AUXILIARY) a WHERE a.ID=7"
			require.NoError(t, resources.Register("", fstest.MapFS{"sql/dependency.sql": {Data: []byte(dependency)}}))
			dependencySQL := dependency
			if embedded {
				dependencySQL = "${embed:sql/dependency.sql}"
			}
			text := `#setting($_ = $route('/orders','PATCH'))
#define($_ = $Orders<[]*OrdersView>(body/Data).Cardinality('Many').Required())
#define($_ = $CurrentOrders<?>(view/CurrentOrders).Cardinality('Many') /* SELECT ID,NAME,WINDOW_START,WINDOW_END FROM ORDERS */)
#define($_ = $CurrentAux<?>(view/CurrentAux).Cardinality('Many') /* SELECT ID,ORDER_ID,NAME FROM (AUXILIARY) */)
#define($_ = $Data<[]*OrdersView>(output/body))
SELECT orders.*, Aux.* FROM (SELECT o.* FROM ORDERS o WHERE o.ID>0) orders LEFT JOIN (` + dependencySQL + `) Aux ON Aux.ORDER_ID=orders.ID`
			source := &Source{Scope: "github.com/viant/datly/auxfixture/generated", Name: "Orders", Connector: "main", Text: text, Resources: resources, Types: typecatalog.NewCatalog(), ColumnRefiner: column.New(column.Connections{"main": db.DB})}
			compiled, err := NewCompiler().Compile(t.Context(), source)
			require.NoError(t, err)
			child := compiled.Component.RootView.Relations[0].View
			require.True(t, child.Auxiliary)
			require.NotContains(t, child.Source.SQL, "(AUXILIARY)")
			identity, err := child.Identity()
			require.NoError(t, err)
			root := t.TempDir()
			(testharness.GeneratedModule{Path: "github.com/viant/datly/auxfixture"}).Write(t, root)
			_, err = NewCompiler().Transcribe(t.Context(), Request{Source: source, Destination: root, Options: Options{Handler: HandlerOptions{Target: HandlerGo, Operation: WritePatch, Current: "CurrentOrders", Currents: []CurrentBinding{{ViewIdentity: identity, Param: "CurrentAux"}}}}})
			require.NoError(t, err)
			require.Equal(t, text, source.Text)
			componentJSON, err := json.Marshal(compiled.Component)
			require.NoError(t, err)
			writeSourceFile(t, root, "generated/component_fixture.json", string(componentJSON))
			schemaJSON, err := json.Marshal(schema)
			require.NoError(t, err)
			runtimeSource := strings.NewReplacer("{{AUX_ROOT}}", "false", "{{IMPORTS}}", `customhandler "github.com/viant/datly/runtime/handler/custom"`, "{{FACTORY}}", "generated:=customhandler.New[OrdersInput,OrdersOutput](NewOrdersHandler())", "{{SCHEMA}}", string(schemaJSON), "{{METHOD}}", "PATCH", "{{BODY}}", `{"Data":[{"id":1,"name":"after","Aux":[{"orderId":999,"name":"business"}]}]}`, "{{OPERATION}}", "patch").Replace(auxiliaryRuntimeSource)
			if mysqlDialect {
				// Discovery and the generated runtime share this owned schema. Let the
				// runtime create its own fresh data and write-rejection triggers.
				_, err = db.DB.Exec("DROP TABLE AUXILIARY")
				require.NoError(t, err)
				_, err = db.DB.Exec("DROP TABLE ORDERS")
				require.NoError(t, err)
				runtimeSource = strings.Replace(runtimeSource, `"context"`, `"context"`+"\n\"database/sql\"\n_ \"github.com/go-sql-driver/mysql\"\n_ \"github.com/viant/sqlx/metadata/product/mysql\"", 1)
				runtimeSource = strings.Replace(runtimeSource, "db:=sqlite.New(t)", `nativeDB,err:=sql.Open("mysql",os.Getenv("DATLY_EMBEDDED_MYSQL_DSN"));if err!=nil{t.Fatal(err)};defer nativeDB.Close();db:=&sqlite.Harness{DB:nativeDB}`, 1)
				// MySQL's NOT NULL identity column has a non-pointer generated field.
				runtimeSource = strings.Replace(runtimeSource, "result.Data[0].Aux[0].Id!=nil", "(result.Data[0].Aux[0].Id!=nil && *result.Data[0].Aux[0].Id!=0)", 1)
			}
			writeSourceFile(t, root, "generated/embedded_fixture_test.go", runtimeSource)
			command := exec.CommandContext(t.Context(), "go", "test", "-mod=mod", "./...")
			command.Dir = root
			output, err := command.CombinedOutput()
			require.NoError(t, err, string(output))
		})
	}
}
