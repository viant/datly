package reader

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/http"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	jwtv5 "github.com/golang-jwt/jwt/v5"
	"github.com/mattn/go-sqlite3"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	runtimeauth "github.com/viant/datly/runtime/auth"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/scy"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/scy/auth/jwt/verifier"
	xpredicate "github.com/viant/xdatly/predicate"
)

type jwtAuthorizationInput struct {
	JWT *jwt.Claims `parameter:"JWT,kind=header,in=Authorization,dataType=string,required,errorCode=401" codec:"JwtClaim" predicate:"handler,example.JWTAuthorization" json:"-"`
}

type jwtAuthorizationPredicate struct {
	Input *jwtAuthorizationInput `bind:"kind=input,required"`
}

func (p *jwtAuthorizationPredicate) Compute(_ context.Context, _ any) (*xpredicate.Criteria, error) {
	if p.Input == nil || p.Input.JWT == nil || p.Input.JWT.Subject != "user-seven" {
		return nil, fmt.Errorf("authorization denied")
	}
	return &xpredicate.Criteria{Expression: "owner_id = ?", Placeholders: []any{p.Input.JWT.UserID}}, nil
}

func TestJWTInputAuthorizationPrecedesProtectedSQL(t *testing.T) {
	ctx := context.Background()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	factory, err := runtimeauth.New(ctx, &runtimeauth.Config{JWTValidator: &verifier.Config{RSA: []*scy.Resource{{URL: "test-public-key", Data: pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: encoded})}}}})
	if err != nil {
		t.Fatal(err)
	}
	sign := func(subject string, expires time.Time) string {
		t.Helper()
		token := jwtv5.NewWithClaims(jwtv5.SigningMethodRS256, jwtv5.MapClaims{"sub": subject, "user_id": 7, "exp": expires.Unix()})
		value, err := token.SignedString(key)
		if err != nil {
			t.Fatal(err)
		}
		return "Bearer " + value
	}
	db := testharness.NewSQLiteHarness(t)
	if err := db.ExecStatements(ctx, "CREATE TABLE protected_records(id INTEGER,owner_id INTEGER)", "INSERT INTO protected_records VALUES(1,7),(2,8)"); err != nil {
		t.Fatal(err)
	}
	db.DB.SetMaxOpenConns(1)
	var reads atomic.Int64
	connection, err := db.DB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = connection.Raw(func(raw any) error {
		raw.(*sqlite3.SQLiteConn).RegisterAuthorizer(func(operation int, table, column, database string) int {
			if operation == sqlite3.SQLITE_READ && table == "protected_records" {
				reads.Add(1)
			}
			return sqlite3.SQLITE_OK
		})
		return nil
	})
	_ = connection.Close()
	if err != nil {
		t.Fatal(err)
	}
	type row struct {
		ID int `sqlx:"id"`
	}
	type output struct{ Data []row }
	component := &spec.Component{Name: "AuthorizedRecords", RootView: &spec.View{Source: &spec.ViewSource{SQL: `SELECT id FROM protected_records ${predicate.Builder().CombineAnd($predicate.FilterGroup(0, "AND")).Build("WHERE")} ORDER BY id`}}, Parameters: []*spec.Parameter{{Name: "Data", Source: spec.BindSource{Kind: "output", Name: "view"}}}}
	for _, tc := range []struct {
		name, token      string
		declared, denied bool
	}{
		{"verified claims", sign("user-seven", time.Now().Add(time.Hour)), true, false},
		{"predicate denies subject", sign("other", time.Now().Add(time.Hour)), true, true},
		{"expired", sign("user-seven", time.Now().Add(-time.Hour)), true, true},
		{"malformed credential", "Bearer invalid.token.signature", true, true},
		{"missing credential", "", true, true},
		{"no JWT input means no implicit authorization", "Bearer invalid.token.signature", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inputType := reflect.TypeFor[jwtAuthorizationInput]()
			if !tc.declared {
				inputType = reflect.TypeFor[struct{}]()
			}
			artifact, err := buildArtifact(bootstrap.ArtifactInput{Component: component, InputType: inputType, OutputType: reflect.TypeFor[output](), DirectViewField: "Data", CodecFactory: factory, Types: testTypeCatalog(t, map[string]reflect.Type{"example.JWTAuthorization": reflect.TypeFor[jwtAuthorizationPredicate]()})})
			if err != nil {
				t.Fatal(err)
			}
			reads.Store(0)
			result, err := NewService().Read(ctx, &Session{Component: artifact.Component, Input: routeInput(t, artifact), OutputType: reflect.TypeFor[output](), Artifact: artifact.Reader, SQL: &dsql.SQLComponent{DB: db.DB}, Scope: testharness.Request{}.WithHeaders(http.Header{"Authorization": {tc.token}})})
			if tc.denied {
				if err == nil || reads.Load() != 0 {
					t.Fatalf("denied invocation: error=%v protected reads=%d", err, reads.Load())
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			want := []row{{1}}
			if !tc.declared {
				want = append(want, row{2})
			}
			if reads.Load() == 0 || !reflect.DeepEqual(result.(*output).Data, want) {
				t.Fatalf("rows=%+v want=%+v protected reads=%d", result, want, reads.Load())
			}
		})
	}
}
