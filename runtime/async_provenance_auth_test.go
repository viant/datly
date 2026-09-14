package runtime

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	jwtv5 "github.com/golang-jwt/jwt/v5"
	"github.com/viant/datly/bootstrap"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/internal/testharness/sqlite"
	"github.com/viant/datly/runtime/auth"
	rhandler "github.com/viant/datly/runtime/handler"
	"github.com/viant/datly/runtime/jobs"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	dsql "github.com/viant/datly/sql"
	sqldml "github.com/viant/datly/sql/dml"
	"github.com/viant/scy"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/scy/auth/jwt/verifier"
	xasync "github.com/viant/xdatly/async"
	xhandler "github.com/viant/xdatly/handler"
	"reflect"
	"strings"
	"testing"
	"time"
)

type asyncJWTInput struct {
	JWT   *jwt.Claims
	Token string
	ID    int
}

func TestAsyncReplayReverifiesDeclaredJWTSourceSQLite(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pub, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	factory, err := auth.New(context.Background(), &auth.Config{JWTValidator: &verifier.Config{RSA: []*scy.Resource{{URL: "async-test-key", Data: pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pub})}}}})
	if err != nil {
		t.Fatal(err)
	}
	sign := func(expiry time.Time) string {
		token := jwtv5.NewWithClaims(jwtv5.SigningMethodRS256, jwtv5.MapClaims{"sub": "approved", "exp": expiry.Unix()})
		value, err := token.SignedString(key)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	valid := sign(time.Now().Add(time.Hour))
	expired := sign(time.Now().Add(-time.Hour))
	for _, mode := range []string{"valid", "expired before replay", "stored claims", "denied authorizer", "param valid", "param stored claims", "param altered source", "altered claims"} {
		t.Run(mode, func(t *testing.T) {
			derived := strings.HasPrefix(mode, "param ")
			mode = strings.TrimPrefix(mode, "param ")
			sourceName := "JWT"
			if derived {
				sourceName = "Token"
			}
			ctx := context.Background()
			db := testharness.NewSQLiteHarness(t)
			if err := db.ExecStatements(ctx, sqlite.DatlyJobsSchema, "CREATE TABLE audit(ID INTEGER)"); err != nil {
				t.Fatal(err)
			}
			required := true
			component := &spec.Component{Key: spec.Key{Kind: spec.KindComponent, Name: "JWTJob"}, Routes: []*spec.Route{{Method: "POST", Path: "/jwtjob"}}, Parameters: []*spec.Parameter{{Name: "JWT", TypeExpr: "string", Source: spec.BindSource{Kind: "header", Name: "Authorization"}, Codec: &spec.Codec{Body: auth.JwtClaim}, Required: &required, ErrorStatusCode: 401, ErrorMessage: "invalid authentication"}, {Name: "ID", Source: spec.BindSource{Kind: "query", Name: "id"}}}}
			if derived {
				component.Parameters[0].Source = spec.BindSource{Kind: "param", Name: "Token"}
				component.Parameters[0].TypeExpr = ""
				component.Parameters = append([]*spec.Parameter{{Name: "Token", Source: spec.BindSource{Kind: "header", Name: "Authorization"}, Required: &required}}, component.Parameters...)
			}
			artifact, err := bootstrap.BuildArtifact(bootstrap.ArtifactInput{Component: component, InputType: reflect.TypeOf(asyncJWTInput{}), OutputType: reflect.TypeOf(struct{}{}), CodecFactory: factory})
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			handler := rhandler.HandlerFunc(func(ctx context.Context, inv rhandler.Invocation) (any, error) {
				calls++
				input := inv.Input.(*asyncJWTInput)
				if input.JWT == nil || input.JWT.Subject != "approved" {
					return nil, errors.New("unverified input reached handler")
				}
				data, _, err := inv.Binder.Lookup(ctx, xhandler.DataKey)
				if err != nil {
					return nil, err
				}
				return nil, data.(xhandler.Data).Execute("INSERT INTO audit VALUES(?)", input.ID)
			})
			rt, err := NewRuntime([]*registry.RegisteredComponent{{Component: artifact.Component, Input: artifact.Input, OutputType: reflect.TypeOf(struct{}{}), Handler: handler, DataSource: sqldml.Source{DB: db.DB}}})
			if err != nil {
				t.Fatal(err)
			}
			store, err := (bootstrap.JobStoreConfig{SQL: &dsql.SQLComponent{DB: db.DB}}).NewStore(ctx)
			if err != nil {
				t.Fatal(err)
			}
			service, err := rt.NewAsyncService(jobs.Config{Store: store, Authorize: func(_ context.Context, access jobs.Access) error {
				if access.Input != nil {
					input := access.Input.(*asyncJWTInput)
					if input.JWT == nil || input.JWT.Subject != "approved" {
						t.Fatal("authorizer did not receive verified claims")
					}
				}
				if access.Action == jobs.Replay && mode == "altered source" {
					access.Input.(*asyncJWTInput).Token = expired
				}
				if access.Action == jobs.Replay && mode == "altered claims" {
					access.Input.(*asyncJWTInput).JWT.Subject = "forged"
				}
				if access.Action == jobs.Replay && mode == "denied authorizer" {
					return errors.New("denied")
				}
				return nil
			}})
			if err != nil {
				t.Fatal(err)
			}
			raw, _ := json.Marshal(map[string]any{sourceName: valid, "ID": 7})
			scheduled, err := service.Schedule(ctx, jobs.Submission{Job: xasync.Job{Request: xasync.Request{Method: "POST", URI: "/jwtjob"}}, SourceState: string(raw)})
			if err != nil {
				t.Fatal(err)
			}
			record, err := store.Get(ctx, scheduled.Job.ID)
			if err != nil {
				t.Fatal(err)
			}
			var persisted map[string]json.RawMessage
			if err := json.Unmarshal([]byte(record.State), &persisted); err != nil {
				t.Fatal(err)
			}
			var token string
			if err := json.Unmarshal(persisted[sourceName], &token); err != nil || token != valid {
				t.Fatal("decoded JWT claims were persisted as source")
			}
			if mode == "expired before replay" {
				raw, _ = json.Marshal(map[string]any{sourceName: expired, "ID": 7})
			}
			if mode == "stored claims" {
				raw, _ = json.Marshal(map[string]any{sourceName: map[string]any{"sub": "approved", "exp": 9999999999}, "ID": 7})
			}
			if mode == "expired before replay" || mode == "stored claims" {
				if _, err := db.DB.Exec("UPDATE DATLY_JOBS SET State=? WHERE ID=?", string(raw), record.ID); err != nil {
					t.Fatal(err)
				}
			}
			_, runErr := service.Run(ctx, record.ID)
			expected := 0
			if mode == "valid" {
				expected = 1
				if runErr != nil {
					t.Fatal(runErr)
				}
			} else if runErr == nil {
				t.Fatal("invalid replay was accepted")
			}
			var count int
			if err := db.DB.QueryRow("SELECT count(*) FROM audit").Scan(&count); err != nil || count != expected || calls != expected {
				t.Fatalf("writes=%d calls=%d err=%v", count, calls, err)
			}
			row, err := store.Get(ctx, record.ID)
			if err != nil {
				t.Fatal(err)
			}
			if mode != "valid" && row.Status != xasync.StatusPending {
				t.Fatalf("denial changed durable state: %s", row.Status)
			}
			if _, err := service.Schedule(ctx, jobs.Submission{Job: xasync.Job{Request: xasync.Request{Method: "POST", URI: "/jwtjob"}}, Input: &asyncJWTInput{JWT: &jwt.Claims{}, ID: 8}}); err == nil {
				t.Fatal("decoded codec value accepted without raw source")
			}
		})
	}
}
