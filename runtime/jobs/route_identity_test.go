package jobs

import (
	"context"
	"testing"
	"time"

	"github.com/viant/datly/internal/testharness/sqlite"
	xasync "github.com/viant/xdatly/async"
)

func TestJobMatchUsesRouteInstanceNotQueryControlsSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	store, err := NewSQLStore(ctx, SQLConfig{DB: h.DB})
	if err != nil {
		t.Fatal(err)
	}
	created := time.Now().UTC().Truncate(time.Microsecond)
	stored := &Record{Job: xasync.Job{ID: "kept", MatchKey: "rows/one", Status: xasync.StatusPending,
		Request: xasync.Request{Method: "GET", URI: "/rows/a%2Fb?tenant=1&key=one"}, CreationTime: created}}
	if err := store.Create(ctx, stored); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		uri    string
		method string
		match  bool
	}{
		{"/rows/a%2Fb?tenant=1&key=one&sync=true", "GET", true},
		{"/rows/a%2Fb?tenant=2&key=one", "GET", true}, // The reader's durable SQL guard, not a new job, must reject changed tenant.
		{"/rows/a/b?tenant=1&key=one", "GET", false},
		{"/rows/a%252Fb?tenant=1&key=one", "GET", false},
		{"/rows/other?tenant=1&key=one", "GET", false},
		{"/rows/a%2Fb?tenant=1&key=one", "PATCH", false},
	} {
		owner := &Record{Job: xasync.Job{MatchKey: stored.MatchKey, Request: xasync.Request{Method: tc.method, URI: tc.uri}}}
		got, err := store.match(ctx, owner, time.Hour, time.Minute)
		if err != nil || (got != nil) != tc.match {
			t.Fatalf("%s %s: got=%v err=%v want match=%v", tc.method, tc.uri, got, err, tc.match)
		}
		if got != nil && (got.ID != stored.ID || got.URI != stored.URI) {
			t.Fatal("matching rewrote durable execution identity")
		}
	}
}

func TestRouteIdentityRetainsEscapesAndAuthority(t *testing.T) {
	for _, tc := range []struct{ raw, want string }{
		{"/rows/a%2Fb?sync=true", "/rows/a%2Fb"},
		{"/rows/a%252Fb?tenant=2", "/rows/a%252Fb"},
		{"/rows/a/b?", "/rows/a/b"},
		{"https://example.test/rows/7?key=one", "https://example.test/rows/7"},
	} {
		got, err := routeURI(tc.raw)
		if err != nil || got != tc.want {
			t.Fatalf("%q: %q %v, want %q", tc.raw, got, err, tc.want)
		}
	}
	if _, err := routeURI("/rows/%zz"); err == nil {
		t.Fatal("malformed route identity accepted")
	}
}

func TestRouteMatchAmbiguityOnlyWithinTheSelectedInstanceSQLite(t *testing.T) {
	ctx := context.Background()
	h := sqlite.New(t)
	store, err := NewSQLStore(ctx, SQLConfig{DB: h.DB})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	for _, tc := range []struct{ id, uri string }{{"one", "/rows/1?key=shared"}, {"two", "/rows/2?key=shared"}} {
		if err := store.Create(ctx, &Record{Job: xasync.Job{ID: tc.id, MatchKey: "shared", Status: xasync.StatusPending,
			Request: xasync.Request{Method: "GET", URI: tc.uri}, CreationTime: now}}); err != nil {
			t.Fatal(err)
		}
	}
	owner := &Record{Job: xasync.Job{MatchKey: "shared", Request: xasync.Request{Method: "GET", URI: "/rows/1?key=shared&sync=true"}}}
	got, err := store.match(ctx, owner, time.Hour, time.Minute)
	if err != nil || got == nil || got.ID != "one" {
		t.Fatalf("other route created false ambiguity: %v %v", got, err)
	}
	if err := store.Create(ctx, &Record{Job: xasync.Job{ID: "duplicate", MatchKey: "shared", Status: xasync.StatusPending,
		Request: xasync.Request{Method: "GET", URI: "/rows/1?key=shared&tenant=2"}, CreationTime: now}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.match(ctx, owner, time.Hour, time.Minute); err == nil {
		t.Fatal("same-route same-time durable ambiguity was hidden")
	}
}
