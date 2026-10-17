package responsebinding

import (
	"context"
	"errors"
	legacy "github.com/viant/xdatly/handler/response"
	"reflect"
	"testing"
)

func TestProjectionTokenOwnerTargetAndOnce(t *testing.T) {
	owner := new(int)
	typ := reflect.TypeFor[int]()
	candidate := &legacy.Error{Code: 403, Message: "unavailable"}
	projector := func(*legacy.Error) (any, bool, error) { return new(int), true, nil }
	ctx, r := New(context.Background(), owner, "GET:/reader", typ, projector)
	if _, c := Claim(ctx, new(int), "GET:/reader", typ); c != nil {
		t.Fatal("foreign owner claimed")
	}
	if _, c := Claim(ctx, owner, "GET:/other", typ); c != nil {
		t.Fatal("foreign route claimed")
	}
	if _, c := Claim(ctx, owner, "GET:/reader", reflect.TypeFor[string]()); c != nil {
		t.Fatal("foreign output claimed")
	}
	child, c := Claim(ctx, owner, "GET:/reader", typ)
	if c != r {
		t.Fatal("not claimed")
	}
	if _, c = Claim(child, owner, "GET:/reader", typ); c != nil {
		t.Fatal("descendant borrowed token")
	}
	if _, c = Claim(ctx, owner, "GET:/reader", typ); c != nil {
		t.Fatal("reused token")
	}
	if _, ok, err := r.Project(owner, candidate, candidate, 1); err != nil || !ok {
		t.Fatalf("project %v %v", ok, err)
	}
	if _, ok, _ := r.Project(owner, candidate, candidate, 1); ok {
		t.Fatal("projected twice")
	}
	if r.Consume() != candidate || r.Consume() != nil {
		t.Fatal("consumed twice")
	}
	for _, mode := range []string{"identity", "calls", "private", "projection"} {
		t.Run(mode, func(t *testing.T) {
			failure := errors.New("projection failure")
			p := projector
			e := &legacy.Error{Code: 403, Message: "unavailable"}
			var raw error = e
			calls := 1
			switch mode {
			case "identity":
				raw = &legacy.Error{Code: 403, Message: e.Message}
			case "calls":
				calls = 2
			case "private":
				e.Err = failure
			case "projection":
				p = func(*legacy.Error) (any, bool, error) { return nil, false, failure }
			}
			ctx, r := New(context.Background(), owner, "GET:/reader", typ, p)
			_, _ = Claim(ctx, owner, "GET:/reader", typ)
			_, ok, err := r.Project(owner, raw, e, calls)
			if ok || r.Consume() != nil {
				t.Fatal("ineligible projected")
			}
			if mode == "projection" && (!errors.Is(err, e) || !errors.Is(err, failure)) {
				t.Fatalf("projection lost cause: %v", err)
			}
		})
	}
}

func TestProjectionTokenAbsentOwner(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		tokenOwner, incoming any
		want                 bool
	}{
		{"absent", nil, nil, true},
		{"unexpected owner", nil, new(int), false},
		{"missing owner", new(int), nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			typ := reflect.TypeFor[int]()
			candidate := &legacy.Error{Code: 403, Message: "unavailable"}
			ctx, token := New(context.Background(), tc.tokenOwner, "GET:/reader", typ, func(*legacy.Error) (any, bool, error) { return 1, true, nil })
			child, claimed := Claim(ctx, tc.incoming, "GET:/reader", typ)
			if (claimed != nil) != tc.want {
				t.Fatalf("claimed=%v want=%v", claimed != nil, tc.want)
			}
			if !tc.want {
				return
			}
			if _, next := Claim(child, nil, "GET:/reader", typ); next != nil {
				t.Fatal("descendant claimed")
			}
			if _, next := Claim(ctx, nil, "GET:/reader", typ); next != nil {
				t.Fatal("claimed twice")
			}
			if _, ok, err := token.Project(nil, candidate, candidate, 1); !ok || err != nil {
				t.Fatalf("projection %v %v", ok, err)
			}
			if token.Consume() != candidate || token.Consume() != nil {
				t.Fatal("consume once")
			}
		})
	}
}
