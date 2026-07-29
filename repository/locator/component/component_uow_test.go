package component

import (
	"context"
	"database/sql"
	"net/http"
	"net/url"
	"reflect"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/service/executor/uow"
	"github.com/viant/datly/view/state/kind/locator"
)

type componentTestOperation string

func (o componentTestOperation) TableName() string { return string(o) }

type componentScopeDispatcher struct {
	db    *sql.DB
	order *[]string
}

func (d *componentScopeDispatcher) Dispatch(ctx context.Context, path *contract.Path, _ ...contract.Option) (interface{}, error) {
	ctx, _, frame, _, err := uow.Enter(ctx, path.Method+" "+path.URI)
	if err != nil {
		return nil, err
	}
	defer frame.Seal()
	buffer := frame.NewBuffer(func(context.Context) (*sql.DB, error) { return d.db, nil }, nil,
		func(_ context.Context, _ *sql.Tx, value any) error {
			*d.order = append(*d.order, string(value.(componentTestOperation)))
			return nil
		})
	if err = buffer.Append(componentTestOperation(path.URI)); err != nil {
		return nil, err
	}
	return struct{}{}, nil
}

type componentQueryDispatcher struct {
	query   url.Values
	request *http.Request
}

func (d *componentQueryDispatcher) Dispatch(_ context.Context, _ *contract.Path, opts ...contract.Option) (interface{}, error) {
	options := contract.NewOptions(opts...)
	d.query = options.Query
	d.request = options.Request
	return struct{}{}, nil
}

func TestComponentLocatorCreatesOrderedBindingFrames(t *testing.T) {
	db, _ := sql.Open("sqlite3", ":memory:")
	defer db.Close()
	ctx, scope, root := uow.NewRoot(context.Background(), "root")
	var order []string
	dispatcher := &componentScopeDispatcher{db: db, order: &order}
	request, _ := http.NewRequest(http.MethodGet, "/", nil)
	componentLocator := &componentLocator{
		dispatch: dispatcher,
		getRequest: func() (*http.Request, error) {
			return request, nil
		},
	}
	for _, binding := range []struct {
		order string
		name  string
	}{{"00000001", "GET:/second"}, {"00000000", "GET:/first"}} {
		bindingCtx := uow.WithBindingOrder(ctx, binding.order)
		if _, found, err := componentLocator.Value(bindingCtx, reflect.TypeOf(""), binding.name); err != nil || !found {
			t.Fatalf("Value(%s) found=%v err=%v", binding.name, found, err)
		}
	}
	rootBuffer := root.NewBuffer(func(context.Context) (*sql.DB, error) { return db, nil }, nil,
		func(_ context.Context, _ *sql.Tx, value any) error {
			order = append(order, string(value.(componentTestOperation)))
			return nil
		})
	if err := rootBuffer.Append(componentTestOperation("root")); err != nil {
		t.Fatal(err)
	}
	root.Seal()
	if err := scope.Finish(ctx, nil); err != nil {
		t.Fatal(err)
	}
	want := []string{"root", "/first", "/second"}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("order=%v want=%v", order, want)
	}
}

func TestComponentLocatorDropsSelectorQueryParamsForChildDispatch(t *testing.T) {
	dispatcher := &componentQueryDispatcher{}
	request, _ := http.NewRequest(http.MethodGet, "/?_fields=AudienceId&_orderby=AudienceId&_limit=10&_offset=5&_page=2&_criteria=AudienceId+%3D+1&criteria=business+criteria&audience_id=123&order_id=456&from=2026-07-01&to=2026-07-02", nil)
	query := url.Values{
		"_fields":     {"AudienceId"},
		"_orderby":    {"AudienceId"},
		"_limit":      {"10"},
		"_offset":     {"5"},
		"_page":       {"2"},
		"_criteria":   {"AudienceId = 1"},
		"criteria":    {"business criteria"},
		"audience_id": {"123"},
		"order_id":    {"456"},
		"from":        {"2026-07-01"},
		"to":          {"2026-07-02"},
	}
	componentLocator := &componentLocator{
		dispatch: dispatcher,
		query:    query,
		getRequest: func() (*http.Request, error) {
			return request, nil
		},
	}

	_, found, err := componentLocator.Value(context.Background(), reflect.TypeOf(""), "GET:/child")
	if err != nil || !found {
		t.Fatalf("Value() found=%v err=%v", found, err)
	}

	for _, key := range []string{"_fields", "_orderby", "_limit", "_offset", "_page", "_criteria"} {
		if _, ok := dispatcher.query[key]; ok {
			t.Fatalf("selector query key %q was forwarded: %v", key, dispatcher.query)
		}
	}
	for key, want := range map[string]string{
		"criteria":    "business criteria",
		"audience_id": "123",
		"order_id":    "456",
		"from":        "2026-07-01",
		"to":          "2026-07-02",
	} {
		if got := dispatcher.query.Get(key); got != want {
			t.Fatalf("query[%s]=%q want %q; query=%v", key, got, want, dispatcher.query)
		}
	}
	if dispatcher.request == nil || dispatcher.request.URL == nil {
		t.Fatal("expected forwarded request")
	}
	requestQuery := dispatcher.request.URL.Query()
	for _, key := range []string{"_fields", "_orderby", "_limit", "_offset", "_page", "_criteria"} {
		if _, ok := requestQuery[key]; ok {
			t.Fatalf("selector query key %q was forwarded on request URL: %s", key, dispatcher.request.URL.RawQuery)
		}
	}
	for key, want := range map[string]string{
		"criteria":    "business criteria",
		"audience_id": "123",
		"order_id":    "456",
		"from":        "2026-07-01",
		"to":          "2026-07-02",
	} {
		if got := requestQuery.Get(key); got != want {
			t.Fatalf("request query[%s]=%q want %q; raw=%s", key, got, want, dispatcher.request.URL.RawQuery)
		}
	}
	if request.URL.Query().Get("_fields") != "AudienceId" {
		t.Fatal("original parent request was mutated")
	}
}

func TestSanitizeSelectorQueryClonesForwardedValues(t *testing.T) {
	query := url.Values{"_fields": {"AudienceId"}, "order_id": {"456"}}
	sanitized := sanitizeSelectorQuery(query)

	query.Set("order_id", "mutated")

	if got, want := sanitized.Get("order_id"), "456"; got != want {
		t.Fatalf("sanitized query was not cloned, got %q want %q", got, want)
	}
}

func TestSanitizeSelectorRequestClonesForwardedRequest(t *testing.T) {
	request, _ := http.NewRequest(http.MethodGet, "/?_fields=AudienceId&order_id=456", nil)

	sanitized := sanitizeSelectorRequest(request)

	if sanitized == request {
		t.Fatal("expected sanitized request clone")
	}
	if got := sanitized.URL.Query().Get("_fields"); got != "" {
		t.Fatalf("sanitized request still has _fields=%q", got)
	}
	if got, want := sanitized.URL.Query().Get("order_id"), "456"; got != want {
		t.Fatalf("sanitized request order_id=%q want %q", got, want)
	}
	if got, want := request.URL.Query().Get("_fields"), "AudienceId"; got != want {
		t.Fatalf("original request was mutated, _fields=%q want %q", got, want)
	}
}

func TestSanitizeSelectorRequestDoesNotReencodeWhenNoSelectorParams(t *testing.T) {
	request, _ := http.NewRequest(http.MethodGet, "/?b=two%20words&a=1", nil)
	originalRawQuery := request.URL.RawQuery

	sanitized := sanitizeSelectorRequest(request)

	if sanitized != request {
		t.Fatal("expected original request when no selector params are present")
	}
	if sanitized.URL.RawQuery != originalRawQuery {
		t.Fatalf("raw query changed, got %q want %q", sanitized.URL.RawQuery, originalRawQuery)
	}
}

func TestComponentLocatorRequiresInvocationDispatcher(t *testing.T) {
	if _, err := newComponentLocator(locator.WithConstants(nil)); err == nil {
		t.Fatal("expected missing dispatcher error")
	}
}
