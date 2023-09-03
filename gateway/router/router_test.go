package router_test

/*

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"fmt"
	"github.com/golang-jwt/jwt/v4"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs"
	"github.com/viant/afs/option/content"
	"github.com/viant/assertly"
	"github.com/viant/datly/config"
	"github.com/viant/datly/gateway/warmup"
	"github.com/viant/datly/internal/tests"
	"github.com/viant/datly/reader"
	"github.com/viant/datly/gateway/router/openapi3"
	"github.com/viant/datly/utils/httputils"
	"github.com/viant/datly/view"
	_ "github.com/viant/sqlx/metadata/product/sqlite"
	"github.com/viant/xreflect"
	"google.golang.org/api/oauth2/v2"
	"gopkg.in/yaml.v3"
	"io"
	"io/ioutil"
	"math"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/viant/datly/gateway/router"
	"github.com/viant/toolbox"
	"net/http"
	"path"
	"testing"
)

type testcase struct {
	preWarmup           bool
	closeAfterPreWarmup map[string]bool
	description         string
	resourceURI         string
	uri                 string
	method              string
	useAssertPkg        bool
	expected            string
	codecs              config.CodecsRegistry
	types               *xreflect.Types
	headers             http.Header
	requestBody         string
	shouldDecompress    bool
	extraRequests       int
	envVariables        map[string]string

	corsHeaders                map[string]string
	dependenciesUrl            map[string]string
	afterInsertUri             string
	afterInsertMethod          string
	afterInsertExpected        string
	expectedHeaders            http.Header
	afterInsertExpectedHeaders http.Header
}

type (
	eventAfterFetcher  struct{}
	eventBeforeFetcher struct{}
	gcpMockDecoder     struct{}
	asStrings          struct{}
)

func (a *asStrings) ResultType(paramType reflect.Type) (reflect.Type, error) {
	return reflect.TypeOf([]string{}), nil
}

func (g *gcpMockDecoder) ResultType(paramType reflect.Type) (reflect.Type, error) {
	return reflect.TypeOf(&oauth2.Tokeninfo{}), nil
}

func (a *asStrings) Output(ctx context.Group, raw interface{}, options ...interface{}) (interface{}, error) {
	rawString, ok := asString(raw)
	if !ok {
		return "", fmt.Errorf("expected to got string but got %T", raw)
	}

	return strings.Split(rawString, " "), nil
}

func asString(raw interface{}) (string, bool) {
	rawString, ok := raw.(string)
	if ok {
		return rawString, true
	}

	strPtr, ok := raw.(*string)
	if ok {
		if strPtr != nil {
			return *strPtr, true
		}
		return "", true
	}

	return "", false
}

func (e *eventBeforeFetcher) Output(ctx context.Group, raw interface{}, options ...interface{}) (interface{}, error) {
	return nil, nil
}

func (e *eventAfterFetcher) Output(ctx context.Group, raw interface{}, options ...interface{}) (interface{}, error) {
	return nil, nil
}

func (g *gcpMockDecoder) Output(_ context.Group, raw interface{}, _ ...interface{}) (interface{}, error) {
	rawString, ok := raw.(string)
	if !ok {
		return nil, fmt.Errorf("expected to get string but got %T", raw)
	}

	tokenType := "Bearer "
	if index := strings.Views(rawString, tokenType); index != -1 {
		rawString = rawString[index+len(tokenType):]
		decoded, err := base64.URLEncoding.DecodeString(rawString)
		if err != nil {
			return nil, err
		}

		claims := jwt.MapClaims{}
		_, _ = jwt.ParseWithClaims(string(decoded), claims, func(token *jwt.Token) (interface{}, error) {
			return nil, nil
		})

		email := claims["Email"]
		if emailAsString, ok := email.(string); ok {
			return &oauth2.Tokeninfo{
				Email: emailAsString,
			}, nil
		}

		return &oauth2.Tokeninfo{}, err
	}

	return nil, fmt.Errorf("unsupported token type")
}

func (e *eventBeforeFetcher) BeforeFetch(response http.ResponseWriter, request *http.Request) error {
	response.WriteHeader(http.StatusBadRequest)
	response.Write([]byte("[]"))
	return nil
}

func (e *eventAfterFetcher) AfterFetch(data interface{}, response http.ResponseWriter, request *http.Request) error {
	if _, ok := data.(*[]*event); !ok {
		response.WriteHeader(http.StatusInternalServerError)
		response.Write([]byte("unexpected data type"))
		return nil
	}

	events := data.(*[]*event)
	for _, ev := range *events {
		ev.Quantity = math.Round(ev.Quantity)
	}

	return nil
}

type event struct {
	Id        int
	Quantity  float64
	Timestamp time.Time
}

// TODO: add testcases against sql injection
func TestRouter(t *testing.T) {
	view.PingTimeInS = 1000
	reader.Dif = func(t1, t2 time.Time) time.Duration {
		return time.Duration(5) * time.Millisecond
	}

	reader.Now = func() time.Time {
		parse, _ := time.Parse("2006-01-02 15:04:05.000000000 -0700 MST", "2014-11-12 11:45:26.000000000 +0000 UTC")
		return parse
	}

	defer func() {
		view.ResetConnectionConfig()
		reader.Dif = func(t1, t2 time.Time) time.Duration {
			return t1.Sub(t2)
		}
		reader.Now = time.Now
	}()

	//testLocation := toolbox.CallerDirectory(3)
	testLocation := "./"
	_ = toolbox.CreateDirIfNotExist(path.Join(testLocation, "testdata/db"))

	type FooParam struct {
		QUANTITY float64
		USER_ID  int
	}

	type params struct {
		FOO           *FooParam
		EVENT_TYPE_ID int
	}

	type FooPresence struct {
		QUANTITY bool
		USER_ID  bool
	}

	type presenceParams struct {
		FOO           *FooPresence
		EVENT_TYPE_ID bool
	}

	testcases := []*testcase{
		{
			description: "regular http",
			resourceURI: "001_get",
			uri:         "/api/events",
			method:      http.MethodGet,
			expected:    `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","EventTypeId":2,"Quantity":33.23432374000549,"UserId":1},{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","EventTypeId":11,"Quantity":21.957962334156036,"UserId":2},{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3}]`,
		},
		{
			description: "selectors | fields, offset, limit",
			resourceURI: "002_selectors",
			uri:         fmt.Sprintf("/api/events?ev%v=Id,Quantity&ev%v=1&ev%v=2", view.FieldsQuery, view.OffsetQuery, view.LimitQuery),
			expected:    `[{"Id":10,"Quantity":21.957962334156036},{"Id":100,"Quantity":5.084940046072006}]`,
			method:      http.MethodGet,
		},
		{
			description: "selectors | orderBy, offset, limit",
			resourceURI: "002_selectors",
			uri:         fmt.Sprintf("/api/events?ev%v=Quantity&ev%v=1&ev%v=3", view.OrderByQuery, view.OffsetQuery, view.LimitQuery),
			expected:    `[{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","EventTypeId":11,"Quantity":21.957962334156036,"UserId":2},{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","EventTypeId":2,"Quantity":33.23432374000549,"UserId":1}]`,
			method:      http.MethodGet,
		},
		{
			description: "selectors | orderBy, criteria",
			resourceURI: "002_selectors",
			uri:         fmt.Sprintf("/api/events?%v=Id&ev%v=(ID%%20in%%20(1,100))", view.OrderByQuery, view.CriteriaQuery),
			expected:    `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","EventTypeId":2,"Quantity":33.23432374000549,"UserId":1},{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3}]`,
			method:      http.MethodGet,
		},
		{
			description: "return single | found",
			resourceURI: "003_route_config",
			uri:         "/api/events/1",
			expected:    `{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","EventTypeId":2,"Quantity":33.23432374000549,"UserId":1}`,
			method:      http.MethodGet,
		},
		{
			description: "return single | not found",
			resourceURI: "003_route_config",
			uri:         "/api/events/3",
			method:      http.MethodGet,
			expected:    `{"Status":"error"}`,
		},
		{
			description: "codecs | AfterFetcher",
			resourceURI: "004_visitors",
			uri:         "/api/events",
			codecs: config.NewCodecs(
				config.NewVisitor("event_visitor", &eventAfterFetcher{}),
			),
			types: xreflect.NewTypes(xreflect.WithTypes(
				xreflect.NewType("event", xreflect.WithReflectType(reflect.TypeOf(&event{}))),
			)),
			expected: `[{"Id":1,"Quantity":33,"Timestamp":"2019-03-11T02:20:33Z"},{"Id":10,"Quantity":22,"Timestamp":"2019-03-15T12:07:33Z"},{"Id":100,"Quantity":5,"Timestamp":"2019-04-10T05:15:33Z"}]`,
			method:   http.MethodGet,
		},
		{
			description: "codecs | BeforeFetcher",
			resourceURI: "004_visitors",
			uri:         "/api/events",
			codecs: config.NewCodecs(
				config.NewVisitor("event_visitor", &eventBeforeFetcher{}),
			),
			types: xreflect.NewTypes(xreflect.WithTypes(
				xreflect.NewType("event", xreflect.WithReflectType(reflect.TypeOf(&event{}))),
			)),
			expected: `[]`,
			method:   http.MethodGet,
		},
		{
			description: "templates | all values set",
			resourceURI: "004_visitors",
			uri:         "/api/events",
			types: xreflect.NewTypes(xreflect.WithTypes(
				xreflect.NewType("event", xreflect.WithReflectType(reflect.TypeOf(&event{}))),
			)),
			codecs: config.NewCodecs(
				config.NewVisitor("event_visitor", &eventBeforeFetcher{}),
			),
			expected: `[]`,
			method:   http.MethodGet,
		},
		{
			description: "templates | none value set",
			resourceURI: "005_templates",
			uri:         "/api/events",
			codecs: config.NewCodecs(
				config.NewVisitor("event_visitor", &eventBeforeFetcher{}),
			),
			types: xreflect.NewTypes(xreflect.WithTypes(
				xreflect.NewType("event", xreflect.WithReflectType(reflect.TypeOf(&event{}))),
			)),
			expected: `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","EventTypeId":2,"Quantity":33.23432374000549,"UserId":1},{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","EventTypeId":11,"Quantity":21.957962334156036,"UserId":2},{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3}]`,
			method:   http.MethodGet,
		},
		{
			description: "templates | user_id",
			resourceURI: "005_templates",
			uri:         "/api/events?user_id=1",
			codecs: config.NewCodecs(
				config.NewVisitor("event_visitor", &eventBeforeFetcher{}),
			),
			types: xreflect.NewTypes(xreflect.WithTypes(
				xreflect.NewType("event", xreflect.WithReflectType(reflect.TypeOf(&event{}))),
			)),
			expected: `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","EventTypeId":2,"Quantity":33.23432374000549,"UserId":1}]`,
			method:   http.MethodGet,
		},
		{
			description: "templates | quantity",
			resourceURI: "005_templates",
			uri:         "/api/events?quantity=10",
			codecs: config.NewCodecs(
				config.NewVisitor("event_visitor", &eventBeforeFetcher{}),
			),
			types: xreflect.NewTypes(xreflect.WithTypes(
				xreflect.NewType("event", xreflect.WithReflectType(reflect.TypeOf(&event{}))),
			)),
			expected: `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","EventTypeId":2,"Quantity":33.23432374000549,"UserId":1},{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","EventTypeId":11,"Quantity":21.957962334156036,"UserId":2}]`,
			method:   http.MethodGet,
		},
		{
			description: "param path | all set",
			resourceURI: "006_param_path",
			uri:         "/api/events?quantity=10&event_type_id=2&user_id=1",
			types: xreflect.NewTypes(xreflect.WithTypes(
				xreflect.NewType("event", xreflect.WithReflectType(reflect.TypeOf(&event{}))),
			)),
			expected: `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","EventTypeId":2,"Quantity":33.23432374000549,"UserId":1}]`,
			method:   http.MethodGet,
		},
		{
			description: "param path | user_id",
			resourceURI: "006_param_path",
			uri:         "/api/events?user_id=3",
			types: xreflect.NewTypes(xreflect.WithTypes(
				xreflect.NewType("event", xreflect.WithReflectType(reflect.TypeOf(&event{}))),
			)),
			expected: `[{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3}]`,
			method:   http.MethodGet,
		},
		{
			description: "param path typed | user_id, non-pointers",
			resourceURI: "007_param_path_typed",
			uri:         "/api/events?user_id=3",
			types: xreflect.NewTypes(xreflect.WithTypes(
				xreflect.NewType("event", xreflect.WithReflectType(reflect.TypeOf(&event{}))),
				xreflect.NewType("params", xreflect.WithReflectType(reflect.TypeOf(params{}))),
				xreflect.NewType("presence_params", xreflect.WithReflectType(reflect.TypeOf(presenceParams{}))),
			)),
			expected: `[{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3}]`,
			method:   http.MethodGet,
		},
		{
			description: "param path typed | user_id, pointers",
			resourceURI: "007_param_path_typed",
			uri:         "/api/events?user_id=3",
			types: xreflect.NewTypes(xreflect.WithTypes(
				xreflect.NewType("event", xreflect.WithReflectType(reflect.TypeOf(&event{}))),
				xreflect.NewType("params", xreflect.WithReflectType(reflect.TypeOf(params{}))),
				xreflect.NewType("presence_params", xreflect.WithReflectType(reflect.TypeOf(presenceParams{}))),
			)),
			expected: `[{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3}]`,
			method:   http.MethodGet,
		},
		{
			description: "view acl | role leader",
			resourceURI: "008_acl",
			uri:         "/api/employees",
			method:      http.MethodGet,
			expected:    `[{"Id":1,"DepId":1,"Email":"abc@example.com"},{"Id":3,"DepId":1,"Email":"tom@example.com"}]`,
			headers: map[string][]string{
				//ID: 1, Email: abc@example.com
				"Authorization": {"Bearer " + encodeToken("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJJZCI6MSwiRW1haWwiOiJhYmNAZXhhbXBsZS5jb20ifQ.dm3jSSuqy9wf4BsjU1dElRQQEySC5nn6fCUTmTKqt2")},
			},
			codecs: config.NewCodecs(
				config.NewCodec(config.CodecKeyJwtClaim, &gcpMockDecoder{}, reflect.TypeOf(&oauth2.Tokeninfo{})),
			),
			types: xreflect.NewTypes(xreflect.WithTypes(
				xreflect.NewType(config.TypeJwtTokenInfo, xreflect.WithReflectType(reflect.TypeOf(&oauth2.Tokeninfo{}))),
			)),
		},
		{
			description: "view acl | role engineer",
			resourceURI: "008_acl",
			uri:         "/api/employees",
			method:      http.MethodGet,
			expected:    `[{"Id":2,"DepId":2,"Email":"example@gmail.com"}]`,
			headers: map[string][]string{
				//ID: 1
				"Authorization": {"Bearer " + encodeToken("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJJZCI6MiwiRW1haWwiOiJleGFtcGxlQGdtYWlsLmNvbSJ9.XsZ115KqQK8uQE9for6NaphYS1VHdJc_famKWHo1Dcw")},
			},
			codecs: config.NewCodecs(
				config.NewCodec(config.CodecKeyJwtClaim, &gcpMockDecoder{}, reflect.TypeOf(&oauth2.Tokeninfo{})),
			),
			types: xreflect.NewTypes(xreflect.WithTypes(
				xreflect.NewType(config.TypeJwtTokenInfo, xreflect.WithReflectType(reflect.TypeOf(&oauth2.Tokeninfo{}))),
			)),
		},
		{
			description: "view acl | user acl",
			resourceURI: "008_acl",
			uri:         "/api/employees",
			method:      http.MethodGet,
			expected:    `[{"Id":1,"DepId":1,"Email":"abc@example.com"},{"Id":2,"DepId":2,"Email":"example@gmail.com"},{"Id":3,"DepId":1,"Email":"tom@example.com"},{"Id":4,"DepId":2,"Email":"Ann@example.com"}]`,
			headers: map[string][]string{
				//ID: 4
				"Authorization": {"Bearer " + encodeToken("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJFbWFpbCI6IkFubkBleGFtcGxlLmNvbSIsIklkIjo0fQ.9A0LWtsh_tskG-hLBFVNj7PNRQE8qWc5ZioqLWPS1gQ")},
			},
			codecs: config.NewCodecs(
				config.NewCodec(config.CodecKeyJwtClaim, &gcpMockDecoder{}, reflect.TypeOf(&oauth2.Tokeninfo{})),
			),
			types: xreflect.NewTypes(xreflect.WithTypes(
				xreflect.NewType(config.TypeJwtTokenInfo, xreflect.WithReflectType(reflect.TypeOf(&oauth2.Tokeninfo{}))),
			)),
		},
		{
			description: "CORS",
			resourceURI: "009_cors",
			uri:         "/api/events",
			method:      http.MethodGet,
			expected:    `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","EventTypeId":2,"Quantity":33.23432374000549,"UserId":1},{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","EventTypeId":11,"Quantity":21.957962334156036,"UserId":2},{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3}]`,
			corsHeaders: map[string]string{
				httputils.AllowCredentialsHeader: "true",
				httputils.AllowHeadersHeader:     "Header-1, Header-2",
				httputils.AllowOriginHeader:      "*",
				httputils.ExposeHeadersHeader:    "Header-Exposed-1, Header-Exposed-2",
				httputils.MaxAgeHeader:           "10500",
				httputils.AllowMethodsHeader:     "OPTIONS",
			},
		},
		{
			description: "relations | with specified fields",
			resourceURI: "010_relations",
			uri:         fmt.Sprintf("/api/events?ev%v=Id,Quantity,EventType&typ%v=Id,Code&ev%v=1&ev%v=2", view.FieldsQuery, view.FieldsQuery, view.OffsetQuery, view.LimitQuery),
			method:      http.MethodGet,
			expected:    `[{"Id":10,"Quantity":21.957962334156036,"EventType":{"Id":11,"Code":"code - 11"}},{"Id":100,"Quantity":5.084940046072006,"EventType":{"Id":111,"Code":"code - 111"}}]`,
		},
		{
			description: "relations | with specified fields, without relation Id",
			resourceURI: "010_relations",
			expected:    `[{"Id":10,"Quantity":21.957962334156036,"EventType":{"Code":"code - 11"}},{"Id":100,"Quantity":5.084940046072006,"EventType":{"Code":"code - 111"}}]`,
			uri:         fmt.Sprintf("/api/events?ev%v=Id,Quantity,EventType&typ%v=Code&ev%v=1&ev%v=2", view.FieldsQuery, view.FieldsQuery, view.OffsetQuery, view.LimitQuery),
			method:      http.MethodGet,
		},
		{
			description: "relations | with specified fields, without relation",
			resourceURI: "010_relations",
			expected:    `[{"Id":10,"Quantity":21.957962334156036},{"Id":100,"Quantity":5.084940046072006}]`,
			uri:         fmt.Sprintf("/api/events?ev%v=Id,Quantity&ev%v=1&ev%v=2", view.FieldsQuery, view.OffsetQuery, view.LimitQuery),
			method:      http.MethodGet,
		},
		{
			description: "styles | error",
			resourceURI: "011_style",
			uri:         "/api/events?_criteria=(id%20=%201%20UNION%20ALL%20SELECT%209%20as%20id%2C%20To_Date%28%222019-03-11T02%3A20%3A33Z%22%29%20as%20timestamp%2C%2010%20as%20event_type_id%2C%2020%20as%20quantity%2C%206%20as%20user_id)",
			expected:    `{"Status":"error","Errors":[{"View":"events","Parameter":"_criteria","Message":"can't use criteria on view events"}],"DataPtr":[]}`,
			method:      http.MethodGet,
		},
		{
			description: "styles | response",
			resourceURI: "011_style",
			uri:         "/api/events?_fields=Id,Timestamp,EventTypeId",
			expected:    `{"Status":"ok","DataPtr":[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","EventTypeId":2},{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","EventTypeId":11},{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111}]}`,
			method:      http.MethodGet,
		},
		{
			description: "default | default tag",
			resourceURI: "012_default",
			uri:         "/api/events",
			expected:    `[{"Id":1,"Quantity":33.23432374000549,"Timestamp":"2019-03-11"},{"Id":10,"Quantity":21.957962334156036,"Timestamp":"2019-03-15"},{"Id":100,"Quantity":10.5,"Timestamp":"2019-04-10"}]`,
			method:      http.MethodGet,
		},
		{
			description:      "reader post | request body param",
			resourceURI:      "013_reader_post",
			uri:              "/api/events",
			expected:         `[{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","EventTypeId":11,"Quantity":21.957962334156036,"UserId":2}]`,
			method:           http.MethodPost,
			requestBody:      `{"UserId":2,"Id":10}`,
			shouldDecompress: true,
		},
		{
			description:      "reader post | compressed",
			resourceURI:      "013_reader_post",
			uri:              "/api/events",
			expected:         `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","EventTypeId":2,"Quantity":33.23432374000549,"UserId":1},{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","EventTypeId":11,"Quantity":21.957962334156036,"UserId":2},{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3}]`,
			method:           http.MethodPost,
			shouldDecompress: true,
		},
		{
			description:   "cache | fields, offset, limit",
			resourceURI:   "014_cache",
			uri:           fmt.Sprintf("/api/events?ev%v=Id,Quantity&ev%v=1&ev%v=2", view.FieldsQuery, view.OffsetQuery, view.LimitQuery),
			expected:      `[{"Id":10,"Quantity":21.957962334156036},{"Id":100,"Quantity":5.084940046072006}]`,
			method:        http.MethodGet,
			extraRequests: 1,
		},
		{
			description: "relations many | with specified fields, without relation Id",
			resourceURI: "015_relations_many",
			expected:    `[{"Id":1,"Events":[]},{"Id":2,"Events":[{"UserId":1},{"UserId":10}]},{"Id":11,"Events":[{"UserId":2}]},{"Id":111,"Events":[{"UserId":3}]}]`,
			uri:         "/api/event-types?typ_fields=Events,Id&ev_fields=UserId",
			method:      http.MethodGet,
		},
		{
			description: "case format | with specified fields",
			resourceURI: "016_case_format",
			expected:    `[{"id":1,"events":[]},{"id":2,"events":[{"userId":1},{"userId":10}]},{"id":11,"events":[{"userId":2}]},{"id":111,"events":[{"userId":3}]}]`,
			uri:         "/api/event-types?typ_fields=events,id&ev_fields=userId",
			method:      http.MethodGet,
		},
		{
			description: "case format | criteria",
			resourceURI: "016_case_format",
			expected:    `[{"id":1,"events":[]},{"id":2,"events":[{"id":123,"timestamp":"2019-04-10T05:15:33Z","quantity":5,"userId":10}]},{"id":11,"events":[{"id":10,"timestamp":"2019-03-15T12:07:33Z","quantity":21.957962334156036,"userId":2}]},{"id":111,"events":[{"id":100,"timestamp":"2019-04-10T05:15:33Z","quantity":5.084940046072006,"userId":3}]}]`,
			//(userId in (10,2,3))
			uri:    "/api/event-types?typ_fields=events,id&ev_criteria=%28userId%20in%20%2810%2C2%2C3%29%29",
			method: http.MethodGet,
		},
		{
			description: "time_logger ",
			resourceURI: "017_time_logger",
			uri:         "/api/events",
			method:      http.MethodGet,
			expected:    `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","EventTypeId":2,"Quantity":33.23432374000549,"UserId":1},{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","EventTypeId":11,"Quantity":21.957962334156036,"UserId":2},{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3}]`,
		},
		{
			description: "relations_template | discover criteria",
			resourceURI: "018_relations_template",
			uri:         "/api/events?eventTypeId=2",
			method:      http.MethodGet,
			expected:    `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","Quantity":33.23432374000549,"UserId":1,"EventType":{"Id":2,"Type":"type - 2","Code":"code - 2"}},{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","Quantity":21.957962334156036,"UserId":2,"EventType":null},{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","Quantity":5.084940046072006,"UserId":3,"EventType":null}]`,
		},
		{
			description: "custom selector | environment variables",
			resourceURI: "019_custom_selector",
			uri:         "/api/events",
			method:      http.MethodGet,
			expected:    `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","EventTypeId":2,"Quantity":33.23432374000549,"UserId":1},{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","EventTypeId":11,"Quantity":21.957962334156036,"UserId":2},{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3}]`,
			envVariables: map[string]string{
				"alias": "t.",
				"table": "events.",
			},
		},
		{
			description: "custom selector | custom selector params",
			resourceURI: "019_custom_selector",
			uri:         "/api/events?limit=2&skip=1&names=Id,Quantity&sort=Quantity",
			method:      http.MethodGet,
			envVariables: map[string]string{
				"alias": "t.",
				"table": "events.",
			},
			expected: `[{"Id":10,"Quantity":21.957962334156036},{"Id":1,"Quantity":33.23432374000549}]`,
		},
		{
			description: "validator | error",
			resourceURI: "021_validator",
			uri:         "/api/events",
			method:      http.MethodPost,
			requestBody: `{"Id":0,"Quantity":0}`,
			expected:    `{"Status":"error","Errors":[{"View":"events","Parameter":"EventFilter","Message":"required unsupported type: int"}]}`,
		},
		{
			description: "exclude | remove columns",
			resourceURI: "022_exclude",
			expected:    `[{"id":1,"quantity":33.23432374000549,"event_type":{"id":2,"type":"type - 2"}},{"id":10,"quantity":21.957962334156036,"event_type":{"id":11,"type":"type - 11"}},{"id":100,"quantity":5.084940046072006,"event_type":{"id":111,"type":"type - 111"}}]`,
			uri:         "/api/events",
			method:      http.MethodGet,
		},
		{
			description: "slices | filters",
			resourceURI: "023_criteria_sanitizer",
			uri:         "/api/events?filters=%7B%22column%22:%5B%7B%22column_name%22:%22user_id%22,%22search_values%22:%5B2,11%5D,%22inclusive%22:true%7D,%7B%22column_name%22:%22event_type_id%22,%22search_values%22:%5B2,11%5D,%22inclusive%22:true%7D%5D%7D",
			method:      http.MethodGet,
			envVariables: map[string]string{
				"alias": "t",
				"table": "events",
			},
			expected: `[{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","EventTypeId":11,"Quantity":21.957962334156036,"UserId":2}]`,
		},
		{
			description: "view_cache",
			resourceURI: "024_view_cache",
			uri:         "/api/events?filters=%7B%22column%22:%5B%7B%22column_name%22:%22user_id%22,%22search_values%22:%5B2,11%5D,%22inclusive%22:true%7D,%7B%22column_name%22:%22event_type_id%22,%22search_values%22:%5B2,11%5D,%22inclusive%22:true%7D%5D%7D",
			method:      http.MethodGet,
			expected:    `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","EventTypeId":2,"Quantity":33.23432374000549,"UserId":1},{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","EventTypeId":11,"Quantity":21.957962334156036,"UserId":2},{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3}]`,
		},
		{
			description: "transforms",
			resourceURI: "025_transforms",
			uri:         "/api/employees",
			method:      http.MethodGet,
			codecs: map[string]interface{}{
				"AsStrings": config.NewCodec("AsStrings", &asStrings{}, reflect.TypeOf([]string{})),
			},
			expected: `[{"Id":1,"Email":"abc@example.com","Department":{"Id":1,"Name":["dep","-","1"]}},{"Id":2,"Email":"example@gmail.com","Department":{"Id":2,"Name":["dep","-","2"]}},{"Id":3,"Email":"tom@example.com","Department":{"Id":1,"Name":["dep","-","1"]}},{"Id":4,"Email":"Ann@example.com","Department":{"Id":2,"Name":["dep","-","2"]}}]`,
		},
		{
			description: "transforms",
			resourceURI: "026_date_format",
			uri:         "/api/events",
			method:      http.MethodGet,
			codecs:      map[string]interface{}{},
			expected:    `[{"Id":1,"Timestamp":"11-03-2019","EventTypeId":2,"Quantity":33.23432374000549,"UserId":1},{"Id":10,"Timestamp":"15-03-2019","EventTypeId":11,"Quantity":21.957962334156036,"UserId":2},{"Id":100,"Timestamp":"10-04-2019","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3}]`,
		},
		{
			description:   "aerospike cache",
			resourceURI:   "027_aerospike_cache",
			uri:           "/api/events",
			method:        http.MethodGet,
			codecs:        map[string]interface{}{},
			expected:      `[{"Id":1,"Timestamp":"11-03-2019","EventTypeId":2,"Quantity":33.23432374000549,"UserId":1},{"Id":10,"Timestamp":"15-03-2019","EventTypeId":11,"Quantity":21.957962334156036,"UserId":2},{"Id":100,"Timestamp":"10-04-2019","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3}]`,
			extraRequests: 1,
		},
		{
			description: "page",
			resourceURI: "028_page",
			uri:         "/api/events?_page=3",
			method:      http.MethodGet,
			codecs:      map[string]interface{}{},
			expected:    `[{"Id":102,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3},{"Id":103,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3}]`,
		},
		{
			description:       "executor",
			resourceURI:       "029_executor",
			uri:               "/api/events",
			method:            http.MethodPost,
			codecs:            map[string]interface{}{},
			afterInsertUri:    "/api/events",
			afterInsertMethod: http.MethodGet,
			requestBody: `{"items": [
			{"Id": 1, "Quantity": 125.5, "Timestamp": "2022-08-09T23:10:17.720975+02:00"},
			{"Id": 2, "Quantity": 250.5, "Timestamp": "2022-01-09T23:10:17.720975+02:00"},
			{"Id": 3, "Quantity": 300, "Timestamp": "2020-01-09T23:10:17.720975+02:00"}
]}`,
			afterInsertExpected: `[{"Id":1,"Timestamp":"2022-08-09T23:10:17+02:00","EventTypeId":0,"Quantity":125.5,"UserId":0},{"Id":2,"Timestamp":"2022-01-09T23:10:17+02:00","EventTypeId":0,"Quantity":250.5,"UserId":0},{"Id":3,"Timestamp":"2020-01-09T23:10:17+02:00","EventTypeId":0,"Quantity":300,"UserId":0}]`,
		},
		{
			description:         "executor with param slice",
			resourceURI:         "030_param_slice",
			uri:                 "/api/events",
			method:              http.MethodPost,
			codecs:              map[string]interface{}{},
			afterInsertUri:      "/api/events?_criteria=Quantity=40",
			afterInsertMethod:   http.MethodGet,
			requestBody:         `{"ID": [1,10,103], "Quantity": 40}`,
			afterInsertExpected: `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","EventTypeId":2,"Quantity":40,"UserId":1},{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","EventTypeId":11,"Quantity":40,"UserId":2},{"Id":103,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":40,"UserId":3}]`,
		},
		{
			description: "executor with param slice | error",
			resourceURI: "030_param_slice",
			uri:         "/api/events",
			method:      http.MethodPost,
			codecs:      map[string]interface{}{},
			requestBody: `{"ID": [1,10,103], "Quantity": 0}`,
			expected:    `{"Message":"invalid status"}`,
		},
		{
			description:         "multiple execution statements | multiple execs",
			resourceURI:         "031_multiple_execs",
			uri:                 "/api/events",
			method:              http.MethodPost,
			codecs:              map[string]interface{}{},
			afterInsertUri:      "/api/events?_criteria=Quantity=40",
			afterInsertMethod:   http.MethodGet,
			requestBody:         `{"ID": [1,10,103], "Quantity": 40}`,
			afterInsertExpected: `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","EventTypeId":2,"Quantity":40,"UserId":1},{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","EventTypeId":11,"Quantity":40,"UserId":2},{"Id":103,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":40,"UserId":3}]`,
		},
		{
			description: "extract values from Request Body",
			resourceURI: "032_request_body",
			uri:         "/api/events",
			method:      http.MethodPost,
			codecs:      map[string]interface{}{},
			requestBody: `{"ID": 1, "Wrapper": {"Quantity": 40, "Timestamp": "2019-03-12T02:20:33Z"}}`,
			expected:    `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","EventTypeId":2,"Quantity":33.23432374000549,"UserId":1}]`,
		},
		{
			description: "executor with param slice",
			resourceURI: "033_custom_err_message",
			uri:         "/api/events",
			method:      http.MethodPost,
			codecs:      map[string]interface{}{},
			requestBody: `{"ID": [1,10,103], "Quantity": 0}`,
			expected:    `{"Errors":[{"View":"events","Parameter":"data","Object":[{"Id":1,"Status":false},{"Id":10,"Status":false},{"Id":103,"Status":false}]}]}`,
		},
		{
			description: "executor with param slice",
			resourceURI: "034_slice_expansion",
			uri:         "/api/events",
			method:      http.MethodPost,
			codecs:      map[string]interface{}{},
			requestBody: `[1,10,103]`,
			expected:    `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","EventTypeId":2,"Quantity":33.23432374000549,"UserId":1},{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","EventTypeId":11,"Quantity":21.957962334156036,"UserId":2},{"Id":103,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3}]`,
		},
		{
			description:         "executor with param slice",
			resourceURI:         "035_logger",
			uri:                 "/api/events",
			method:              http.MethodPost,
			codecs:              map[string]interface{}{},
			requestBody:         `{"ID": [1,10,103], "Quantity": 0}`,
			afterInsertUri:      "/api/events",
			afterInsertMethod:   http.MethodGet,
			afterInsertExpected: `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","EventTypeId":2,"Quantity":0,"UserId":1},{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","EventTypeId":11,"Quantity":0,"UserId":2},{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3},{"Id":101,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3},{"Id":102,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3},{"Id":103,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":0,"UserId":3}]`,
		},
		{
			description: "pagination over main view | basic, header",
			resourceURI: "036_pagination_basic",
			uri:         "/api/events?_page=2",
			method:      http.MethodGet,
			codecs:      map[string]interface{}{},
			expected:    `[{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3},{"Id":101,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3}]`,
			expectedHeaders: map[string][]string{
				"Events-Summary": {`{"TotalRecords":6,"CurrentPage":2,"PageSize":2}`},
			},
		},
		{
			description: "pagination over main view | comprehensive, record",
			resourceURI: "037_pagination_comprehensive",
			uri:         "/api/events?_page=2",
			method:      http.MethodGet,
			codecs:      map[string]interface{}{},
			expected:    `{"Status":"ok","ResponseBody":[{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3},{"Id":101,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3}],"EventsMeta":{"TotalRecords":6,"CurrentPage":2,"PageSize":2}}`,
		},
		{
			description: "meta over nested view | comprehensive, record",
			resourceURI: "038_pagination_nested",
			uri:         "/api/event-types",
			method:      http.MethodGet,
			codecs:      map[string]interface{}{},
			expected:    `{"Status":"ok","ResponseBody":[{"Id":1,"Type":"type - 1","Code":"code - 1","Events":[]},{"Id":2,"Type":"type - 2","Code":"code - 2","Events":[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","EventTypeId":2,"Quantity":33.23432374000549,"UserId":1}],"EventsMeta":{"EventTypeId":2,"TotalCount":1}},{"Id":11,"Type":"type - 11","Code":"code - 11","Events":[{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","EventTypeId":11,"Quantity":21.957962334156036,"UserId":2}],"EventsMeta":{"EventTypeId":11,"TotalCount":1}},{"Id":111,"Type":"type - 111","Code":"code - 111","Events":[{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3},{"Id":101,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3},{"Id":102,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3},{"Id":103,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3}],"EventsMeta":{"EventTypeId":111,"TotalCount":4}}]}`,
		},
		{
			description: "tree",
			resourceURI: "039_tree",
			uri:         "/api/nodes",
			method:      http.MethodGet,
			codecs:      map[string]interface{}{},
			expected:    `[{"Id":0,"Label":"/parent-1","Children":[{"Id":2,"Label":"/parent-1/children-1","Children":[{"Id":3,"Label":"/parent-1/children-1/children-1","Children":[{"Id":6,"Label":"/parent-1/children-1/children-1/children-1","Children":[{"Id":4,"Label":"/parent-1/children-1/children-1/children-1/children-1","Children":[{"Id":5,"Label":"/parent-1/children-1/children-1/children-1/children-1/children-1","Children":[]}]}]}]}]}]},{"Id":1,"Label":"/parent-2","Children":[]}]`,
		},
		{
			description: "csv output format",
			resourceURI: "040_csv_output",
			uri:         "/api/events?_format=CSV",
			expected: `"Id","Timestamp","EventTypeId","Quantity","UserId"
1,"2019-03-11T02:20:33Z",2,33.23432374000549,1
10,"2019-03-15T12:07:33Z",11,21.957962334156036,2
100,"2019-04-10T05:15:33Z",111,5.084940046072006,3`,
			method: http.MethodGet,
			expectedHeaders: map[string][]string{
				"Content-Type": {"text/csv; charset=utf-8"},
			},
			useAssertPkg: true,
		},
		{
			description: "csv input",
			resourceURI: "041_csv_input",
			uri:         "/api/events",
			requestBody: `ID,Quantity
1,6`,
			expected: `[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","EventTypeId":2,"Quantity":33.23432374000549,"UserId":1},{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3}]`,
			method:   http.MethodPost,
			expectedHeaders: map[string][]string{
				"Content-Type": {"application/json; charset=utf-8"},
			},
			headers: map[string][]string{
				"Content-Type": {router.CSVContentType},
			},
			codecs: map[string]interface{}{
				config.CodecKeyCSV: config.CsvFactory(""),
			},
		},
		{
			description: "meta prewarmup | DebugKind record",
			resourceURI: "042_meta_prewarmup",
			uri:         "/api/event-types",
			method:      http.MethodGet,
			preWarmup:   true,
			closeAfterPreWarmup: map[string]bool{
				"events": true,
			},
			expected: `{"Status":"ok","ResponseBody":[{"Id":1,"Type":"type - 1","Code":"code - 1","Events":[]},{"Id":2,"Type":"type - 2","Code":"code - 2","Events":[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","EventTypeId":2,"Quantity":33.23432374000549,"UserId":1}],"EventsMeta":{"EventTypeId":2,"TotalCount":1}},{"Id":11,"Type":"type - 11","Code":"code - 11","Events":[{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","EventTypeId":11,"Quantity":21.957962334156036,"UserId":2}],"EventsMeta":{"EventTypeId":11,"TotalCount":1}},{"Id":111,"Type":"type - 111","Code":"code - 111","Events":[{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3},{"Id":101,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3},{"Id":102,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3},{"Id":103,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3}],"EventsMeta":{"EventTypeId":111,"TotalCount":4}}]}`,
		},
		{
			description: "meta prewarmup with cache disabled | DebugKind record",
			resourceURI: "042_meta_prewarmup",
			uri:         "/api/event-types",
			method:      http.MethodGet,
			preWarmup:   true,
			headers: map[string][]string{
				httputils.DatlyRequestDisableCacheHeader: {"true"},
			},
			closeAfterPreWarmup: map[string]bool{
				"events": true,
			},
			expected: `{"Status":"ok","ResponseBody":[{"Id":1,"Type":"type - 1","Code":"code - 1","Events":[]},{"Id":2,"Type":"type - 2","Code":"code - 2","Events":[]},{"Id":11,"Type":"type - 11","Code":"code - 11","Events":[]},{"Id":111,"Type":"type - 111","Code":"code - 111","Events":[]}]}`,
		},
		{
			description: "meta prewarmup with debug enabled | DebugKind record",
			resourceURI: "042_meta_prewarmup",
			uri:         "/api/event-types",
			method:      http.MethodGet,
			preWarmup:   true,
			headers: map[string][]string{
				httputils.DatlyRequestMetricsHeader: {httputils.DatlyDebugHeaderValue},
			},
			closeAfterPreWarmup: map[string]bool{
				"events": true,
			},
			expected: `{"Status":"ok","ResponseBody":[{"Id":1,"Type":"type - 1","Code":"code - 1","Events":[]},{"Id":2,"Type":"type - 2","Code":"code - 2","Events":[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","EventTypeId":2,"Quantity":33.23432374000549,"UserId":1}],"EventsMeta":{"EventTypeId":2,"TotalCount":1}},{"Id":11,"Type":"type - 11","Code":"code - 11","Events":[{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","EventTypeId":11,"Quantity":21.957962334156036,"UserId":2}],"EventsMeta":{"EventTypeId":11,"TotalCount":1}},{"Id":111,"Type":"type - 111","Code":"code - 111","Events":[{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3},{"Id":101,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3},{"Id":102,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3},{"Id":103,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3}],"EventsMeta":{"EventTypeId":111,"TotalCount":4}}],"_datly_debug_":[{"View":"event_types","Template":[{"SQL":"SELECT  COALESCE(id,0) AS id,  COALESCE(type,\"\") AS type,  COALESCE(code,\"\") AS code FROM (SELECT * FROM EVENT_TYPES   ) AS t ","CacheStats":{"Type":"lazy","RecordsCounter":1,"Column":"-7975909061465053937","FoundLazy":true}}],"TemplateMeta":[{"SQL":"SELECT COUNT(*) AS total_count FROM (SELECT  COALESCE(id,0) AS id,  COALESCE(type,\"\") AS type,  COALESCE(code,\"\") AS code FROM (SELECT * FROM EVENT_TYPES   ) AS t ) T","CacheStats":{"Type":"lazy","RecordsCounter":1,"Column":"4341218873677092370","FoundLazy":true}}],"Elapsed":"5ms"},{"View":"events","Template":[{"SQL":"SELECT  COALESCE(id,0) AS id,  t.timestamp,  COALESCE(event_type_id,0) AS event_type_id,  COALESCE(quantity,0) AS quantity,  COALESCE(user_id,0) AS user_id FROM (SELECT * FROM EVENTS WHERE 1 = 1 AND EVENT_TYPE_ID IN (?, ?, ?, ? )  AND ( event_type_id IN (?, ?, ?, ?))  ) AS t ","Args":[1,2,11,111,1,2,11,111],"CacheStats":{"Type":"warmup","RecordsCounter":4,"Column":"event_type_id#-4347759850797876299","FoundWarmup":true}}],"TemplateMeta":[{"SQL":"SELECT event_type_id, COUNT(*) AS total_count FROM (SELECT  COALESCE(id,0) AS id,  t.timestamp,  COALESCE(event_type_id,0) AS event_type_id,  COALESCE(quantity,0) AS quantity,  COALESCE(user_id,0) AS user_id FROM (SELECT * FROM EVENTS WHERE 1 = 1 AND EVENT_TYPE_ID IN (?, ?, ?, ? )  AND ( event_type_id IN (?, ?, ?, ?))  ) AS t ) T GROUP BY event_type_id","Args":[1,2,11,111,1,2,11,111],"CacheStats":{"Type":"warmup","RecordsCounter":4,"Column":"event_type_id#2930711288512290438","FoundWarmup":true}}],"Elapsed":"5ms"}]}`,
		},
		{
			description: "csv output format with default config: float prec == -1",
			resourceURI: "043_nested_csv_output",
			uri:         "/api/event-types?_format=CSV",
			expected: `"Id","Type","Code","Events.Id","Events.Timestamp","Events.Quantity","Events.UserId"
1,"type - 1","code - 1",null,null,null,null
2,"type - 2","code - 2",1,"2019-03-11T02:20:33Z",33.23432374000549,1
2,"type - 2","code - 2",123,"2019-04-10T05:15:33Z",5,10
11,"type - 11","code - 11",10,"2019-03-15T12:07:33Z",21.957962334156036,2
111,"type - 111","code - 111",100,"2019-04-10T05:15:33Z",5.084940046072006,3`,
			method: http.MethodGet,
			expectedHeaders: map[string][]string{
				"Content-Type": {"text/csv; charset=utf-8"},
			},
		},
		{
			description: "meta prewarmup with debug enabled | DebugKind header",
			resourceURI: "044_debug_headers",
			uri:         "/api/event-types",
			method:      http.MethodGet,
			preWarmup:   true,
			headers: map[string][]string{
				httputils.DatlyRequestMetricsHeader: {httputils.DatlyDebugHeaderValue},
			},
			closeAfterPreWarmup: map[string]bool{
				"events": true,
			},
			expectedHeaders: map[string][]string{
				"Datly-Metrics-Event_types": {
					`{"View":"event_types","Template":[{"SQL":"SELECT  COALESCE(id,0) AS id,  COALESCE(type,\"\") AS type,  COALESCE(code,\"\") AS code FROM (SELECT * FROM EVENT_TYPES   ) AS t ","CacheStats":{"Type":"lazy","RecordsCounter":1,"Column":"-7975909061465053937","FoundLazy":true}}],"TemplateMeta":[{"SQL":"SELECT COUNT(*) AS total_count FROM (SELECT  COALESCE(id,0) AS id,  COALESCE(type,\"\") AS type,  COALESCE(code,\"\") AS code FROM (SELECT * FROM EVENT_TYPES   ) AS t ) T","CacheStats":{"Type":"lazy","RecordsCounter":1,"Column":"4341218873677092370","FoundLazy":true}}],"Elapsed":"5ms"}`,
					`{"View":"event_types","Template":[{"SQL":"SELECT  COALESCE(id,0) AS id,  COALESCE(type,\"\") AS type,  COALESCE(code,\"\") AS code FROM (SELECT * FROM EVENT_TYPES   ) AS t ","CacheStats":{"Type":"write","RecordsCounter":0,"Column":""}}],"TemplateMeta":[{"SQL":"SELECT COUNT(*) AS total_count FROM (SELECT  COALESCE(id,0) AS id,  COALESCE(type,\"\") AS type,  COALESCE(code,\"\") AS code FROM (SELECT * FROM EVENT_TYPES   ) AS t ) T","CacheStats":{"Type":"write","RecordsCounter":0,"Column":""}}],"Elapsed":"5ms"}`,
				},
				"Datly-Metrics-Events": {`{"View":"events","Template":[{"SQL":"SELECT  COALESCE(id,0) AS id,  t.timestamp,  COALESCE(event_type_id,0) AS event_type_id,  COALESCE(quantity,0) AS quantity,  COALESCE(user_id,0) AS user_id FROM (SELECT * FROM EVENTS WHERE 1 = 1 AND EVENT_TYPE_ID IN (?, ?, ?, ? )  AND ( event_type_id IN (?, ?, ?, ?))  ) AS t ","Args":[1,2,11,111,1,2,11,111],"CacheStats":{"Type":"warmup","RecordsCounter":4,"Column":"event_type_id#-4347759850797876299","FoundWarmup":true}}],"TemplateMeta":[{"SQL":"SELECT event_type_id, COUNT(*) AS total_count FROM (SELECT  COALESCE(id,0) AS id,  t.timestamp,  COALESCE(event_type_id,0) AS event_type_id,  COALESCE(quantity,0) AS quantity,  COALESCE(user_id,0) AS user_id FROM (SELECT * FROM EVENTS WHERE 1 = 1 AND EVENT_TYPE_ID IN (?, ?, ?, ? )  AND ( event_type_id IN (?, ?, ?, ?))  ) AS t ) T GROUP BY event_type_id","Args":[1,2,11,111,1,2,11,111],"CacheStats":{"Type":"warmup","RecordsCounter":4,"Column":"event_type_id#2930711288512290438","FoundWarmup":true}}],"Elapsed":"5ms"}`},
			},
			expected: `[{"Id":1,"Type":"type - 1","Code":"code - 1","Events":[]},{"Id":2,"Type":"type - 2","Code":"code - 2","Events":[{"Id":1,"Timestamp":"2019-03-11T02:20:33Z","EventTypeId":2,"Quantity":33.23432374000549,"UserId":1}],"EventsMeta":{"EventTypeId":2,"TotalCount":1}},{"Id":11,"Type":"type - 11","Code":"code - 11","Events":[{"Id":10,"Timestamp":"2019-03-15T12:07:33Z","EventTypeId":11,"Quantity":21.957962334156036,"UserId":2}],"EventsMeta":{"EventTypeId":11,"TotalCount":1}},{"Id":111,"Type":"type - 111","Code":"code - 111","Events":[{"Id":100,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3},{"Id":101,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3},{"Id":102,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3},{"Id":103,"Timestamp":"2019-04-10T05:15:33Z","EventTypeId":111,"Quantity":5.084940046072006,"UserId":3}],"EventsMeta":{"EventTypeId":111,"TotalCount":4}}]`,
		},
		{
			description: "executor",
			resourceURI: "045_exec_output",
			uri:         "/api/events",
			method:      http.MethodPost,
			codecs:      map[string]interface{}{},
			requestBody: `{"items": [
			{"Id": 1, "Quantity": 125.5, "Timestamp": "2022-08-09T23:10:17.720975+02:00"},
			{"Id": 2, "Quantity": 250.5, "Timestamp": "2022-01-09T23:10:17.720975+02:00"},
			{"Id": 3, "Quantity": 300, "Timestamp": "2020-01-09T23:10:17.720975+02:00"}
]}`,
			expected: `{"items":[{"Id":1,"Quantity":125.5,"Timestamp":"2022-08-09T23:10:17+02:00"},{"Id":2,"Quantity":250.5,"Timestamp":"2022-01-09T23:10:17+02:00"},{"Id":3,"Quantity":300,"Timestamp":"2020-01-09T23:10:17+02:00"}]}`,
		},
		{
			description:  "tabular json output format with default config: float prec == -1",
			resourceURI:  "046_tabjson_output_def_conf",
			uri:          "/api/events",
			useAssertPkg: true,
			expected:     `[["Id","Timestamp","EventTypeId","Quantity","UserId"],[1,"2019-03-11T02:20:33Z",2,33.23432374000549,1],[10,"2019-03-15T12:07:33Z",11,21.957962334156036,2],[100,"2019-04-10T05:15:33Z",111,5.084940046072006,3]]`,
			method:       http.MethodGet,
			expectedHeaders: map[string][]string{
				"Content-Type": {"application/json; charset=utf-8"},
			},
		},
		{
			description:  "tabular json output format with custom config - float prec == 3",
			resourceURI:  "047_tabular_json_output",
			uri:          "/api/events",
			useAssertPkg: true,
			expected:     `[["Id","Timestamp","EventTypeId","Quantity","UserId"],[1,"2019-03-11T02:20:33Z",2,33.234323740005493164062500000000,1],[10,"2019-03-15T12:07:33Z",11,21.957962334156036376953125000000,2],[100,"2019-04-10T05:15:33Z",111,5.084940046072006225585937500000,3]]`,
			method:       http.MethodGet,
			expectedHeaders: map[string][]string{
				"Content-Type": {"application/json; charset=utf-8"},
			},
		},
		{
			description:  "styles | error | comprehensive - tabular JSON",
			resourceURI:  "048_tabjson_style",
			uri:          "/api/events?_criteria=(id%20=%201%20UNION%20ALL%20SELECT%209%20as%20id%2C%20To_Date%28%222019-03-11T02%3A20%3A33Z%22%29%20as%20timestamp%2C%2010%20as%20event_type_id%2C%2020%20as%20quantity%2C%206%20as%20user_id)",
			useAssertPkg: true,
			expected:     `{"Status":"error","Message":"can't use criteria on view events","Errors":[{"View":"events","Parameter":"_criteria","Message":"can't use criteria on view events"}],"DataPtr":[]}`,
			method:       http.MethodGet,
		},
		{
			description:  "pagination over main view | comprehensive, record | comprehensive - tabular JSON",
			resourceURI:  "049_tabjson_pagination_comprehensive",
			uri:          "/api/events?_page=2",
			method:       http.MethodGet,
			codecs:       map[string]interface{}{},
			useAssertPkg: true,
			expected:     `{"Status":"ok","ResponseBody":[["Id","Timestamp","EventTypeId","Quantity","UserId"],[100,"2019-04-10T05:15:33Z",111,5.084940046072006,3],[101,"2019-04-10T05:15:33Z",111,5.084940046072006,3]],"EventsMeta":{"TotalRecords":6,"CurrentPage":2,"PageSize":2}}`,
		},
		{
			description:  "meta over nested view | comprehensive, record - tabular JSON",
			resourceURI:  "050_tabjson_pagination_nested",
			uri:          "/api/event-types",
			method:       http.MethodGet,
			codecs:       map[string]interface{}{},
			useAssertPkg: true,
			expected:     `{"Status":"ok","ResponseBody":[["Id","Type","Code","Events","EventsMeta"],[1,"type - 1","code - 1",null,null],[2,"type - 2","code - 2",[["Id","Timestamp","EventTypeId","Quantity","UserId"],[1,"2019-03-11T02:20:33Z",2,33.23432374000549,1]],[["EventTypeId","TotalCount"],[2,1]]],[11,"type - 11","code - 11",[["Id","Timestamp","EventTypeId","Quantity","UserId"],[10,"2019-03-15T12:07:33Z",11,21.957962334156036,2]],[["EventTypeId","TotalCount"],[11,1]]],[111,"type - 111","code - 111",[["Id","Timestamp","EventTypeId","Quantity","UserId"],[100,"2019-04-10T05:15:33Z",111,5.084940046072006,3],[101,"2019-04-10T05:15:33Z",111,5.084940046072006,3],[102,"2019-04-10T05:15:33Z",111,5.084940046072006,3],[103,"2019-04-10T05:15:33Z",111,5.084940046072006,3]],[["EventTypeId","TotalCount"],[111,4]]]]}`,
		},
		{
			description: "meta prewarmup | DebugKind record | comprehensive - tabular JSON",
			resourceURI: "051_tabjson_meta_prewarmup",
			uri:         "/api/event-types",
			method:      http.MethodGet,
			preWarmup:   true,
			closeAfterPreWarmup: map[string]bool{
				"events": true,
			},
			useAssertPkg: true,
			expected:     `{"Status":"ok","ResponseBody":[["Id","Type","Code","Events","EventsMeta"],[1,"type - 1","code - 1",null,null],[2,"type - 2","code - 2",[["Id","Timestamp","EventTypeId","Quantity","UserId"],[1,"2019-03-11T02:20:33Z",2,33.23432374000549,1]],[["EventTypeId","TotalCount"],[2,1]]],[11,"type - 11","code - 11",[["Id","Timestamp","EventTypeId","Quantity","UserId"],[10,"2019-03-15T12:07:33Z",11,21.957962334156036,2]],[["EventTypeId","TotalCount"],[11,1]]],[111,"type - 111","code - 111",[["Id","Timestamp","EventTypeId","Quantity","UserId"],[100,"2019-04-10T05:15:33Z",111,5.084940046072006,3],[101,"2019-04-10T05:15:33Z",111,5.084940046072006,3],[102,"2019-04-10T05:15:33Z",111,5.084940046072006,3],[103,"2019-04-10T05:15:33Z",111,5.084940046072006,3]],[["EventTypeId","TotalCount"],[111,4]]]]}`,
		},
		{
			description: "csv output format with default config: float prec == -1, no _format param needed in uri\",",
			resourceURI: "052_csv_output_def_config",
			uri:         "/api/events",
			expected: `"Id","Timestamp","EventTypeId","Quantity","UserId"
1,"2019-03-11T02:20:33Z",2,33.23432374000549,1
10,"2019-03-15T12:07:33Z",11,21.957962334156036,2
100,"2019-04-10T05:15:33Z",111,5.084940046072006,3`,
			method: http.MethodGet,
			expectedHeaders: map[string][]string{
				"Content-Type": {"text/csv; charset=utf-8"},
			},
			useAssertPkg: true,
		},
		{
			description:  "exclude | remove columns | tabular JSON",
			resourceURI:  "053_tabjson_exclude",
			useAssertPkg: true,
			expected:     `[["Id","Quantity","EventType"],[1,33.23432374000549,[["Id","Type"],[2,"type - 2"]]],[10,21.957962334156036,[["Id","Type"],[11,"type - 11"]]],[100,5.084940046072006,[["Id","Type"],[111,"type - 111"]]]]`,
			uri:          "/api/events",
			method:       http.MethodGet,
		},
	}

	//for i, tCase := range testcases[len(testcases)-1:] {
	for i, tCase := range testcases {
		testcases[0].cleanup()
		if i != 0 {
			testcases[i-1].cleanup()
		}

		tests.LogHeader(fmt.Sprintf("\n==> Running testcase  %v, %v\n", i, tCase.description))
		testUri := path.Join(testLocation, "testdata")
		routingHandler, ok := tCase.init(t, testUri)
		if !ok {
			continue
		}

		if tCase.corsHeaders != nil {
			corsRequest := httptest.NewRequest(http.MethodOptions, tCase.uri, nil)
			corsWriter := httptest.NewRecorder()
			err := routingHandler.Handle(corsWriter, corsRequest)
			assert.Nil(t, err, tCase.description)

			headers := corsWriter.Header()
			for headerName, headerValue := range tCase.corsHeaders {
				assert.Equal(t, headerValue, headers.Get(headerName), tCase.description)
			}
		}

		for j := 0; j < tCase.extraRequests+1; j++ {
			if !tCase.sendHttpRequest(t, routingHandler, tCase.shouldDecompress, true, tCase.expectedHeaders) {
				return
			}
		}

		if tCase.afterInsertUri != "" {
			if !tCase.sendHttpRequest(t, routingHandler, false, false, tCase.afterInsertExpectedHeaders) {
				return
			}
		}
	}
}

func (c *testcase) init(t *testing.T, testDataLocation string) (*router.Router, bool) {
	for name, value := range c.envVariables {
		_ = os.Setenv(name, value)
	}

	resourceURI := path.Join(testDataLocation, c.resourceURI)
	fs := afs.New()
	if !tests.InitDB(t, path.Join(testDataLocation, "db_config.yaml"), path.Join(testDataLocation, c.resourceURI, "populate"), "db") {
		return nil, false
	}

	dependencies := map[string]*view.Resource{}
	for name, URL := range c.dependenciesUrl {
		resourceUrl := path.Join(resourceURI, fmt.Sprintf("%v.yaml", URL))
		resource, ok := c.readViewResource(t, resourceUrl, c.types, c.codecs)
		if !ok {
			return nil, false
		}
		dependencies[name] = resource
	}

	resourceUrl := path.Join(resourceURI, "resource.yaml")
	resource, ok := c.readResource(t, fs, resourceUrl, dependencies)
	if !ok {
		return nil, false
	}

	aRouter, _ := router.New(resource)

	//if !c.checkGeneratedOpenAPI(t, resource, resourceURI, fs) {
	//	return nil, false
	//}

	if c.preWarmup {
		for _, route := range aRouter.Routes("") {
			views := router.ExtractCacheableViews(route)

			warmup.PreCache(func(method, matchingURI string) ([]*view.View, error) {
				return views, nil
			}, "")

			if len(c.closeAfterPreWarmup) != 0 {
				if !c.cleanAfterPrewarmup(t, testDataLocation, views) {
					return nil, false
				}
			}
		}
	}

	return aRouter, true
}

func (c *testcase) cleanAfterPrewarmup(t *testing.T, location string, views []*view.View) bool {
	_ = toolbox.CreateDirIfNotExist(path.Join(location, "db", "mock.db"))

	for _, aView := range views {
		if !c.closeAfterPreWarmup[aView.Name] {
			continue
		}

		fmt.Printf("[INFO] Replacing view %v connector with mock one to check Aerospike cache connection\n", aView.Name)

		aView.Connector = &view.Connector{
			Name:   "mock",
			DSN:    path.Join(location, "db", "mock.db"),
			Driver: "sqlite3",
		}

		if !assert.Nil(t, aView.Connector.Init(context.TODO(), map[string]*view.Connector{}), c.description) {
			return false
		}
	}

	return true
}

func (c *testcase) checkGeneratedOpenAPI(t *testing.T, resource *router.Resource, resourceURI string, fs afs.Service) bool {
	openAPIURI := path.Join(resourceURI, "openapi3.yaml")
	expectedOpenapi, err := loadOpenApi(context.TODO(), openAPIURI, fs)
	if err != nil {
		fmt.Printf("Skiping openapi3 check for testcase: %v\n", c.description)
		return true
	}

	generated, err := c.readOpenAPI(resource)
	if !assert.Nil(t, err, c.description) {
		return false
	}

	aMap, err := asMap(generated)
	if !assert.Nil(t, err, c.description) {
		return false
	}

	if !assert.EqualValues(t, expectedOpenapi, aMap, c.description) {
		toolbox.Dump(expectedOpenapi)
		expectedBytes, _ := asOpenapiBytes(expectedOpenapi)
		toolbox.Dump(string(expectedBytes))
		actBytes, _ := yaml.Marshal(generated)
		toolbox.Dump(string(actBytes))

	}

	return true
}

func asOpenapiBytes(openapi map[string]interface{}) ([]byte, error) {
	marshal, err := yaml.Marshal(openapi)
	if err != nil {
		return nil, err
	}

	result := &openapi3.OpenAPI{}
	if err = yaml.Unmarshal(marshal, result); err != nil {
		return nil, err
	}

	return yaml.Marshal(result)
}

func asMap(generated *openapi3.OpenAPI) (map[string]interface{}, error) {
	marshal, err := yaml.Marshal(generated)
	if err != nil {
		return nil, err
	}

	aMap := map[string]interface{}{}
	return aMap, yaml.Unmarshal(marshal, &aMap)
}

func (c *testcase) readOpenAPI(resource *router.Resource) (*openapi3.OpenAPI, error) {
	routes := resource.Routes
	return router.GenerateOpenAPI3Spec(openapi3.Info{}, routes...)
}

func (c *testcase) readResource(t *testing.T, fs afs.Service, resourceUrl string, dependencies map[string]*view.Resource) (*router.Resource, bool) {
	resource, err := router.NewResourceFromURL(context.TODO(), fs, resourceUrl, false, c.codecs, c.types, dependencies, nil)
	if !assert.Nil(t, err, c.description) {
		return nil, false
	}

	return resource, true
}

func (c *testcase) readViewResource(t *testing.T, resourceUrl string, types *xreflect.Types, visitors config.CodecsRegistry) (*view.Resource, bool) {
	resource, err := view.NewResourceFromURL(context.TODO(), resourceUrl, types, visitors)
	if !assert.Nil(t, err, c.description) {
		return nil, false
	}
	return resource, true
}

func (c *testcase) sendHttpRequest(t *testing.T, handler *router.Router, shouldDecompress bool, useMainRoute bool, expectedHeaders http.Header) bool {
	method, uri, expected := c.method, c.uri, c.expected
	if !useMainRoute {
		method, uri, expected = c.afterInsertMethod, c.afterInsertUri, c.afterInsertExpected
	}

	var body io.Reader
	if method != http.MethodGet {
		body = bytes.NewReader([]byte(c.requestBody))
	}

	httpRequest := httptest.NewRequest(method, uri, body)
	for header, values := range c.headers {
		httpRequest.Header.Add(header, values[0])
	}

	responseWriter := httptest.NewRecorder()
	err := handler.Handle(responseWriter, httpRequest)
	if !assert.Nil(t, err, c.description) {
		return false
	}

	response, err := ioutil.ReadAll(responseWriter.DataPtr().Body)
	if !assert.Nil(t, err, c.description) {
		return false
	}

	if shouldDecompress {
		assert.Equal(t, httputils.EncodingGzip, responseWriter.Header().Get(content.Encoding), c.description)
		reader, err := gzip.NewReader(bytes.NewReader(response))
		if assert.Nil(t, err, c.description) {
			decompressed, err := ioutil.ReadAll(reader)
			assert.Nil(t, err, c.description)
			response = decompressed
		}
	}

	if !assertly.AssertValues(t, expected, string(response), c.description) {
		fmt.Println("*** ACTUAL")
		fmt.Println(string(response))
		fmt.Println()
		fmt.Println("*** EXPECTED")
		fmt.Println(expected)
	}

	if c.useAssertPkg {
		if !assert.EqualValues(t, expected, string(response), c.description) {
			fmt.Println(string(response))
		}
	}

	for key, value := range expectedHeaders {
		actual := responseWriter.Header()[key][0]
		readOrWrite := value[0]
		writeOrRead := value[0]
		if len(value) > 1 {
			writeOrRead = value[1]
		}

		if !assert.True(t, readOrWrite == actual || writeOrRead == actual, c.description) {
			fmt.Println("*** ACTUAL HEADER: ", actual)
		}
	}

	return true
}

func (c *testcase) cleanup() {
	for key := range c.envVariables {
		os.Unsetenv(key)
	}
}

func encodeToken(token string) string {
	return base64.StdEncoding.EncodeToString([]byte(token))
}

func loadOpenApi(ctx context.Group, URL string, fs afs.Service) (map[string]interface{}, error) {
	data, err := fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return nil, err
	}

	result := map[string]interface{}{}
	if err := yaml.Unmarshal(data, &result); err != nil {
		return nil, err
	}

	return result, nil
}


*/
