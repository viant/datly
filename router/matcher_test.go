package router

import (
	"github.com/stretchr/testify/assert"
	"net/http"
	"testing"
)

func TestMatcher(t *testing.T) {
	testCases := []struct {
		description  string
		routes       []*Route
		route        string
		matchedRoute string
		expectError  bool
		method       string
	}{
		{
			description: "basic match",
			routes:      []*Route{{URI: "/events", Method: http.MethodGet}},
			route:       "/events",
			method:      http.MethodGet,
		},
		{
			description: "multiple routes",
			routes:      []*Route{{URI: "/events", Method: http.MethodGet}, {URI: "/foos", Method: http.MethodGet}},
			route:       "/foos",
			method:      http.MethodGet,
		},
		{
			description: "nested route",
			routes:      []*Route{{URI: "/events/seg1/seg2/seg3", Method: http.MethodGet}, {URI: "/events/seg1/seg2", Method: http.MethodGet}},
			route:       "/events/seg1/seg2/seg3",
			method:      http.MethodGet,
		},
		{
			description: "nested route",
			routes:      []*Route{{URI: "/events/seg1/seg2/seg3", Method: http.MethodGet}, {URI: "/events/seg1/seg2", Method: http.MethodGet}},
			route:       "/events/seg1/seg2",
			method:      http.MethodGet,
		},
		{
			description:  "wildcard route",
			routes:       []*Route{{URI: "/events/seg1/{segID}/seg3", Method: http.MethodGet}, {URI: "/events/seg1/seg2", Method: http.MethodGet}},
			route:        "/events/seg1/1/seg3",
			matchedRoute: "/events/seg1/{segID}/seg3",
			method:       http.MethodGet,
		},
		{
			description:  "post method",
			routes:       []*Route{{URI: "/events/seg1/{segID}/seg3", Method: http.MethodGet}, {URI: "/events/seg1/seg2", Method: http.MethodGet}},
			route:        "/events/seg1/1/seg3",
			matchedRoute: "/events/seg1/{segID}/seg3",
			method:       http.MethodPost,
			expectError:  true,
		},
		{
			description: "exact precedence",
			routes:      []*Route{{URI: "/events/seg1/{segID}/seg3", Method: http.MethodGet}, {URI: "/events/seg1/seg2/seg4", Method: http.MethodGet}},
			route:       "/events/seg1/seg2/seg4",
			method:      http.MethodGet,
		},
		{
			description: "icnorrect route",
			routes:      []*Route{{URI: "/events/seg1/{segID}/seg3", Method: http.MethodGet}, {URI: "/events/seg1/seg2/seg4", Method: http.MethodGet}},
			route:       "//",
			expectError: true,
			method:      http.MethodGet,
		},
		{
			description:  "query param",
			routes:       []*Route{{URI: "/events/seg1/{segID}/seg3", Method: http.MethodGet}, {URI: "/events/seg1/seg2/seg4", Method: http.MethodGet}},
			route:        "/events/seg1/seg2/seg4?abc=true",
			matchedRoute: "/events/seg1/seg2/seg4",
			method:       http.MethodGet,
		},
	}

	//for _, testCase := range testCases[len(testCases)-1:] {
	for _, testCase := range testCases {
		matcher := NewMatcher(testCase.routes)
		match, err := matcher.Match(testCase.method, testCase.route)
		if testCase.expectError {
			assert.NotNil(t, err, testCase.description)
			continue
		}

		matchedURI := testCase.matchedRoute
		if matchedURI == "" {
			matchedURI = testCase.route
		}

		assert.Nil(t, err, testCase.description)
		assert.Equal(t, matchedURI, match.URI, testCase.description)
	}
}
