package http

import (
	"bytes"
	"context"
	"io"
	"mime/multipart"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly/locator"
	"github.com/viant/structology"
)

func TestHTTPMergedForm(t *testing.T) {
	for _, tc := range []struct {
		method, body string
		want         []string
	}{
		{"GET", "", []string{"query", "second"}},
		{"POST", "value=body&value=body2", []string{"body", "body2", "query", "second"}},
	} {
		t.Run(tc.method, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, "/?value=query&value=second&empty=&country=US", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			scope, err := newHTTPRequestScope(req, nil)
			require.NoError(t, err)
			defer scope.Close()
			var form locator.Locator
			for _, p := range scope.Providers() {
				if p.Kind() == "form" {
					form = p.Locate((*structology.State)(nil))
				}
			}
			ctx := context.Background()
			value, found, err := form.Value(ctx, reflect.TypeFor[[]string](), "value")
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, tc.want, value)
			value.([]string)[0] = "changed"
			value, _, _ = form.Value(ctx, reflect.TypeFor[[]string](), "value")
			require.Equal(t, tc.want, value, "lookups must return detached slices")
			value, found, err = form.Value(ctx, reflect.TypeFor[string](), "empty")
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, "", value)
			_, found, err = form.Value(ctx, reflect.TypeFor[string](), "absent")
			require.NoError(t, err)
			require.False(t, found)
			query, _, _ := scope.Query().Locate(nil).Value(ctx, reflect.TypeFor[[]string](), "value")
			require.Equal(t, []string{"query", "second"}, query)
			raw, err := io.ReadAll(req.Body)
			require.NoError(t, err)
			require.Equal(t, tc.body, string(raw))
			require.Nil(t, req.Form, "do not mutate the original request form")
		})
	}
}

func TestHTTPMultipartFormDoesNotInheritQuery(t *testing.T) {
	var body bytes.Buffer
	w := multipart.NewWriter(&body)
	require.NoError(t, w.WriteField("value", "body"))
	require.NoError(t, w.Close())
	req := httptest.NewRequest("POST", "/?value=query&queryOnly=1", &body)
	req.Header.Set("Content-Type", w.FormDataContentType())
	scope, err := newHTTPRequestScope(req, nil)
	require.NoError(t, err)
	defer scope.Close()
	for _, p := range scope.Providers() {
		if p.Kind() == "form" {
			l := p.Locate(nil)
			value, found, err := l.Value(context.Background(), reflect.TypeFor[string](), "value")
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, "body", value)
			_, found, err = l.Value(context.Background(), reflect.TypeFor[string](), "queryOnly")
			require.NoError(t, err)
			require.False(t, found)
		}
	}
}
