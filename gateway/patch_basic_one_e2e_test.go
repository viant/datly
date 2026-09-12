package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/repository/contract"
	"github.com/viant/datly/service/operator"
	readerpkg "github.com/viant/datly/service/reader"
	"github.com/viant/datly/service/session"
	"github.com/viant/datly/view/state"
	"github.com/viant/datly/view/state/kind/locator"
)

func TestGateway_PatchBasicOne_NoRefresh(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", "e2e", "v1", "autogen", "Datly", "config_8081.json"))
	svc, err := New(context.Background(),
		WithConfigURL(root),
		WithRefreshDisabled(true),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = svc.Close()
		ResetSingleton()
	})

	req := httptest.NewRequest(http.MethodPatch, "/v1/api/shape/dev/basic/foos", strings.NewReader(`{"ID":4,"Quantity":2500,"Name":"changed - foo 4"}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	svc.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), `"id":4`)
	require.Contains(t, rec.Body.String(), `"quantity":2500`)
	require.Contains(t, rec.Body.String(), `"name":"changed - foo 4"`)
}

func TestGateway_PatchBasicOne_CurFoosValue(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", "e2e", "v1", "autogen", "Datly", "config_8081.json"))
	svc, err := New(context.Background(),
		WithConfigURL(root),
		WithRefreshDisabled(true),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = svc.Close()
		ResetSingleton()
	})

	component, err := svc.repository.Registry().Lookup(context.Background(), contract.NewPath(http.MethodPatch, "/v1/api/shape/dev/basic/foos"))
	require.NoError(t, err)
	resource := component.View.GetResource()
	require.NotNil(t, resource)
	curFoosView, err := resource.GetViews().Lookup("CurFoos")
	require.NoError(t, err)
	require.NotNil(t, curFoosView)

	plainDest := reflect.New(curFoosView.Schema.SliceType()).Interface()
	err = readerpkg.New().ReadInto(context.Background(), plainDest, curFoosView)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPatch, "/v1/api/shape/dev/basic/foos", strings.NewReader(`{"ID":4,"Quantity":2500,"Name":"changed - foo 4"}`))
	req.Header.Set("Content-Type", "application/json")

	unmarshal := component.UnmarshalFunc(req)
	locatorOptions := append(component.LocatorOptions(req, nil, unmarshal))
	locatorOptions = append(locatorOptions, locator.WithLogger(nil))
	aSession := session.New(component.View,
		session.WithComponent(component),
		session.WithLocatorOptions(locatorOptions...),
		session.WithRegistry(svc.repository.Registry()),
		session.WithOperate(operator.New().Operate))

	err = aSession.InitKinds(state.KindComponent, state.KindHeader, state.KindRequestBody, state.KindForm, state.KindQuery)
	require.NoError(t, err)
	err = aSession.Populate(context.Background())
	require.NoError(t, err)

	param, err := component.View.ParamByName("CurFoos")
	require.NoError(t, err)
	value, has, err := aSession.LookupValue(context.Background(), param, aSession.Indirect(true))
	require.NoError(t, err)
	require.True(t, has)
	require.NotNil(t, value)
}

func TestGateway_PatchBasicOne_CurFoosReadInto_WithAndWithoutResourceState(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", "e2e", "v1", "autogen", "Datly", "config_8081.json"))
	svc, err := New(context.Background(),
		WithConfigURL(root),
		WithRefreshDisabled(true),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = svc.Close()
		ResetSingleton()
	})

	component, err := svc.repository.Registry().Lookup(context.Background(), contract.NewPath(http.MethodPatch, "/v1/api/shape/dev/basic/foos"))
	require.NoError(t, err)
	resource := component.View.GetResource()
	require.NotNil(t, resource)
	curFoosView, err := resource.GetViews().Lookup("CurFoos")
	require.NoError(t, err)
	require.NotNil(t, curFoosView)

	req := httptest.NewRequest(http.MethodPatch, "/v1/api/shape/dev/basic/foos", strings.NewReader(`{"ID":4,"Quantity":2500,"Name":"changed - foo 4"}`))
	req.Header.Set("Content-Type", "application/json")

	unmarshal := component.UnmarshalFunc(req)
	locatorOptions := append(component.LocatorOptions(req, nil, unmarshal))
	locatorOptions = append(locatorOptions, locator.WithLogger(nil))
	aSession := session.New(component.View,
		session.WithComponent(component),
		session.WithLocatorOptions(locatorOptions...),
		session.WithRegistry(svc.repository.Registry()),
		session.WithOperate(operator.New().Operate))

	err = aSession.InitKinds(state.KindComponent, state.KindHeader, state.KindRequestBody, state.KindForm, state.KindQuery)
	require.NoError(t, err)

	err = aSession.SetViewState(context.Background(), curFoosView)
	require.NoError(t, err)
	sqlQuery, buildErr := readerpkg.NewBuilder().Build(context.Background(),
		readerpkg.WithBuilderView(curFoosView),
		readerpkg.WithBuilderStatelet(aSession.State().Lookup(curFoosView)),
	)
	require.NoError(t, buildErr)
	require.NotNil(t, sqlQuery)
	t.Logf("curFoos sql=%s args=%#v", sqlQuery.SQL, sqlQuery.Args)

	stateDest := reflect.New(curFoosView.Schema.SliceType()).Interface()
	err = readerpkg.New().ReadInto(context.Background(), stateDest, curFoosView, readerpkg.WithResourceState(aSession.State()))
	require.Error(t, err)
}
