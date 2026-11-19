package path

import (
	"context"
	_ "embed"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/viant/afs"
	"github.com/viant/afs/asset"
	"github.com/viant/afs/file"
	"github.com/viant/datly/repository/contract"
	"log"
	"testing"
	"time"
)

//go:embed testdata/vars.yaml
var ruleVars []byte

//go:embed testdata/vendor_hauth.yaml
var ruleVendor []byte

//go:embed testdata/.mete/vendor_hauth.yaml
var ruleVendorMeta []byte

func TestNew(t *testing.T) {

	var testCases = []struct {
		description string
		location    string
		assets      []*asset.Resource
	}{
		{

			description: "create paths",
			assets: []*asset.Resource{
				asset.New("dev/vars.yaml", file.DefaultFileOsMode, false, "", ruleVars),
				asset.New("dev/vendor.yml", file.DefaultFileOsMode, false, "", ruleVendor),
				asset.New("dev/.meta/vendor.yaml", file.DefaultFileOsMode, false, "", ruleVendorMeta),
			},
			location: "mem://localhost/test/routes",
		},
	}

	for _, useCase := range testCases {
		mgr, err := afs.Manager(useCase.location)
		if err != nil {
			log.Fatal(err)
		}
		err = asset.Create(mgr, useCase.location, useCase.assets)
		if err != nil {
			log.Fatal(err)
		}
		multiPaths, err := New(context.Background(), afs.New(), useCase.location, time.Second)
		if !assert.Nil(t, err, useCase.description) {
			continue
		}
		aPath := &contract.Path{URI: "/v1/api/ws/vars", Method: "GET"}
		element := multiPaths.Lookup(aPath)
		if !assert.NotNil(t, element, useCase.description) {
			continue
		}
		assert.Equal(t, element.Method, aPath.Method)
		assert.Equal(t, element.URI, aPath.URI)
	}

}

func TestService_onModify_DoesNotAppendDuplicateForTrackedURL(t *testing.T) {
	location := "mem://localhost/test/routes_modify"
	mgr, err := afs.Manager(location)
	require.NoError(t, err)
	err = asset.Create(mgr, location, []*asset.Resource{
		asset.New("dev/vendor.yml", file.DefaultFileOsMode, false, "", ruleVendor),
	})
	require.NoError(t, err)

	service, err := New(context.Background(), afs.New(), location, time.Second)
	require.NoError(t, err)
	require.Len(t, service.Container.Items, 1)

	fs := afs.New()
	object, err := fs.Object(context.Background(), "mem://localhost/test/routes_modify/dev/vendor.yml")
	require.NoError(t, err)

	err = service.onModify(context.Background(), object)
	require.NoError(t, err)

	require.Len(t, service.Container.Items, 1)
	aPath := &contract.Path{URI: "/v1/api/dev/hauth/vendors/{vendorID}", Method: "GET"}
	element := service.Lookup(aPath)
	require.NotNil(t, element)
}
