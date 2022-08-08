package warmup

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs"
	"github.com/viant/datly/internal/tests"
	"github.com/viant/datly/router"
	"github.com/viant/datly/view"
	"path"
	"testing"
)

func TestPopulateCache(t *testing.T) {
	return
	testCases := []struct {
		description string
		URL         string
	}{
		{
			description: "basic",
			URL:         "case001",
		},
	}

	for _, testCase := range testCases {
		dataPath := path.Join("testdata", testCase.URL, "populate")
		configPath := path.Join("testdata", "db_config.yaml")

		if !tests.InitDB(t, configPath, dataPath, "db") {
			continue
		}

		resourcePath := path.Join("testdata", testCase.URL, "resource.yaml")

		resource, err := router.NewResourceFromURL(context.TODO(), afs.New(), resourcePath, false)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		var views []*view.View
		for _, route := range resource.Routes {
			views = append(views, route.View)
		}

		assert.Nil(t, PopulateCache(views), testCase.description)
	}
}
