package warmup

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs"
	"github.com/viant/datly/internal/tests"
	"github.com/viant/datly/reader"
	"github.com/viant/datly/router"
	"github.com/viant/datly/view"
	"path"
	"testing"
)

func TestPopulateCache(t *testing.T) {
	testCases := []struct {
		description      string
		URL              string
		expectedInserted int
	}{
		{
			description:      "basic",
			URL:              "case001",
			expectedInserted: 18,
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

		inserted, err := PopulateCache(views)
		assert.Nil(t, err, testCase.description)
		assert.Equal(t, testCase.expectedInserted, inserted, testCase.description)

		for _, aView := range views {
			cache := aView.Cache
			ctx := context.TODO()
			checkIfCached(t, cache, ctx, testCase, aView)
		}
	}
}

func checkIfCached(t *testing.T, cache *view.Cache, ctx context.Context, testCase struct {
	description      string
	URL              string
	expectedInserted int
}, aView *view.View) error {
	input, err := cache.GenerateCacheInput(ctx)
	if !assert.Nil(t, err, testCase.description) {
		return err
	}

	service, err := cache.Service()
	if !assert.Nil(t, err, testCase.description) {
		return err
	}

	for _, cacheInput := range input {
		build, err := reader.NewBuilder().Build(aView, cacheInput.Selector, &view.BatchData{}, nil, &reader.Exclude{
			ColumnsIn:  true,
			Pagination: true,
		}, nil)

		if err != nil {
			return err
		}

		build.IndexBy = cacheInput.Column
		entry, err := service.Get(ctx, build.SQL, build.Args, build)
		if err != nil {
			return err
		}

		assert.True(t, entry.Has(), testCase.description)
		assert.Nil(t, service.Close(ctx, entry), testCase.description)
	}

	return nil
}
