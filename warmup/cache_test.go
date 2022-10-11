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
		metaIndexed      []interface{}
	}{
		{
			description:      "basic",
			URL:              "case001",
			expectedInserted: 18,
		},
		{
			description:      "template meta",
			URL:              "case002",
			expectedInserted: 36,
			metaIndexed:      []interface{}{2, 11, 111},
		},
		{
			description:      "cache connector",
			URL:              "case003",
			expectedInserted: 36,
			metaIndexed:      []interface{}{2, 11, 111},
		},
		{
			description:      "cache connector",
			URL:              "case004",
			expectedInserted: 2,
		},
		{
			description:      "parent join on",
			URL:              "case005",
			expectedInserted: 1,
		},
	}

	//for _, testCase := range testCases[len(testCases)-1:] {
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
			assert.Nil(t, checkIfCached(t, cache, ctx, testCase, aView), testCase.description)
		}
	}
}

func checkIfCached(t *testing.T, cache *view.Cache, ctx context.Context, testCase struct {
	description      string
	URL              string
	expectedInserted int
	metaIndexed      []interface{}
}, aView *view.View) error {
	input, err := cache.GenerateCacheInput(ctx)
	if !assert.Nil(t, err, testCase.description) {
		return err
	}

	service, err := cache.Service()
	if !assert.Nil(t, err, testCase.description) {
		return err
	}

	builder := reader.NewBuilder()

	for _, cacheInput := range input {
		build, err := builder.CacheSQL(aView, cacheInput.Selector)
		if err != nil {
			return err
		}

		build.By = cacheInput.Column
		entry, err := service.Get(ctx, build.SQL, build.Args, build)
		if err != nil {
			return err
		}

		if assert.True(t, entry.Has(), testCase.description) {
			assert.Nil(t, service.Close(ctx, entry), testCase.description)
		}

		if cacheInput.IndexMeta && aView.Template.Meta != nil {
			metaIndex, err := builder.CacheMetaSQL(aView, cacheInput.Selector, &view.BatchData{
				ValuesBatch: testCase.metaIndexed,
				Values:      testCase.metaIndexed,
			}, nil, nil)
			if !assert.Nil(t, err, testCase.description) {
				continue
			}

			metaIndex.By = cacheInput.MetaColumn
			metaEntry, err := service.Get(ctx, metaIndex.SQL, metaIndex.Args, metaIndex)
			if !assert.Nil(t, err, testCase.description) {
				continue
			}

			if assert.True(t, metaEntry.Has(), testCase.description) {
				assert.Nil(t, service.Close(ctx, metaEntry), testCase.description)
			}
		}
	}

	return nil
}
