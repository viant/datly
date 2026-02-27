package warmup

import (
	"context"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/viant/datly/internal/tests"
	"github.com/viant/datly/service/reader"
	"github.com/viant/datly/view"
)

func TestPopulateCache(t *testing.T) {
	if os.Getenv("DATLY_RUN_WARMUP_TESTS") == "" {
		t.Skip("set DATLY_RUN_WARMUP_TESTS=1 to run warmup integration test")
	}

	testCases := []struct {
		description      string
		URL              string
		expectedInserted int
		metaIndexed      []interface{}
	}{
		{
			description:      "basic",
			URL:              "case001",
			expectedInserted: 30,
		},
		{
			description:      "template meta",
			URL:              "case002",
			expectedInserted: 64,
			metaIndexed:      []interface{}{2, 11, 111},
		},
		{
			description:      "cache connector",
			URL:              "case003",
			expectedInserted: 64,
			metaIndexed:      []interface{}{2, 11, 111},
		},
		{
			description:      "cache connector",
			URL:              "case004",
			expectedInserted: 8,
		},
		{
			description:      "parent join on",
			URL:              "case005",
			expectedInserted: 2,
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

		resource, err := view.NewResourceFromURL(context.TODO(), resourcePath, nil, nil)
		if !assert.Nil(t, err, testCase.description) {
			continue
		}

		var views []*view.View
		for _, item := range resource.Views {
			views = append(views, item)
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
		build, err := builder.CacheSQL(ctx, aView, cacheInput.Selector)
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

		if cacheInput.IndexMeta && aView.Template.Summary != nil {
			metaIndex, err := builder.CacheMetaSQL(ctx, aView, cacheInput.Selector, &view.BatchData{
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
