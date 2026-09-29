package router

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/view"
)

func TestAppendCacheWarmupViews_RecognizesEitherWarmupForm(t *testing.T) {
	singular := &view.View{Cache: &view.Cache{Warmup: &view.Warmup{IndexColumn: "advertiser_id"}}}
	plural := &view.View{Cache: &view.Cache{Warmups: []*view.Warmup{{IndexColumn: "campaign_id"}}}}
	none := &view.View{Cache: &view.Cache{}}

	var result []*view.View
	appendCacheWarmupViews(singular, &result)
	appendCacheWarmupViews(plural, &result)
	appendCacheWarmupViews(none, &result)

	require.Len(t, result, 2)
	require.Same(t, singular, result[0])
	require.Same(t, plural, result[1])
}
