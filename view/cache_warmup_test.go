package view

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/viant/datly/view/state"
)

func TestResolveWarmupValue_RelativeDates(t *testing.T) {
	prevNow := warmupNow
	warmupNow = func() time.Time {
		return time.Date(2026, time.June, 30, 23, 30, 0, 0, time.UTC)
	}
	defer func() {
		warmupNow = prevNow
	}()

	param := &state.Parameter{DateFormat: "2006-01-02"}

	today := resolveWarmupValue("@today", param, nil)
	yesterday := resolveWarmupValue("@yesterday", param, nil)

	assert.Regexp(t, `^\d{4}-\d{2}-\d{2}$`, today)
	assert.Regexp(t, `^\d{4}-\d{2}-\d{2}$`, yesterday)
	assert.NotEqual(t, today, yesterday)
}

func TestResolveWarmupValue_PreservesLiteralValues(t *testing.T) {
	param := &state.Parameter{DateFormat: "2006-01-02"}

	assert.Equal(t, "today", resolveWarmupValue("today", param, nil))
	assert.Equal(t, "@unsupported", resolveWarmupValue("@unsupported", param, nil))
	assert.Equal(t, 7, resolveWarmupValue(7, param, nil))
}

func TestResolveWarmupValue_UsesParameterTimezone(t *testing.T) {
	prevNow := warmupNow
	warmupNow = func() time.Time {
		return time.Date(2026, time.July, 1, 0, 30, 0, 0, time.UTC)
	}
	defer func() {
		warmupNow = prevNow
	}()

	param := &state.Parameter{
		DateFormat: "2006-01-02",
		Tag:        `format:"dateFormat=2006-01-02,tz=America/New_York"`,
	}

	location, err := warmupLocation(param)
	assert.NoError(t, err)
	assert.NotNil(t, location)
	assert.Equal(t, "2026-06-30", resolveWarmupValue("@today", param, location))
	assert.Equal(t, "2026-06-29", resolveWarmupValue("@yesterday", param, location))
}

func TestWarmupLocation_RejectsInvalidTimezone(t *testing.T) {
	param := &state.Parameter{
		Name: "From",
		Tag:  `format:"dateFormat=2006-01-02,tz=America/NewYork"`,
	}

	location, err := warmupLocation(param)
	assert.Nil(t, location)
	assert.Error(t, err)
}

func TestWarmupLocation_RejectsInvalidFormatTag(t *testing.T) {
	param := &state.Parameter{
		Name: "From",
		Tag:  `format:"bogus=value"`,
	}

	location, err := warmupLocation(param)
	assert.Nil(t, location)
	assert.Error(t, err)
}

func TestInitWarmup_RejectsInvalidTimezoneOnGeneratedOptionalParam(t *testing.T) {
	param := state.NewParameter("From", state.NewQueryLocation("from"))
	param.Tag = `format:"dateFormat=2006-01-02,tz=America/NewYork"`
	aView := &View{
		Name: "events",
		Columns: []*Column{
			{Name: "event_type_id"},
		},
		Template: NewTemplate("", WithTemplateParameters(param)),
		Cache: &Cache{
			Warmup: &Warmup{IndexColumn: "event_type_id"},
		},
	}
	aView.Template._parametersIndex = aView.Template.Parameters.Index()
	aView.indexColumns()
	aView.Cache.owner = aView

	err := aView.Cache.initWarmup(context.Background(), EmptyResource())

	assert.Error(t, err)
}

func TestInitWarmup_SetsTimezoneOnGeneratedOptionalParam(t *testing.T) {
	param := state.NewParameter("From", state.NewQueryLocation("from"))
	param.Tag = `format:"dateFormat=2006-01-02,tz=America/New_York"`
	aView := &View{
		Name: "events",
		Columns: []*Column{
			{Name: "event_type_id"},
		},
		Template: NewTemplate("", WithTemplateParameters(param)),
		Cache: &Cache{
			Warmup: &Warmup{IndexColumn: "event_type_id"},
		},
	}
	aView.Template._parametersIndex = aView.Template.Parameters.Index()
	aView.indexColumns()
	aView.Cache.owner = aView

	err := aView.Cache.initWarmup(context.Background(), EmptyResource())

	assert.NoError(t, err)
	if assert.Len(t, aView.Cache.Warmup.Cases, 1) && assert.Len(t, aView.Cache.Warmup.Cases[0].Set, 1) {
		assert.NotNil(t, aView.Cache.Warmup.Cases[0].Set[0]._location)
	}
}
