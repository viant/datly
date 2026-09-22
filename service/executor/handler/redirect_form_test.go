package handler

import (
	"testing"

	hstate "github.com/viant/xdatly/handler/state"
)

func TestRedirectFormPreservesDelegatedFormValues(t *testing.T) {
	form := hstate.NewForm()
	form.Set("campaign_id", "532743")
	options := hstate.NewOptions(hstate.WithForm(form))

	actual := redirectForm(options)
	if actual != form || actual.Get("campaign_id") != "532743" {
		t.Fatalf("delegated form was replaced or cleared: %#v", actual)
	}
}

func TestRedirectFormDefaultsToEmptyForm(t *testing.T) {
	actual := redirectForm(hstate.NewOptions())
	if actual == nil || len(actual.Values) != 0 {
		t.Fatalf("expected a non-nil empty form, got %#v", actual)
	}
}
