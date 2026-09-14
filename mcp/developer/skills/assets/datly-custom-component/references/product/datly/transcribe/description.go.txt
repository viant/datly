package transcribe

import (
	"context"
	"strings"

	"github.com/viant/datly/spec"
	xdocs "github.com/viant/xdatly/docs"
)

func enrichDescription(ctx context.Context, component *spec.Component, docs xdocs.Service) {
	if component == nil || docs == nil || component.Settings == nil {
		return
	}
	if component.Settings.Generation == nil {
		return
	}
	key := strings.TrimSpace(component.Settings.Generation.DescriptionResource)
	if key == "" {
		return
	}
	if description, ok, err := docs.Lookup(ctx, key); err == nil && ok && strings.TrimSpace(description) != "" {
		component.Description = description
	}
}
