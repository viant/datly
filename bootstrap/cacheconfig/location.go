package cacheconfig

import (
	"github.com/viant/datly/spec"
	"strings"
)

// ExpandLocation expands view metadata before application constants are resolved.
// Alias is the canonical SQL namespace; Table comes from the view source.
func ExpandLocation(location string, view *spec.View) string {
	if view == nil {
		return location
	}
	table := ""
	if view.Source != nil {
		table = view.Source.Table
	}
	location = strings.ReplaceAll(location, `${View\.`, `${View.`)
	return strings.NewReplacer("${View.Name}", view.CanonicalName(), "$View.Name", view.CanonicalName(), "${View.Alias}", view.Namespace, "$View.Alias", view.Namespace, "${View.Table}", table, "$View.Table", table).Replace(location)
}
