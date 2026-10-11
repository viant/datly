package compiler

import (
	"strings"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	dtag "github.com/viant/datly/tag"
)

func applyFieldViewTag(view *data.View, options *dtag.View) {
	if view == nil || options == nil {
		return
	}
	if options.Name != "" {
		view.Spec.Name = options.Name
	}
	view.Connector = options.Connector
	view.Spec.BatchSize = options.Batch
	view.Spec.BatchConcurrency = options.BatchConcurrency
	view.Spec.PublishParent = options.PublishParent
	view.Spec.RelationalConcurrency = options.RelationalConcurrency
	if options.AllowNulls != nil {
		value := *options.AllowNulls
		view.Spec.AllowNulls = &value
	}
	view.Spec.Groupable = nil
	if options.Groupable != nil {
		value := *options.Groupable
		view.Spec.Groupable = &value
	}
	view.Spec.Partitioning = options.Partitioning.Clone()
	view.Spec.Selector = options.Selector.Clone()
	if options.Cache != "" {
		view.Cache = &data.Cache{Name: options.Cache}
	}
	if options.Table != "" {
		if view.Spec.Source == nil {
			view.Spec.Source = &spec.ViewSource{}
		}
		view.Spec.Source.Table = options.Table
	}
	if options.URI != "" {
		if view.Spec.Source == nil {
			view.Spec.Source = &spec.ViewSource{}
		}
		view.Spec.Source.URI = options.URI
	}
	if options.Limit != nil || options.Offset != nil || strings.TrimSpace(options.OrderBy) != "" {
		if view.Spec.Source == nil {
			view.Spec.Source = &spec.ViewSource{}
		}
		view.Spec.Source.Controls = &spec.ViewControls{OrderBy: strings.TrimSpace(options.OrderBy)}
	}
	if options.Limit != nil {
		limit := *options.Limit
		view.Spec.Source.Controls.Limit = &limit
	}
	if options.Offset != nil {
		offset := *options.Offset
		view.Spec.Source.Controls.Offset = &offset
	}
	if strings.TrimSpace(options.CacheWarmup) != "" {
		if view.Spec.Source == nil {
			view.Spec.Source = &spec.ViewSource{}
		}
		if view.Spec.Source.Bindings == nil {
			view.Spec.Source.Bindings = &spec.ViewBindings{}
		}
		view.Spec.Source.Bindings.CacheWarmup = strings.TrimSpace(options.CacheWarmup)
	}
}

func applyFieldSQLTag(view *data.View, source *dtag.SQL) {
	if view == nil || source == nil {
		return
	}
	if view.Spec.Source == nil {
		view.Spec.Source = &spec.ViewSource{}
	}
	if source.URI != "" {
		view.Spec.Source.URI = source.URI
		return
	}
	view.Spec.Source.SQL = source.Text
}

func parseFieldMatchStrategy(value string) data.MatchStrategy {
	if strings.EqualFold(strings.TrimSpace(value), "read_all") {
		return data.MatchReadAll
	}
	return data.MatchSequential
}

func viewMatch(view *dtag.View) string {
	if view == nil {
		return ""
	}
	return view.Match
}
