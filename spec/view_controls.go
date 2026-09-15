package spec

const (
	ViewControlUseConnector         = "use_connector"
	ViewControlSetLimit             = "set_limit"
	ViewControlOrderBy              = "order_by"
	ViewControlUseCache             = "use_cache"
	ViewControlCacheWarmup          = "cache_warmup"
	ViewControlAllowNulls           = "allow_nulls"
	ViewControlGroupable            = "groupable"
	ViewControlGrouping             = "grouping_enabled"
	ViewControlAllowedOrder         = "allowed_order_by_columns"
	ViewControlCardinality          = "cardinality"
	ViewControlSelfRef              = "self_ref"
	ViewControlType                 = "type"
	ViewControlDest                 = "dest"
	ViewControlBatchSize            = "batch_size"
	ViewControlBatchConcurrency     = "batch_concurrency"
	ViewControlMatch                = "match_strategy"
	ViewControlPartitioner          = "set_partitioner"
	ViewControlPublish              = "publish_parent"
	ViewControlConcurrency          = "relational_concurrency"
	ViewControlEntityHooks          = "lifecycle_type"
	ViewControlSelectorFields       = "selector_fields"
	ViewControlSelectorOrderBy      = "selector_order_by"
	ViewControlSelectorCriteria     = "selector_criteria"
	ViewControlSelectorLimit        = "selector_limit"
	ViewControlSelectorOffset       = "selector_offset"
	ViewControlSelectorPage         = "selector_page"
	ViewControlSelectorDefaultOrder = "selector_default_order"
	ViewControlSelectorDefaultLimit = "selector_default_limit"
	ViewControlSelectorNoLimit      = "selector_no_limit"
	ViewControlSelectorFilterable   = "selector_filterable"
	ViewControlSelectorNamespace    = "selector_namespace"
	ViewControlSelectorSQLMethods   = "selector_sql_methods"
)

// ViewControls are the query-shaping controls derived from authored SQL-ish
// view-control calls and overlaid by the runtime selector. They govern the
// result-set shape only: they are applied to the SQL text at prepare time and
// do not flow into the prepared artifact (see ViewBindings for the
// resource/cache controls that do). Limit and Offset are typed numeric
// controls: a nil pointer means "unset" (no clause is emitted), which keeps
// "unset" distinct from an explicit zero (LIMIT 0/OFFSET 0).
type ViewControls struct {
	OrderBy string `json:"orderBy,omitempty"`
	Limit   *int   `json:"limit,omitempty"`
	Offset  *int   `json:"offset,omitempty"`
}

func (c *ViewControls) Clone() *ViewControls {
	if c == nil {
		return nil
	}
	cloned := *c
	if c.Limit != nil {
		limit := *c.Limit
		cloned.Limit = &limit
	}
	if c.Offset != nil {
		offset := *c.Offset
		cloned.Offset = &offset
	}
	return &cloned
}

func (c *ViewControls) IsZero() bool {
	return c == nil || (c.OrderBy == "" && c.Limit == nil && c.Offset == nil)
}
