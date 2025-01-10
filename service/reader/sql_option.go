package reader

import (
	"github.com/viant/datly/service/executor/expand"
	"github.com/viant/datly/view"
)

type (
	builderOptions struct {
		view      *view.View
		statelet  *view.Statelet
		batchData *view.BatchData
		relation  *view.Relation
		parent    *expand.ViewContext
		expander  expand.Expander
		exclude   *Exclude
		partition *view.Partition
	}

	Exclude struct {
		ColumnsIn  bool
		Pagination bool
	}

	BuilderOption func(*builderOptions)
)

func newBuilderOptions(options ...BuilderOption) *builderOptions {
	var result = &builderOptions{}
	for _, option := range options {
		option(result)
	}
	if result.exclude == nil {
		result.exclude = &Exclude{}
	}
	if result.expander == nil {
		result.expander = &expand.MockExpander{}
	}
	return result
}

// WithBuilderView sets view
func WithBuilderView(view *view.View) BuilderOption {
	return func(o *builderOptions) {
		o.view = view
	}
}

// WithBuilderStatelet set statelet
func WithBuilderStatelet(statelet *view.Statelet) BuilderOption {
	return func(o *builderOptions) {
		o.statelet = statelet
	}
}

// WithBuilderBatchData with batch data
func WithBuilderBatchData(batchData *view.BatchData) BuilderOption {
	return func(o *builderOptions) {
		o.batchData = batchData
	}
}

// WithBuilderRelation sets relation
func WithBuilderRelation(relation *view.Relation) BuilderOption {
	return func(o *builderOptions) {
		o.relation = relation
	}
}

// WithBuilderParent sets parent
func WithBuilderParent(parent *expand.ViewContext) BuilderOption {
	return func(o *builderOptions) {
		o.parent = parent
	}
}

func WithBuilderPartitions(partition *view.Partition) BuilderOption {
	return func(o *builderOptions) {
		if partition != nil {
			o.partition = partition
		}
	}

}

// WithBuilderExpander sets expander
func WithBuilderExpander(expander expand.Expander) BuilderOption {
	return func(o *builderOptions) {
		o.expander = expander
	}
}

// WithExcludeAll excludes all
func WithBuilderExclude(columnsIn, pagination bool) BuilderOption {
	return func(o *builderOptions) {
		o.exclude = &Exclude{
			ColumnsIn:  columnsIn,
			Pagination: pagination,
		}
	}
}
