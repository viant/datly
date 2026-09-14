package reader

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/data"
	"github.com/viant/datly/sql/criteria"
	"github.com/viant/datly/sql/reader/collector"
	"github.com/viant/datly/sql/reader/rowcodec"
	sqltemplate "github.com/viant/datly/sql/template"
	xshape "github.com/viant/x/shape"
	xreader "github.com/viant/xdatly/reader"
)

// ViewPlan is immutable execution metadata for one view in a reader graph.
// Runtime services consume this node directly instead of joining parallel maps
// of view-scoped behavior on every read.
type ViewPlan struct {
	View        *data.View
	Template    sqltemplate.Evaluator
	Criteria    *criteria.Compiler
	Connector   string
	Partitioner xreader.Partitioner
	Collector   *collector.View
	Codec       *rowcodec.Plan
	Relations   []*RelationPlan
}

// RelationPlan is one graph edge from a parent view plan to its target plan.
// Target may point to an existing shared node. Executable dependency edges
// remain acyclic; recursive row data uses the view's self-reference metadata.
type RelationPlan struct {
	Relation *data.Relation
	Target   *ViewPlan
}

// Plan is immutable reader execution metadata compiled during bootstrap.
type Plan struct {
	SelectorBindings   []SelectorBindingPlan
	ViewIndex          *ViewIndex
	OutputViewField    string
	OutputMetricsField string
	DirectOutput       bool
	Root               *ViewPlan

	views      map[*data.View]*ViewPlan
	collection *collector.Graph
}

// PlanConfig contains compiler-owned inputs used to assemble one final reader
// plan. The temporary maps do not escape into runtime plan state.
type PlanConfig struct {
	SelectorBindings   []SelectorBindingPlan
	ViewIndex          *ViewIndex
	OutputViewField    string
	OutputMetricsField string
	DirectOutput       bool
	RootView           *data.View
	Templates          map[*data.View]sqltemplate.Evaluator
	Criteria           map[*data.View]*criteria.Compiler
	Partitioners       map[*data.View]xreader.Partitioner
	Collection         *collector.Graph
	Codecs             map[*data.View]*rowcodec.Plan
}

func NewPlan(config PlanConfig) (*Plan, error) {
	views := map[*data.View]*ViewPlan{}
	root, err := assembleViewPlan(config.RootView, config, views)
	if err != nil {
		return nil, err
	}
	if root != nil {
		if err := validateViewPlans(root, map[*ViewPlan]uint8{}); err != nil {
			return nil, err
		}
	}
	return &Plan{
		SelectorBindings:   append([]SelectorBindingPlan(nil), config.SelectorBindings...),
		ViewIndex:          config.ViewIndex,
		OutputViewField:    config.OutputViewField,
		OutputMetricsField: config.OutputMetricsField,
		DirectOutput:       config.DirectOutput,
		Root:               root,
		views:              views,
		collection:         config.Collection,
	}, nil
}

func assembleViewPlan(view *data.View, config PlanConfig, planned map[*data.View]*ViewPlan) (*ViewPlan, error) {
	if view == nil {
		return nil, nil
	}
	if existing := planned[view]; existing != nil {
		return existing, nil
	}
	result := &ViewPlan{
		Criteria: config.Criteria[view],
		View:     view, Template: config.Templates[view], Connector: strings.TrimSpace(view.Connector),
		Partitioner: config.Partitioners[view],
		Codec:       config.Codecs[view],
	}
	if config.Collection != nil {
		result.Collector = config.Collection.View(view)
	}
	planned[view] = result
	for _, relation := range view.Relations {
		if relation == nil || relation.Of == nil || relation.Of.View == nil {
			return nil, fmt.Errorf("view %s has a relation without a target view", viewPlanName(view))
		}
		target, err := assembleViewPlan(relation.Of.View, config, planned)
		if err != nil {
			return nil, err
		}
		result.Relations = append(result.Relations, &RelationPlan{Relation: relation, Target: target})
	}
	return result, nil
}

func (p *Plan) Validate(outputType reflect.Type) error {
	if p == nil {
		return fmt.Errorf("reader plan is required")
	}
	if p.Root == nil || p.Root.View == nil {
		return fmt.Errorf("reader plan root view is required")
	}
	if p.Root.View.Spec.Source == nil || (strings.TrimSpace(p.Root.View.Spec.Source.SQL) == "" && strings.TrimSpace(p.Root.View.Spec.Source.Table) == "") {
		return fmt.Errorf("reader plan root SQL or table is required")
	}
	if p.ViewIndex == nil || p.ViewIndex.root != p.Root.View {
		return fmt.Errorf("reader plan view index is required for its root view")
	}
	if p.collection != nil {
		if err := p.collection.Validate(); err != nil {
			return fmt.Errorf("reader plan collector graph: %w", err)
		}
	}
	if err := validateViewPlans(p.Root, map[*ViewPlan]uint8{}); err != nil {
		return err
	}
	if outputType != nil && (p.OutputViewField != "" || p.DirectOutput) && (p.Root.Collector == nil || p.Root.Collector.Schema.RowType() == nil) {
		return fmt.Errorf("reader plan root schema is required for output field %s", p.OutputViewField)
	}
	return nil
}

func validateViewPlans(view *ViewPlan, visited map[*ViewPlan]uint8) error {
	if view == nil || view.View == nil {
		return fmt.Errorf("reader view plan metadata is required")
	}
	if visited[view] == 1 {
		return fmt.Errorf("reader dependency cycle at view %s; model recursive rows with self-reference metadata", viewPlanName(view.View))
	}
	if visited[view] == 2 {
		return nil
	}
	visited[view] = 1
	defer func() { visited[view] = 2 }()
	if view.View.Spec.BatchConcurrency < 0 {
		return fmt.Errorf("view %s batch concurrency must be non-negative", viewPlanName(view.View))
	}
	if view.Codec != nil && (view.Collector == nil || (xshape.Runtime{}).Indirect(view.Collector.Schema.RowType()) != view.Codec.ModelType()) {
		return fmt.Errorf("column codec row type does not match collector for view %s", viewPlanName(view.View))
	}
	for _, relation := range view.Relations {
		if relation == nil || relation.Relation == nil || relation.Target == nil {
			return fmt.Errorf("reader view plan relation is incomplete for view %s", viewPlanName(view.View))
		}
		if relation.Relation.IsOutput() && relation.Target.Partitioner != nil {
			if _, ok := relation.Target.Partitioner.(xreader.ReducerProvider); !ok {
				return fmt.Errorf("partitioned output relation %s requires a reducer", viewPlanName(relation.Target.View))
			}
		}
		if err := validateViewPlans(relation.Target, visited); err != nil {
			return err
		}
	}
	return nil
}

func (p *Plan) ViewPlanFor(view *data.View) *ViewPlan {
	if p == nil || view == nil {
		return nil
	}
	return p.views[view]
}

func viewPlanName(view *data.View) string {
	if view == nil || strings.TrimSpace(view.Spec.Name) == "" {
		return "<unnamed>"
	}
	return view.Spec.Name
}
