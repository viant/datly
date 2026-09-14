package reader

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/sql/reader/collector"
	sqltemplate "github.com/viant/datly/sql/template"
	xreader "github.com/viant/xdatly/reader"
)

type planTemplate struct{}

func (planTemplate) Evaluate(context.Context, sqltemplate.Invocation) (sqltemplate.Result, error) {
	return sqltemplate.Result{SQL: "SELECT child"}, nil
}

type planPartitioner struct{}

func (*planPartitioner) Partitions(context.Context, xreader.PartitionRequest) ([]xreader.Partition, error) {
	return nil, nil
}

func TestNewPlanBuildsSharedPerViewExecutionGraph(t *testing.T) {
	child := &data.View{Spec: spec.View{Name: "Child", Source: &spec.ViewSource{SQL: "SELECT child_id FROM children"}}, Connector: " child "}
	root := &data.View{Spec: spec.View{Name: "Root", Source: &spec.ViewSource{SQL: "SELECT id FROM roots"}}, Connector: "root",
		Relations: []*data.Relation{
			{Name: "Left", Of: &data.RelationRef{View: child}},
			{Name: "Right", Of: &data.RelationRef{View: child}},
		},
	}
	collection, err := collector.Compile(root, map[*data.View]reflect.Type{
		root: reflect.TypeOf(struct{ ID int }{}), child: reflect.TypeOf(struct{ ChildID int }{}),
	})
	if err != nil {
		t.Fatal(err)
	}
	template := planTemplate{}
	partitioner := &planPartitioner{}
	component := &spec.Component{Name: "Roots"}
	plan, err := NewPlan(PlanConfig{
		RootView: root, ViewIndex: NewViewIndex(component, root),
		Templates:    map[*data.View]sqltemplate.Evaluator{child: template},
		Partitioners: map[*data.View]xreader.Partitioner{child: partitioner}, Collection: collection,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = plan.Validate(nil); err != nil {
		t.Fatal(err)
	}
	if plan.Root == nil || plan.Root.View != root || plan.Root.Connector != "root" || len(plan.Root.Relations) != 2 {
		t.Fatalf("root plan = %+v", plan.Root)
	}
	left := plan.Root.Relations[0].Target
	right := plan.Root.Relations[1].Target
	if left == nil || left != right || plan.ViewPlanFor(child) != left {
		t.Fatalf("shared child plans diverged: left=%p right=%p indexed=%p", left, right, plan.ViewPlanFor(child))
	}
	if left.View != child || left.Connector != "child" || left.Template != template || left.Partitioner != partitioner || left.Collector != collection.View(child) {
		t.Fatalf("child plan = %+v", left)
	}
	child.Connector = "changed-after-compile"
	if left.Connector != "child" {
		t.Fatalf("compiled connector followed mutable metadata: %q", left.Connector)
	}
}

func TestPlanOwnsExecutionThroughPerViewNodes(t *testing.T) {
	planType := reflect.TypeOf(Plan{})
	if _, ok := planType.FieldByName("Root"); !ok {
		t.Fatal("reader Plan must own a root ViewPlan")
	}
	for _, obsolete := range []string{"View", "SQLPrograms", "Partitioners", "Collector"} {
		if _, ok := planType.FieldByName(obsolete); ok {
			t.Fatalf("reader Plan restored parallel/root-only field %s", obsolete)
		}
	}
	viewPlanType := reflect.TypeOf(ViewPlan{})
	for _, required := range []string{"View", "Template", "Connector", "Partitioner", "Collector", "Relations"} {
		if _, ok := viewPlanType.FieldByName(required); !ok {
			t.Fatalf("reader ViewPlan does not own %s", required)
		}
	}
}
