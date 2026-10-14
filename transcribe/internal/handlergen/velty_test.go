package handlergen

import (
	"errors"
	"strings"
	"testing"

	"github.com/viant/datly/spec"
	gen "github.com/viant/datly/transcribe/generate"
	"github.com/viant/datly/transcribe/internal/handlertest"
)

func generateVeltyTarget(component *spec.Component, bindings gen.ViewBindings, options *HandlerOptions) (*gen.VeltyHandlerAsset, error) {
	if options == nil {
		return nil, errors.New("handler options are required")
	}
	compiled := &Context{Component: component, ViewBindings: bindings}
	input := &gen.Input{Component: component, ViewBindings: bindings}
	generation := New(compiled, input, Options{Handler: *options})
	semantic, err := generation.compilePlan()
	if err != nil {
		return nil, err
	}
	input.SetMarkerViews = generation.setMarkerViews(semantic)
	if err = generation.prepareVelty(semantic); err != nil {
		return nil, err
	}
	return input.VeltyHandler, nil
}

func TestGenerateVeltyPostManyFromCanonicalMetadata(t *testing.T) {
	component := handlertest.Component()
	component.Routes = []*spec.Route{{Method: "GET", Path: "/events"}}
	asset, err := generateVeltyTarget(component, nil, &HandlerOptions{Operation: WritePost})
	if err != nil {
		t.Fatalf("generateVeltyTarget() error = %v", err)
	}
	want := `$sequencer.Allocate("EVENTS", $Input.Events, "Id")
#foreach($RecEvents in $Input.Events)
  #set($datlyWriteIndex0 = $foreach.Index)
  #if($writeHooks.Present("Events", $datlyWriteIndex0))
    $writeHooks.Entity("Events", $datlyWriteIndex0)
    #set($RecEvents = $Input.Events[$datlyWriteIndex0])
    $dml.Insert("EVENTS", $writeHooks.Value("Events", $datlyWriteIndex0));
  #end
#end
#set($Output.Data = $Input.Events)`
	if asset == nil || asset.Template != want {
		t.Fatalf("template:\n%s\nwant:\n%s", asset.Template, want)
	}
}

func TestGenerateVeltyPutOneFromCanonicalMetadata(t *testing.T) {
	component := handlertest.Component()
	component.Parameters[0].Cardinality = string(spec.CardinalityOne)
	component.Parameters[0].TypeExpr = "*EventsView"
	component.Parameters[1].TypeExpr = "*EventsView"
	asset, err := generateVeltyTarget(component, nil, &HandlerOptions{Operation: WritePut})
	if err != nil {
		t.Fatalf("generateVeltyTarget() error = %v", err)
	}
	want := `#if($writeHooks.Present("Events"))
  $writeHooks.Entity("Events")
  #set($RecEvents = $Input.Events)
  $dml.Update("EVENTS", $writeHooks.Value("Events"));
#end
#set($Output.Data = $Input.Events)`
	if asset == nil || asset.Template != want {
		t.Fatalf("template:\n%s\nwant:\n%s", asset.Template, want)
	}
}

func TestGenerateVeltyPatchManyFromCanonicalMetadata(t *testing.T) {
	component := handlertest.Component()
	asset, err := generateVeltyTarget(component, handlertest.Bindings(t, component, handlertest.Binding{Param: 3, View: 0}), &HandlerOptions{Operation: WritePatch, Current: "CurrentEvents"})
	if err != nil {
		t.Fatalf("generateVeltyTarget() error = %v", err)
	}
	want := `$sequencer.Allocate("EVENTS", $Input.Events, "Id")
#set($CurrentEventsById = $Input.CurrentEvents.IndexBy("Id"))
#foreach($RecEvents in $Input.Events)
  #set($datlyWriteIndex0 = $foreach.Index)
  #if($writeHooks.Present("Events", $datlyWriteIndex0))
    $writeHooks.Entity("Events", $datlyWriteIndex0)
    #set($RecEvents = $Input.Events[$datlyWriteIndex0])
    #if($CurrentEventsById.HasKey($RecEvents.Id) == true)
      $dml.Update("EVENTS", $writeHooks.Value("Events", $datlyWriteIndex0));
    #else
      $dml.Insert("EVENTS", $writeHooks.Value("Events", $datlyWriteIndex0));
    #end
  #end
#end
#set($Output.Data = $Input.Events)`
	if asset == nil || asset.Template != want {
		t.Fatalf("template:\n%s\nwant:\n%s", asset.Template, want)
	}
}

func TestGenerateVeltyPatchManyWithCompoundKey(t *testing.T) {
	component := handlertest.Component()
	tenant := &spec.Column{Name: "TENANT_ID", Source: "TENANT_ID", Type: spec.TypeRef{Name: "int64"}, PrimaryKey: true}
	component.RootView.Columns = append(component.RootView.Columns, tenant)
	component.Views[0].Columns = append(component.Views[0].Columns, tenant.Clone())
	asset, err := generateVeltyTarget(component, handlertest.Bindings(t, component, handlertest.Binding{Param: 3, View: 0}), &HandlerOptions{Operation: WritePatch, Current: "CurrentEvents"})
	if err != nil {
		t.Fatalf("generateVeltyTarget() error = %v", err)
	}
	want := `$index.Build("CurrentEventsByIdAndTenantId", $Input.CurrentEvents, $Input, "Events", "Id", "Id", "TenantId", "TenantId")
#foreach($RecEvents in $Input.Events)
  #set($datlyWriteIndex0 = $foreach.Index)
  #if($writeHooks.Present("Events", $datlyWriteIndex0))
    $writeHooks.Entity("Events", $datlyWriteIndex0)
    #set($RecEvents = $Input.Events[$datlyWriteIndex0])
    #if($index.Has("CurrentEventsByIdAndTenantId", $RecEvents) == true)
      $dml.Update("EVENTS", $writeHooks.Value("Events", $datlyWriteIndex0));
    #else
      $dml.Insert("EVENTS", $writeHooks.Value("Events", $datlyWriteIndex0));
    #end
  #end
#end
#set($Output.Data = $Input.Events)`
	if asset == nil || asset.Template != want {
		t.Fatalf("template:\n%s\nwant:\n%s", asset.Template, want)
	}
}

func TestGenerateVeltyPatchOneFromCanonicalMetadata(t *testing.T) {
	component := handlertest.Component()
	component.Parameters[0].Cardinality = string(spec.CardinalityOne)
	component.Parameters[0].TypeExpr = "*EventsView"
	component.Parameters[1].TypeExpr = "*EventsView"
	asset, err := generateVeltyTarget(component, handlertest.Bindings(t, component, handlertest.Binding{Param: 3, View: 0}), &HandlerOptions{Operation: WritePatch, Current: "CurrentEvents"})
	if err != nil {
		t.Fatalf("generateVeltyTarget() error = %v", err)
	}
	want := `$sequencer.Allocate("EVENTS", $Input.Events, "Id")
#set($CurrentEventsById = $Input.CurrentEvents.IndexBy("Id"))
#if($writeHooks.Present("Events"))
  $writeHooks.Entity("Events")
  #set($RecEvents = $Input.Events)
  #if($CurrentEventsById.HasKey($RecEvents.Id) == true)
    $dml.Update("EVENTS", $writeHooks.Value("Events"));
  #else
    $dml.Insert("EVENTS", $writeHooks.Value("Events"));
  #end
#end
#set($Output.Data = $Input.Events)`
	if asset == nil || asset.Template != want {
		t.Fatalf("template:\n%s\nwant:\n%s", asset.Template, want)
	}
}

func TestGenerateVeltyPatchManyWithCanonicalChildRelation(t *testing.T) {
	component, child, detail := handlertest.RecursiveComponent()
	childIdentity, err := child.Identity()
	if err != nil {
		t.Fatal(err)
	}
	detailIdentity, err := detail.Identity()
	if err != nil {
		t.Fatal(err)
	}
	asset, err := generateVeltyTarget(component, handlertest.Bindings(t, component,
		handlertest.Binding{Param: 2, View: 0},
		handlertest.Binding{Param: 3, View: 1},
		handlertest.Binding{Param: 4, View: 2}), &HandlerOptions{
		Operation: WritePatch, Current: "CurrentOrders",
		Currents: []CurrentBinding{
			{ViewIdentity: childIdentity, Param: "CurrentItems"},
			{ViewIdentity: detailIdentity, Param: "CurrentDetails"},
		},
	})
	if err != nil {
		t.Fatalf("generateVeltyTarget() error = %v", err)
	}
	want := `$sequencer.Allocate("ORDERS", $Input.Orders, "Id")
$sequencer.Allocate("ITEMS", $Input.Orders, "Items/Id")
$sequencer.Allocate("DETAILS", $Input.Orders, "Items/Details/Id")
#set($CurrentOrdersById = $Input.CurrentOrders.IndexBy("Id"))
#set($CurrentItemsById = $Input.CurrentItems.IndexBy("Id"))
#set($CurrentDetailsById = $Input.CurrentDetails.IndexBy("Id"))
#foreach($RecOrders in $Input.Orders)
  #set($datlyWriteIndex0 = $foreach.Index)
  #if($writeHooks.Present("Orders", $datlyWriteIndex0))
    $writeHooks.Entity("Orders", $datlyWriteIndex0)
    #set($RecOrders = $Input.Orders[$datlyWriteIndex0])
    #if($CurrentOrdersById.HasKey($RecOrders.Id) == true)
      $dml.Update("ORDERS", $writeHooks.Value("Orders", $datlyWriteIndex0));
    #else
      $dml.Insert("ORDERS", $writeHooks.Value("Orders", $datlyWriteIndex0));
    #end
    $writeHooks.Relation("Orders", "Items", $datlyWriteIndex0)
    #set($RecOrders = $Input.Orders[$datlyWriteIndex0])
    #foreach($RecItems in $Input.Orders[$datlyWriteIndex0].Items)
      #set($datlyWriteIndex1 = $foreach.Index)
      #if($writeHooks.Present("Orders/Items", $datlyWriteIndex0, $datlyWriteIndex1))
        $writeHooks.Require($Input.Orders[$datlyWriteIndex0].Id, "Id", "Items")
        $writeHooks.Link($writeHooks.Value("Orders/Items", $datlyWriteIndex0, $datlyWriteIndex1), $writeHooks.Value("Orders", $datlyWriteIndex0), "OrderId", "Id")
        $writeHooks.Mark("Orders/Items", "OrderId", $datlyWriteIndex0, $datlyWriteIndex1)
        $writeHooks.Entity("Orders/Items", $datlyWriteIndex0, $datlyWriteIndex1)
        #set($RecItems = $Input.Orders[$datlyWriteIndex0].Items[$datlyWriteIndex1])
        #if($CurrentItemsById.HasKey($RecItems.Id) == true)
          $dml.Update("ITEMS", $writeHooks.Value("Orders/Items", $datlyWriteIndex0, $datlyWriteIndex1));
        #else
          $dml.Insert("ITEMS", $writeHooks.Value("Orders/Items", $datlyWriteIndex0, $datlyWriteIndex1));
        #end
        $writeHooks.Relation("Orders/Items", "Details", $datlyWriteIndex0, $datlyWriteIndex1)
        #set($RecItems = $Input.Orders[$datlyWriteIndex0].Items[$datlyWriteIndex1])
        #foreach($RecDetails in $Input.Orders[$datlyWriteIndex0].Items[$datlyWriteIndex1].Details)
          #set($datlyWriteIndex2 = $foreach.Index)
          #if($writeHooks.Present("Orders/Items/Details", $datlyWriteIndex0, $datlyWriteIndex1, $datlyWriteIndex2))
            $writeHooks.Require($Input.Orders[$datlyWriteIndex0].Items[$datlyWriteIndex1].Id, "Id", "Details")
            $writeHooks.Link($writeHooks.Value("Orders/Items/Details", $datlyWriteIndex0, $datlyWriteIndex1, $datlyWriteIndex2), $writeHooks.Value("Orders/Items", $datlyWriteIndex0, $datlyWriteIndex1), "ItemId", "Id")
            $writeHooks.Mark("Orders/Items/Details", "ItemId", $datlyWriteIndex0, $datlyWriteIndex1, $datlyWriteIndex2)
            $writeHooks.Entity("Orders/Items/Details", $datlyWriteIndex0, $datlyWriteIndex1, $datlyWriteIndex2)
            #set($RecDetails = $Input.Orders[$datlyWriteIndex0].Items[$datlyWriteIndex1].Details[$datlyWriteIndex2])
            #if($CurrentDetailsById.HasKey($RecDetails.Id) == true)
              $dml.Update("DETAILS", $writeHooks.Value("Orders/Items/Details", $datlyWriteIndex0, $datlyWriteIndex1, $datlyWriteIndex2));
            #else
              $dml.Insert("DETAILS", $writeHooks.Value("Orders/Items/Details", $datlyWriteIndex0, $datlyWriteIndex1, $datlyWriteIndex2));
            #end
          #end
        #end
      #end
    #end
  #end
#end
#set($Output.Data = $Input.Orders)`
	if asset == nil || asset.Template != want {
		t.Fatalf("template:\n%s\nwant:\n%s", asset.Template, want)
	}
}

func TestGenerateVeltyPatchUsesExactCurrentViewBinding(t *testing.T) {
	component := handlertest.Component()
	component.Views[0].Namespace = "current"
	archive := component.Views[0].Clone()
	archive.Namespace = "archive"
	archive.TypeName = "ArchivedEventsView"
	archive.Columns[0].Type.Name = "string"
	component.Views = append(component.Views, archive)
	currentIdentity, err := component.Views[0].Identity()
	if err != nil {
		t.Fatal(err)
	}
	param := component.Parameters[3]
	asset, err := generateVeltyTarget(component, gen.ViewBindings{param.Identity(): currentIdentity}, &HandlerOptions{Operation: WritePatch, Current: "CurrentEvents"})
	if err != nil || asset == nil {
		t.Fatalf("generateVeltyTarget() = (%+v, %v)", asset, err)
	}
	archiveIdentity, err := archive.Identity()
	if err != nil {
		t.Fatal(err)
	}
	_, err = generateVeltyTarget(component, gen.ViewBindings{param.Identity(): archiveIdentity}, &HandlerOptions{Operation: WritePatch, Current: "CurrentEvents"})
	if err == nil || !strings.Contains(err.Error(), "does not match root type") {
		t.Fatalf("archive binding error = %v", err)
	}
}

func TestGenerateVeltyHandlerFailsClosedOnIncompleteIntent(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*spec.Component)
		intent  HandlerOptions
		match   string
	}{
		{name: "patch current", intent: HandlerOptions{Operation: WritePatch, Current: "Missing"}, match: "current view input"},
		{name: "operation", intent: HandlerOptions{Operation: "delete"}, match: "unsupported"},
		{name: "input", intent: HandlerOptions{Operation: WritePost, Input: "Missing"}, match: "body input"},
		{name: "output", intent: HandlerOptions{Operation: WritePost, Output: "Missing"}, match: "output"},
		{name: "status output", intent: HandlerOptions{Operation: WritePost, Output: "Status"}, match: "output"},
		{name: "table", intent: HandlerOptions{Operation: WritePost}, prepare: func(component *spec.Component) {
			component.RootView.Source.Table = ""
		}, match: "table"},
		{name: "key", intent: HandlerOptions{Operation: WritePost, Key: "Missing"}, match: "key"},
		{name: "put without key", intent: HandlerOptions{Operation: WritePut}, prepare: func(component *spec.Component) {
			component.RootView.Columns[0].PrimaryKey = false
		}, match: "primary key"},
		{name: "ambiguous input", intent: HandlerOptions{Operation: WritePost}, prepare: func(component *spec.Component) {
			component.Parameters = append(component.Parameters, &spec.Parameter{Name: "Other", Source: spec.BindSource{Kind: "body", Name: "Other"}, TypeExpr: "*EventsView"})
		}, match: "ambiguous"},
		{name: "ambiguous current", intent: HandlerOptions{Operation: WritePatch}, prepare: func(component *spec.Component) {
			component.Parameters = append(component.Parameters, &spec.Parameter{Name: "OtherCurrent", Source: spec.BindSource{Kind: "view", Name: "OtherCurrent"}, TypeExpr: "[]*EventsView", Cardinality: string(spec.CardinalityMany)})
		}, match: "ambiguous"},
		{name: "current one", intent: HandlerOptions{Operation: WritePatch, Current: "CurrentEvents"}, prepare: func(component *spec.Component) {
			component.Parameters[3].Cardinality = string(spec.CardinalityOne)
			component.Parameters[3].TypeExpr = "*EventsView"
		}, match: "many cardinality"},
		{name: "current view missing key", intent: HandlerOptions{Operation: WritePatch, Current: "CurrentEvents"}, prepare: func(component *spec.Component) {
			component.Views[0].Columns = component.Views[0].Columns[1:]
		}, match: "has no root key"},
		{name: "current key source mismatch", intent: HandlerOptions{Operation: WritePatch, Current: "CurrentEvents"}, prepare: func(component *spec.Component) {
			component.Views[0].Columns[0].Source = "OTHER_ID"
		}, match: "does not match root source"},
		{name: "non-primary patch key", intent: HandlerOptions{Operation: WritePatch, Current: "CurrentEvents", Key: "ALT_ID"}, prepare: func(component *spec.Component) {
			component.RootView.Columns = append(component.RootView.Columns, &spec.Column{Name: "ALT_ID", Source: "ALT_ID", Type: spec.TypeRef{Name: "int64"}})
		}, match: "not the canonical primary key"},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			component := handlertest.Component()
			if testCase.prepare != nil {
				testCase.prepare(component)
			}
			viewBindings := handlertest.Bindings(t, component, handlertest.Binding{Param: 3, View: 0})
			_, err := generateVeltyTarget(component, viewBindings, &testCase.intent)
			if err == nil || !strings.Contains(err.Error(), testCase.match) {
				t.Fatalf("generateVeltyTarget() error = %v, want %q", err, testCase.match)
			}
		})
	}
}
