package compiler

import (
	"reflect"
	"testing"

	"github.com/viant/datly/data"
	"github.com/viant/datly/spec"
	rcollector "github.com/viant/datly/sql/reader/collector"
)

func enrichViewForTest(view *data.View, rowType reflect.Type) error {
	_, err := compileViewForTest(view, rowType)
	return err
}

func compileViewForTest(view *data.View, rowType reflect.Type) (*rcollector.Graph, error) {
	rowTypes := map[*data.View]reflect.Type{}
	if err := newViewDeriver(rowTypes).enrich(view, rowType); err != nil {
		return nil, err
	}
	return rcollector.Compile(view, rowTypes)
}

func TestFromComponentBuildsRuntimeView(t *testing.T) {
	component := &spec.Component{
		Key:         spec.Key{Kind: spec.KindComponent, Scope: "example.com/demo", Name: "Users"},
		Name:        "Users",
		Description: "users component",
		Settings: &spec.Settings{
			DefaultConnector: "analytics",
			Cache: &spec.CacheSettings{
				Name: "users-cache",
				Warmup: &spec.CacheWarmupSettings{
					IndexColumn: "user_id",
				},
			},
		},
		RootView: &spec.View{
			Key:  spec.Key{Kind: spec.KindView, Scope: "example.com/demo", Name: "UsersView"},
			Name: "UsersView",
			Source: &spec.ViewSource{
				SQL: "SELECT id, name FROM users",
			},
		},
	}
	view := data.FromComponent(component)
	if view == nil {
		t.Fatalf("expected runtime-facing view")
	}
	if view.Spec.Key != component.RootView.Key || view.Spec.Name != "UsersView" {
		t.Fatalf("unexpected view identity: %+v", view)
	}
	if view.Connector != "analytics" {
		t.Fatalf("expected connector, got %q", view.Connector)
	}
	if view.Cache == nil || view.Cache.Name != "users-cache" || view.Cache.Warmup == nil || view.Cache.Warmup.IndexColumn != "user_id" {
		t.Fatalf("unexpected cache projection: %+v", view.Cache)
	}
	if view.Spec.Source == nil || view.Spec.Source.SQL != "SELECT id, name FROM users" {
		t.Fatalf("unexpected source projection: %+v", view.Spec.Source)
	}
	if view.Relations == nil {
		t.Fatalf("expected relation substrate slice")
	}
}

func TestBuildDataViewsCarriesAndResolvesCanonicalRelations(t *testing.T) {
	type item struct {
		OrderID int `sqlx:"order_id"`
	}
	type order struct {
		ID    int     `sqlx:"id"`
		Items []*item `sqlx:"-"`
	}
	type output struct {
		Data []order
	}
	component := &spec.Component{RootView: &spec.View{
		Name: "Orders", Source: &spec.ViewSource{Table: "orders"},
		Relations: []*spec.Relation{{
			Name: "items", Kind: spec.RelationKindSubview, Holder: "Items", Cardinality: spec.CardinalityMany,
			View: &spec.View{Name: "Items", Source: &spec.ViewSource{Table: "items"}},
			On:   []*spec.RelationLink{{ParentNamespace: "o", ParentColumn: "id", ChildNamespace: "i", ChildColumn: "order_id"}},
		}},
	}}
	views, err := buildDataViews(component, reflect.TypeOf(output{}), "Data")
	if err != nil {
		t.Fatalf("buildDataViews() error = %v", err)
	}
	if len(views.root.Relations) != 1 {
		t.Fatalf("relations = %#v", views.root.Relations)
	}
	relation := views.root.Relations[0]
	if relation.Holder != "Items" || relation.On[0].Field != "ID" || relation.Of.On[0].Field != "OrderID" {
		t.Fatalf("relation = %#v", relation)
	}
	if views.rowTypes[relation.Of.View] != reflect.TypeOf(item{}) {
		t.Fatalf("child row type = %v", views.rowTypes[relation.Of.View])
	}
}

func TestBuildDataViewsNormalizesLegacyRelationCardinality(t *testing.T) {
	type item struct {
		OrderID int `sqlx:"order_id"`
	}
	type order struct {
		ID    int     `sqlx:"id"`
		Items []*item `sqlx:"-"`
	}
	type output struct{ Data []order }
	component := &spec.Component{RootView: &spec.View{
		Name: "Orders", Source: &spec.ViewSource{Table: "orders"},
		Relations: []*spec.Relation{{
			Name: "items", Holder: "Items", Cardinality: spec.Cardinality("Many"),
			View: &spec.View{Name: "Items", Source: &spec.ViewSource{Table: "items"}},
			On:   []*spec.RelationLink{{ParentColumn: "id", ChildColumn: "order_id"}},
		}},
	}}
	views, err := buildDataViews(component, reflect.TypeOf(output{}), "Data")
	if err != nil {
		t.Fatalf("buildDataViews() error = %v", err)
	}
	if actual := views.root.Relations[0].Cardinality; actual != spec.CardinalityMany {
		t.Fatalf("cardinality = %q, want %q", actual, spec.CardinalityMany)
	}
}

func TestRelationCarriesResolvedReaderMetadata(t *testing.T) {
	relation := &data.Relation{
		Name: "user_accounts-rel", Kind: spec.RelationKindSubview, Holder: "Accounts", Cardinality: spec.CardinalityMany,
		On: data.Links{data.NewLink("u", "id", "ID")},
		Of: &data.RelationRef{View: &data.View{Spec: spec.View{Name: "accounts"}}, On: data.Links{data.NewLink("a", "user_id", "UserID")}},
	}
	if relation.Name != "user_accounts-rel" || relation.Holder != "Accounts" || relation.Cardinality != spec.CardinalityMany ||
		len(relation.On) != 1 || relation.On[0].Column != "id" || relation.Of == nil || relation.Of.View.Spec.Name != "accounts" ||
		len(relation.Of.On) != 1 || relation.Of.On[0].Column != "user_id" {
		t.Fatalf("unexpected relation metadata: %+v", relation)
	}
}

func TestEnrichViewFromOutputType_PlansTaggedRelations(t *testing.T) {
	type AccountView struct {
		ID     int
		UserID int
	}
	type UserView struct {
		ID       int
		Accounts []*AccountView `view:"accounts,uri=queries/accounts.sql,connector=analytics,batch=25,batchConcurrency=2,match=read_all,publishParent=true" sql:"SELECT id, user_id FROM accounts" on:"ID:u.id=UserID:a.user_id"`
	}

	view := &data.View{Spec: spec.View{Key: spec.Key{Kind: spec.KindView, Scope: "example.com/demo", Name: "users"},
		Name: "users"}, Relations: []*data.Relation{},
		CaseFormat: "lowerCamel",
	}
	if err := enrichViewForTest(view, reflect.TypeOf(UserView{})); err != nil {
		t.Fatalf("EnrichViewFromOutputType failed: %v", err)
	}
	if len(view.Relations) != 1 {
		t.Fatalf("expected 1 relation, got %d", len(view.Relations))
	}
	relation := view.Relations[0]
	if relation.Holder != "Accounts" || relation.Cardinality != spec.CardinalityMany {
		t.Fatalf("unexpected relation metadata: %+v", relation)
	}
	if len(relation.On) != 1 || len(relation.Of.On) != 1 {
		t.Fatalf("expected resolved relation links: %+v", relation)
	}
	if relation.On[0].Field != "ID" || relation.On[0].Namespace != "u" ||
		relation.Of.On[0].Field != "UserID" || relation.Of.On[0].Namespace != "a" {
		t.Fatalf("unexpected relation field links: parent=%+v child=%+v", relation.On[0], relation.Of.On[0])
	}
	if relation.Of == nil || relation.Of.View == nil || relation.Of.View.Spec.Name != "accounts" {
		t.Fatalf("expected child view planning: %+v", relation.Of)
	}
	if relation.Of.MatchStrategy != data.MatchReadAll {
		t.Fatalf("expected read-all match strategy, got %v", relation.Of.MatchStrategy)
	}
	if relation.Of.View.Connector != "analytics" || relation.Of.View.Spec.BatchSize != 25 || relation.Of.View.Spec.BatchConcurrency != 2 || !relation.Of.View.Spec.PublishParent {
		t.Fatalf("unexpected child view runtime metadata: %+v", relation.Of.View)
	}
	if relation.Of.View.Spec.Source == nil || relation.Of.View.Spec.Source.SQL != "SELECT id, user_id FROM accounts" || relation.Of.View.Spec.Source.URI != "queries/accounts.sql" {
		t.Fatalf("expected child sql source, got %+v", relation.Of.View.Spec.Source)
	}
}

func TestEnrichViewFromOutputType_IgnoresUnexportedSelfTag(t *testing.T) {
	type row struct {
		ID     int
		hidden int `self:"child=ID"`
	}
	view := &data.View{Relations: []*data.Relation{}}
	if err := enrichViewForTest(view, reflect.TypeOf(row{})); err != nil {
		t.Fatalf("EnrichViewFromOutputType() error = %v", err)
	}
	if view.Spec.SelfReference != nil {
		t.Fatalf("unexpected self reference: %+v", view.Spec.SelfReference)
	}
}

func TestEnrichViewFromOutputType_PlansPromotedRelationField(t *testing.T) {
	type account struct {
		ID     int
		UserID int
	}
	type Relations struct {
		Accounts []*account `view:"accounts" sql:"SELECT id, user_id FROM accounts" on:"ID:id=UserID:user_id"`
	}
	type user struct {
		ID int
		Relations
	}
	view := &data.View{Relations: []*data.Relation{}}
	if err := enrichViewForTest(view, reflect.TypeOf(user{})); err != nil {
		t.Fatalf("EnrichViewFromOutputType() error = %v", err)
	}
	if len(view.Relations) != 1 || view.Relations[0].Holder != "Accounts" {
		t.Fatalf("unexpected promoted relations: %+v", view.Relations)
	}
}

func TestEnrichViewFromOutputType_PlansOneCardinalitySummaryAsRelation(t *testing.T) {
	type AccountsSummaryView struct {
		UserID        int
		TotalAccounts int
	}
	type UserView struct {
		ID              int
		AccountsSummary *AccountsSummaryView `view:"accountsSummary,match=read_all" sql:"SELECT user_id, COUNT(*) AS total_accounts FROM accounts GROUP BY user_id" on:"ID:id=UserID:user_id"`
	}

	view := &data.View{Spec: spec.View{Key: spec.Key{Kind: spec.KindView, Scope: "example.com/demo", Name: "users"},
		Name: "users"}, Relations: []*data.Relation{},
		CaseFormat: "lowerCamel",
	}
	if err := enrichViewForTest(view, reflect.TypeOf(UserView{})); err != nil {
		t.Fatalf("EnrichViewFromOutputType failed: %v", err)
	}
	if len(view.Relations) != 1 {
		t.Fatalf("expected 1 relation, got %d", len(view.Relations))
	}
	relation := view.Relations[0]
	if relation.Holder != "AccountsSummary" || relation.Cardinality != spec.CardinalityOne ||
		len(relation.On) != 1 || relation.On[0].Column != "id" || len(relation.Of.On) != 1 || relation.Of.On[0].Column != "user_id" {
		t.Fatalf("unexpected summary relation metadata: %+v", relation)
	}
	if relation.Of == nil || relation.Of.View == nil || relation.Of.View.Spec.Name != "accountsSummary" {
		t.Fatalf("expected summary child view planning: %+v", relation.Of)
	}
	if relation.Of.MatchStrategy != data.MatchReadAll {
		t.Fatalf("expected read-all strategy for one-to-one relation, got %v", relation.Of.MatchStrategy)
	}
	if relation.Of.View.Spec.Source == nil || relation.Of.View.Spec.Source.SQL != "SELECT user_id, COUNT(*) AS total_accounts FROM accounts GROUP BY user_id" {
		t.Fatalf("expected summary relation sql source, got %+v", relation.Of.View.Spec.Source)
	}
}

func TestEnrichViewFromOutputType_PlansNestedOneToOneSubrelation(t *testing.T) {
	type AccountSummaryView struct {
		AccountID int
		TodayCnt  int
	}
	type AccountView struct {
		ID             int
		UserID         int
		AccountSummary *AccountSummaryView `view:"accountSummary,match=read_all" sql:"SELECT account_id, COUNT(*) AS today_cnt FROM account_events GROUP BY account_id" on:"ID:id=AccountID:account_id"`
	}
	type UserView struct {
		ID       int
		Accounts []*AccountView `view:"accounts,match=read_all" sql:"SELECT id, user_id FROM accounts" on:"ID:id=UserID:user_id"`
	}

	view := &data.View{Spec: spec.View{Key: spec.Key{Kind: spec.KindView, Scope: "example.com/demo", Name: "users"},
		Name: "users"}, Relations: []*data.Relation{},
		CaseFormat: "lowerCamel",
	}
	if err := enrichViewForTest(view, reflect.TypeOf(UserView{})); err != nil {
		t.Fatalf("EnrichViewFromOutputType failed: %v", err)
	}
	if len(view.Relations) != 1 {
		t.Fatalf("expected 1 top-level relation, got %d", len(view.Relations))
	}
	accountsRel := view.Relations[0]
	if accountsRel.Of == nil || accountsRel.Of.View == nil || len(accountsRel.Of.View.Relations) != 1 {
		t.Fatalf("expected nested child relation planning, got %+v", accountsRel.Of)
	}
	summaryRel := accountsRel.Of.View.Relations[0]
	if summaryRel.Holder != "AccountSummary" || summaryRel.Cardinality != spec.CardinalityOne {
		t.Fatalf("unexpected nested one-to-one relation: %+v", summaryRel)
	}
	if summaryRel.Of.MatchStrategy != data.MatchReadAll {
		t.Fatalf("expected read-all strategy for nested one-to-one relation, got %v", summaryRel.Of.MatchStrategy)
	}
	if summaryRel.Of == nil || summaryRel.Of.View == nil || summaryRel.Of.View.Spec.Name != "accountSummary" {
		t.Fatalf("expected nested summary child view planning: %+v", summaryRel.Of)
	}
}

func TestEnrichViewFromOutputType_PlansCompositeMatchedRelation(t *testing.T) {
	type SignalPerformanceView struct {
		FeatureType string
		Value       string
		Metric      int
	}
	type AudienceView struct {
		FeatureType       string
		FeatureValue      string
		SignalPerformance *SignalPerformanceView `view:"signalPerformance" sql:"SELECT feature_type, value, metric FROM signal_performance" on:"FeatureType:feature_type=FeatureType:feature_type,FeatureValue:feature_value=Value:value"`
	}

	view := &data.View{Spec: spec.View{Key: spec.Key{Kind: spec.KindView, Scope: "example.com/demo", Name: "audience"},
		Name: "audience"}, Relations: []*data.Relation{},
		CaseFormat: "lowerCamel",
	}
	if err := enrichViewForTest(view, reflect.TypeOf(AudienceView{})); err != nil {
		t.Fatalf("EnrichViewFromOutputType failed: %v", err)
	}
	if len(view.Relations) != 1 {
		t.Fatalf("expected 1 relation, got %d", len(view.Relations))
	}
	relation := view.Relations[0]
	if !relation.IsComposite() || relation.Cardinality != spec.CardinalityOne {
		t.Fatalf("expected composite one-to-one relation, got %+v", relation)
	}
	if len(relation.On) != 2 || len(relation.Of.On) != 2 {
		t.Fatalf("expected two composite links, got parent=%d child=%d", len(relation.On), len(relation.Of.On))
	}
}

func TestEnrichViewFromOutputType_PlansEmbeddedFieldRelations(t *testing.T) {
	type AccountView struct {
		ID     int
		UserID int
	}
	type EmbeddedKeys struct {
		UserID int
	}
	type UserView struct {
		EmbeddedKeys
		Accounts []*AccountView `view:"accounts,match=read_all" sql:"SELECT id, user_id FROM accounts" on:"UserID:user_id=UserID:user_id"`
	}

	view := &data.View{Spec: spec.View{Key: spec.Key{Kind: spec.KindView, Scope: "example.com/demo", Name: "users"},
		Name: "users"}, Relations: []*data.Relation{},
		CaseFormat: "lowerCamel",
	}
	graph, err := compileViewForTest(view, reflect.TypeOf(UserView{}))
	if err != nil {
		t.Fatalf("EnrichViewFromOutputType failed: %v", err)
	}
	if len(view.Relations) != 1 {
		t.Fatalf("expected 1 relation, got %d", len(view.Relations))
	}
	relation := view.Relations[0]
	compiled := graph.View(view).Relations[0]
	if compiled.On[0].XField == nil {
		t.Fatalf("expected compiled parent field for embedded relation")
	}
	if relation.On[0].Field != "UserID" || relation.Of == nil || len(relation.Of.On) != 1 || relation.Of.On[0].Column != "user_id" {
		t.Fatalf("unexpected embedded relation metadata: %+v", relation)
	}
}

func TestEnrichViewFromOutputType_PlansPointerEmbeddedFieldRelations(t *testing.T) {
	type AccountView struct {
		ID     int
		UserID int
	}
	type EmbeddedKeys struct {
		UserID int
	}
	type UserView struct {
		*EmbeddedKeys
		Accounts []*AccountView `view:"accounts,match=read_all" sql:"SELECT id, user_id FROM accounts" on:"UserID:user_id=UserID:user_id"`
	}

	view := &data.View{Spec: spec.View{Key: spec.Key{Kind: spec.KindView, Scope: "example.com/demo", Name: "users"},
		Name: "users"}, Relations: []*data.Relation{},
		CaseFormat: "lowerCamel",
	}
	graph, err := compileViewForTest(view, reflect.TypeOf(UserView{}))
	if err != nil {
		t.Fatalf("EnrichViewFromOutputType failed: %v", err)
	}
	if len(view.Relations) != 1 {
		t.Fatalf("expected 1 relation, got %d", len(view.Relations))
	}
	relation := graph.View(view).Relations[0]
	if relation.On[0].XField == nil || relation.On[0].Field != "UserID" {
		t.Fatalf("expected compiled parent field for pointer-embedded relation: %+v", relation.On[0])
	}
}

func TestEnrichViewFromOutputType_PlansNestedEmbeddedFieldRelations(t *testing.T) {
	type AccountView struct {
		ID     int
		UserID int
	}
	type Identity struct {
		UserID int
	}
	type Embedded struct {
		Identity
	}
	type UserView struct {
		Embedded
		Accounts []*AccountView `view:"accounts,match=read_all" sql:"SELECT id, user_id FROM accounts" on:"UserID:user_id=UserID:user_id"`
	}

	view := &data.View{Spec: spec.View{Key: spec.Key{Kind: spec.KindView, Scope: "example.com/demo", Name: "users"},
		Name: "users"}, Relations: []*data.Relation{},
		CaseFormat: "lowerCamel",
	}
	graph, err := compileViewForTest(view, reflect.TypeOf(UserView{}))
	if err != nil {
		t.Fatalf("EnrichViewFromOutputType failed: %v", err)
	}
	if len(view.Relations) != 1 {
		t.Fatalf("expected 1 relation, got %d", len(view.Relations))
	}
	relation := graph.View(view).Relations[0]
	if relation.On[0].XField == nil || relation.On[0].Field != "UserID" {
		t.Fatalf("expected compiled parent field for nested embedded relation: %+v", relation.On[0])
	}
}
