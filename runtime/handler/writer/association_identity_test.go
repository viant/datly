package writer

import (
	"context"
	"errors"
	"github.com/viant/govalidator"
	"reflect"
	"testing"

	"github.com/viant/datly/spec"
)

func TestAssociationIdentityPhysicalRole(t *testing.T) {
	type link struct{ ID, Owner, Member int }
	type owner struct {
		ID               int
		Source, Prepared []*link
	}
	parentKey := Field{Name: "ID", Column: "ID", Index: []int{0}}
	links := []Link{{Parent: parentKey, Child: Field{Name: "Owner", Column: "OWNER", Index: []int{1}}}}
	childKey := Field{Name: "ID", Column: "ID", AutoIncrement: true, Index: []int{0}}
	for _, tc := range []struct {
		name, role, grant          string
		writableSource, wrongOwner bool
		want                       bool
	}{
		{"exact", "Prepared", "links", false, false, true},
		{"unresolved", "Other", "links", false, false, false},
		{"source cannot be target", "Source", "links", false, false, false},
		{"different table grant", "Prepared", "events", false, false, false},
		{"physical source", "Prepared", "links", true, false, false},
		{"borrowed owner key", "Prepared", "links", false, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			physical := &Record{Table: "links", EntityType: reflect.TypeFor[link](), WriterActionPolicy: insertDeleteActionPolicy, Sequence: &childKey, Keys: []Field{childKey}}
			source := &Record{Table: "links", Auxiliary: !tc.writableSource, EntityType: reflect.TypeFor[link]()}
			holder := &Relation{Child: source, Field: []int{1}, Links: links, association: &associationMapping{insertIdentityRole: tc.role}}
			target := &Relation{Child: physical, Field: []int{2}, Links: links}
			ownerKey := parentKey
			if tc.wrongOwner {
				ownerKey.Column = "UNRELATED"
			}
			root := &Record{EntityType: reflect.TypeFor[owner](), Keys: []Field{ownerKey}, Relations: []*Relation{holder, target}}
			component := &spec.Component{Settings: &spec.Settings{ProtectedFlushTables: []string{tc.grant}}}
			err := compileAssociationIdentities(root, component)
			if (err == nil) != tc.want {
				t.Fatalf("compile: %v", err)
			}
			if tc.want && holder.association.physicalIdentity != target {
				t.Fatal("unexpected target")
			}
		})
	}
}

type observedIdentityLink struct{ ID int }
type associationRowAudit struct{}

func (*associationRowAudit) AfterQueueRow(_ context.Context, row *observedIdentityLink) error {
	if row.ID == 0 {
		return errors.New("observer saw an unset ID")
	}
	row.ID = 99
	return nil
}

type associationGraphAudit struct{}

func (*associationGraphAudit) AfterQueue() {}

type associationGate struct{}

func (*associationGate) WriteEligible() {}

type associationEarlyAudit struct{}

func (*associationEarlyAudit) AfterSequence() {}

type associationPhaseAudit struct{}

func (*associationPhaseAudit) ObservePhase() {}

type associationAttemptAudit struct{}

func (*associationAttemptAudit) ObserveQueueAttempt() {}

type associationStatefulAudit struct{ Sibling *observedIdentityLink }

func (*associationStatefulAudit) AfterQueueRow(context.Context, *observedIdentityLink) error {
	return nil
}

type associationWrongSignature struct{}

func (*associationWrongSignature) AfterQueueRow(context.Context, *observedIdentityLink, int) error {
	return nil
}

func TestAssociationIdentityObserverEligibility(t *testing.T) {
	link := reflect.TypeFor[observedIdentityLink]()
	for _, tc := range []struct {
		name                string
		parent, leaf, other reflect.Type
		auxiliary           bool
		want                bool
	}{
		{"narrow leaf", nil, reflect.TypeFor[associationRowAudit](), nil, true, true},
		{"root AfterQueue", reflect.TypeFor[associationGraphAudit](), nil, nil, true, false},
		{"parent WriteEligible", reflect.TypeFor[associationGate](), nil, nil, true, false},
		{"sibling AfterSequence", nil, nil, reflect.TypeFor[associationEarlyAudit](), true, false},
		{"sibling phase observer", nil, nil, reflect.TypeFor[associationPhaseAudit](), true, false},
		{"root queue attempt observer", reflect.TypeFor[associationAttemptAudit](), nil, nil, true, false},
		{"child full graph", nil, reflect.TypeFor[associationGraphAudit](), nil, true, false},
		{"stateful row observer", nil, reflect.TypeFor[associationStatefulAudit](), nil, true, false},
		{"extra argument", nil, reflect.TypeFor[associationWrongSignature](), nil, true, false},
		{"unmapped physical sibling", nil, nil, nil, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			child := &Record{associationIdentityLeaf: true, EntityType: link, HookType: tc.leaf, Path: "Owner/Prepared"}
			root := &Record{Auxiliary: true, HookType: tc.parent, Relations: []*Relation{{Child: child}, {Child: &Record{Auxiliary: tc.auxiliary, EntityType: link, HookType: tc.other, Path: "Owner/Sibling"}}}}
			err := validateAssociationIdentityGraph(root)
			if (err == nil) != tc.want {
				t.Fatalf("graph validation: %v", err)
			}
			if !tc.want {
				return
			}
			if !child.rowQueueObserver {
				t.Fatal("observer was lost")
			}
			p := &Program{}
			for _, id := range []int{14, 17} {
				row := &observedIdentityLink{ID: id}
				frame := &Frame{Hook: reflect.ValueOf(&associationRowAudit{}), Entity: reflect.ValueOf(row), Record: child}
				if err := p.callEntityHook(t.Context(), "AfterQueue", frame); err != nil {
					t.Fatal(err)
				}
				if row.ID != id {
					t.Fatalf("observer changed writer row ID to %d", row.ID)
				}
			}
		})
	}
}

func TestAssociationIdentityKeyTypes(t *testing.T) {
	type named int64
	for _, v := range []any{int(0), int8(0), int16(0), int32(0), int64(0)} {
		if !supportedAssociationKey(reflect.TypeOf(v)) {
			t.Fatalf("rejected %T", v)
		}
	}
	for _, v := range []any{uint(0), uint8(0), uint16(0), uint32(0), uint64(0), named(0), new(int), ""} {
		if supportedAssociationKey(reflect.TypeOf(v)) {
			t.Fatalf("accepted %T", v)
		}
	}
}

// A predicate attached to Member can read ID from the validation session,
// including when the writer requests a shallow pass. Deferring the ID would
// change the rule's outcome, so this graph must retain ordinary allocation.
func TestAssociationIdentityDependentValidation(t *testing.T) {
	type Link struct {
		ID     int
		Member int `validate:"association_writer_needs_link_key"`
	}
	govalidator.RegisterWithDependencies("association_writer_needs_link_key", func(*govalidator.Field, *govalidator.Check) (govalidator.IsValid, error) {
		return func(ctx context.Context, _ any) (bool, error) {
			owner := reflect.Indirect(reflect.ValueOf(ctx.Value(govalidator.SessionKey).(*govalidator.Session).ParentValue))
			return owner.FieldByName("ID").Int() > 0, nil
		}, nil
	}, func(*govalidator.Field, *govalidator.Check) ([]string, error) { return []string{"ID"}, nil })
	rt := reflect.TypeFor[Link]()
	checks, err := govalidator.NewChecks(rt)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := checks.Prepare(govalidator.WithShallow(true))
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []int{0, 17} {
		result, err := plan.Validate(t.Context(), &Link{ID: id, Member: 22})
		if err != nil || (len(result.Violations) == 0) != (id != 0) {
			t.Fatalf("id=%d result %v, error %v", id, result, err)
		}
	}
	if err := validateAssociationLeaf(&Record{EntityType: rt, Sequence: &Field{Name: "ID"}}); err == nil {
		t.Fatal("key-dependent Go predicate admitted")
	}
}

func TestAssociationIdentitySQLValidation(t *testing.T) {
	type unique struct {
		ID     int
		Member int `sqlx:"MEMBER,uniquedep=ID,table=links"`
	}
	type compositeFK struct {
		ID     int
		Member int `sqlx:"member,refTable=owners,refColumn=member" refKey:"ID:id"`
	}
	type keyFK struct {
		ID int `sqlx:"id,refTable=records,refColumn=id"`
	}
	type keyEmpty struct {
		ID int `sqlx:"id,required,nullifyEmpty"`
	}
	type nested struct {
		ID    int
		Other struct {
			Value string `validate:"required"`
		}
	}
	type direct struct {
		ID    int `sqlx:"id,required,autoincrement"`
		Owner int `sqlx:"owner,required,refTable=owners,refColumn=id"`
	}
	for _, tc := range []struct {
		typ  reflect.Type
		want bool
	}{{reflect.TypeFor[unique](), false}, {reflect.TypeFor[compositeFK](), false}, {reflect.TypeFor[keyFK](), false}, {reflect.TypeFor[keyEmpty](), false}, {reflect.TypeFor[nested](), false}, {reflect.TypeFor[direct](), true}} {
		err := validateAssociationLeaf(&Record{EntityType: tc.typ, Sequence: &Field{Name: "ID"}})
		if (err == nil) != tc.want {
			t.Fatalf("%s: %v", tc.typ, err)
		}
	}
}
