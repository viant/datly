package contract

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/viant/bindly"
	xcontract "github.com/viant/xdatly/contract"
)

type campaignPolicy struct {
	Campaign *patchPolicy
	Spend    *float64
	AdOrders []*orderPolicy
}
type orderPolicy struct {
	AdOrder *patchPolicy
	Fees    map[int][]*patchPolicy
	Stable  *policyLeaf
}
type patchPolicy struct {
	Secret string
	Public string
}

func compositeRole(ctx context.Context, b bindly.Binder) (string, error) {
	var state struct {
		Role string `bind:"kind=header,in=X-Role,required"`
	}
	err := b.Bind(ctx, &state)
	return state.Role, err
}
func (*patchPolicy) Contract(ctx context.Context, b bindly.Binder) (*xcontract.Policy, error) {
	role, err := compositeRole(ctx, b)
	if err != nil {
		return nil, err
	}
	if role == "admin" {
		return nil, nil
	}
	return &xcontract.Policy{Name: "patch/" + role, Field: func(_ string, _ reflect.Type, f *xcontract.Field) (*xcontract.Field, error) {
		if f.Name == "Secret" {
			return f.Hide().SetDescription("nested"), nil
		}
		return nil, nil
	}}, nil
}
func (*campaignPolicy) Contract(ctx context.Context, b bindly.Binder) (*xcontract.Policy, error) {
	role, err := compositeRole(ctx, b)
	if err != nil {
		return nil, err
	}
	if role == "admin" {
		return nil, nil
	}
	return &xcontract.Policy{Name: "campaign/" + role, Field: func(path string, parent reflect.Type, f *xcontract.Field) (*xcontract.Field, error) {
		if f.Name == "Secret" && parent == reflect.TypeFor[patchPolicy]() {
			if f.Tag.Get("description") != "nested" {
				return nil, fmtCompositeOrder(path)
			}
			return f.SetDescription("parent wins"), nil
		}
		return nil, nil
	}}, nil
}

type compositeOrderError string

func (e compositeOrderError) Error() string { return string(e) }
func fmtCompositeOrder(path string) error {
	return compositeOrderError("nested callback did not precede parent at " + path)
}

func TestCompositeSliceDelegatesNestedHooksAndParentHasFinalAuthority(t *testing.T) {
	original := reflect.TypeFor[[]*campaignPolicy]()
	p, err := New(original, 8)
	if err != nil {
		t.Fatal(err)
	}
	if !p.Dynamic() {
		t.Fatal("slice wrapper concealed nested hooks")
	}
	reader, err := p.Resolve(t.Context(), policyBinder(t, "reader"))
	if err != nil {
		t.Fatal(err)
	}
	campaign := reader.Elem().Elem()
	patch := campaign.Field(0).Type.Elem()
	if patch.Field(0).Tag.Get("json") != "-" || patch.Field(0).Tag.Get("description") != "parent wins" {
		t.Fatal("parent/nested composition")
	}
	order := campaign.Field(2).Type.Elem().Elem()
	fee := order.Field(1).Type.Elem().Elem().Elem()
	if fee.Field(0).Tag.Get("description") != "parent wins" {
		t.Fatal("map/slice nested policy not applied")
	}
	if order.Field(2).Type != reflect.TypeFor[*policyLeaf]() {
		t.Fatal("unchanged child lost linked type")
	}
	if admin, err := p.Resolve(t.Context(), policyBinder(t, "admin")); err != nil || admin != original {
		t.Fatal("admin no-op lost linked composite", err)
	}
	if again, err := p.Resolve(t.Context(), policyBinder(t, "reader")); err != nil || again != reader {
		t.Fatal("composite cache identity", err)
	}
}

func TestCompositeWithoutRootHookStillFindsNestedOwners(t *testing.T) {
	type wrapper struct {
		Orders []*orderPolicy
		Other  *policyLeaf
	}
	p, err := New(reflect.TypeFor[wrapper](), 8)
	if err != nil {
		t.Fatal(err)
	}
	if !p.Dynamic() {
		t.Fatal("missing nested hook")
	}
	actual, err := p.Resolve(t.Context(), policyBinder(t, "reader"))
	if err != nil {
		t.Fatal(err)
	}
	field := actual.Field(0).Type.Elem().Elem().Field(0).Type.Elem().Field(0)
	if field.Tag.Get("description") != "nested" {
		t.Fatal(field.Tag)
	}
	if _, err = p.Resolve(t.Context(), policyBinder(t, "")); err == nil || !strings.Contains(err.Error(), "contract policy") {
		t.Fatal("cached nested policy bypassed binding", err)
	}
}
