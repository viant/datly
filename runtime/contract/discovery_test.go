package contract

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	"github.com/viant/datly/exec"
	"github.com/viant/x/shape"
	xcontract "github.com/viant/xdatly/contract"
)

type discoveryRows struct {
	First  []discoveryRow `json:"first"`
	Second []discoveryRow `json:"second"`
}
type discoveryRow struct {
	ID     int
	Secret string
}
type discoveryEnvelope struct {
	Data *discoveryRows
	Flag bool
}

var discoveryProviders atomic.Int64
var discoveryParentProviders atomic.Int64
var discoveryFieldCalls atomic.Int64
var discoveryCause = errors.New("fixture dependency unavailable")

func (*discoveryEnvelope) Contract(ctx context.Context, binder bindly.Binder) (*xcontract.Policy, error) {
	discoveryParentProviders.Add(1)
	var state struct {
		Role string `bind:"kind=header,in=X-Role,required"`
	}
	if err := binder.Bind(ctx, &state); err != nil {
		return nil, err
	}
	selected := &xcontract.Policy{Name: "parent/" + state.Role, Field: func(_ string, parent reflect.Type, field *xcontract.Field) (*xcontract.Field, error) {
		if parent == reflect.TypeFor[discoveryRow]() && field.Name == "Secret" && state.Role != "admin" {
			return field.Hide(), nil
		}
		return nil, nil
	}}
	if state.Role == "cartesian" || state.Role == "cartesian-distinct" {
		hidden := *selected
		hidden.Name += "/flag-hidden"
		base := selected.Field
		hidden.Field = func(path string, parent reflect.Type, field *xcontract.Field) (*xcontract.Field, error) {
			if parent == reflect.TypeFor[discoveryEnvelope]() && field.Name == "Flag" {
				return field.Hide(), nil
			}
			return base(path, parent, field)
		}
		selected.DiscoveryAlternatives = []*xcontract.Policy{{Name: selected.Name, Field: selected.Field}, &hidden}
	}
	return selected, nil
}

func (*discoveryRows) Contract(ctx context.Context, binder bindly.Binder) (*xcontract.Policy, error) {
	discoveryProviders.Add(1)
	var state struct {
		Role string `bind:"kind=header,in=X-Role,required"`
	}
	if err := binder.Bind(ctx, &state); err != nil {
		return nil, err
	}
	if state.Role == "unavailable" {
		return nil, fmt.Errorf("auth dependency: %w", discoveryCause)
	}
	branch := func(holder string) *xcontract.Policy {
		return &xcontract.Policy{Name: "rows/" + holder, Field: func(_ string, parent reflect.Type, field *xcontract.Field) (*xcontract.Field, error) {
			discoveryFieldCalls.Add(1)
			if parent == reflect.TypeFor[discoveryRows]() {
				if field.Name != holder {
					return field.Hide(), nil
				}
				return field.SetTag("json", "data"), nil
			}
			return nil, nil
		}}
	}
	selected := branch("First")
	selected.NoCache = state.Role == "nocache" || state.Role == "nocache-distinct"
	selected.DiscoveryAlternatives = []*xcontract.Policy{branch("First"), branch("Second")}
	if state.Role == "distinct" || state.Role == "cartesian-distinct" || state.Role == "nocache-distinct" || state.Role == "distinct-duplicate" || state.Role == "distinct-uncached-branch" {
		selected.Name += "/selected"
	}
	if state.Role == "distinct-duplicate" {
		selected.DiscoveryAlternatives[1] = branch("First")
	}
	if state.Role == "inactive-branches" || state.Role == "inactive-uncached" {
		for _, alternative := range selected.DiscoveryAlternatives {
			alternative.Field = nil
		}
	}
	if state.Role == "distinct-uncached-branch" || state.Role == "inactive-uncached" {
		selected.DiscoveryAlternatives[0].NoCache = true
	}
	if state.Role == "unnamed-branch" {
		selected.DiscoveryAlternatives[0].Name = ""
	}
	if state.Role == "full-capacity" {
		for i := 2; i < 64; i++ {
			selected.DiscoveryAlternatives = append(selected.DiscoveryAlternatives, branch(fmt.Sprintf("Variant%d", i)))
		}
	}
	if state.Role == "invalid" {
		selected.DiscoveryAlternatives[1] = nil
	}
	if state.Role == "branch-failure" {
		selected.DiscoveryAlternatives[1].Field = func(string, reflect.Type, *xcontract.Field) (*xcontract.Field, error) { return nil, discoveryCause }
	}
	return selected, nil
}

func TestOutputDiscoveryAccountsForSelectedVariantAtCapacity(t *testing.T) {
	for _, test := range []struct {
		name         string
		role         string
		capacity     int
		branches     int
		preparations int
		uncached     int
		admitted     bool
	}{
		{name: "selected overlaps", role: "user", capacity: 2, branches: 2, admitted: true},
		{name: "full native capacity overlaps", role: "full-capacity", capacity: 64, branches: 64, admitted: true},
		{name: "selected distinct exceeds", role: "distinct", capacity: 2},
		{name: "selected distinct fits", role: "distinct", capacity: 3, branches: 2, admitted: true},
		{name: "cartesian selected overlaps", role: "cartesian", capacity: 4, branches: 4, admitted: true},
		{name: "cartesian selected distinct exceeds", role: "cartesian-distinct", capacity: 4},
		{name: "cartesian selected distinct fits", role: "cartesian-distinct", capacity: 5, branches: 4, admitted: true},
		{name: "duplicate branches", role: "distinct-duplicate", capacity: 2, branches: 2, preparations: 1, admitted: true},
		{name: "inactive branch names", role: "inactive-branches", capacity: 2, branches: 2, preparations: 1, admitted: true},
		{name: "inactive uncached branch", role: "inactive-uncached", capacity: 2, branches: 2, uncached: 1, admitted: true},
		{name: "unnamed branch", role: "unnamed-branch", capacity: 2, branches: 2, uncached: 1, admitted: true},
		{name: "uncached selected distinct", role: "nocache-distinct", capacity: 2, branches: 2, uncached: 2, admitted: true},
		{name: "uncached branch", role: "distinct-uncached-branch", capacity: 2, branches: 2, uncached: 1, admitted: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			preparations := 0
			var coldFieldCalls int64
			beforeProviders, beforeParents := discoveryProviders.Load(), discoveryParentProviders.Load()
			p, err := NewOutput(reflect.TypeFor[discoveryEnvelope](), test.capacity, func(*shape.Representation) (*exec.OutputDiscoverySchemas, error) {
				preparations++
				return &exec.OutputDiscoverySchemas{}, nil
			})
			require.NoError(t, err)
			for pass := 1; pass <= 3; pass++ {
				selected, branches, schemas, err := p.ResolveDiscoveryRepresentations(t.Context(), policyBinder(t, test.role))
				if !test.admitted {
					require.ErrorContains(t, err, fmt.Sprintf("capacity %d", test.capacity))
					require.Nil(t, selected)
					require.Nil(t, branches)
					require.Nil(t, schemas)
					require.Zero(t, preparations, "rejected catalogs never prepare schemas")
					continue
				}
				require.NoError(t, err)
				require.Len(t, branches, test.branches)
				require.Len(t, schemas, test.branches)
				want := test.branches
				if test.preparations != 0 {
					want = test.preparations
				}
				want += (pass - 1) * test.uncached
				require.Equal(t, want, preparations, "pass %d", pass)
				if pass == 1 {
					coldFieldCalls = discoveryFieldCalls.Load()
				} else if test.uncached == 0 {
					require.Equal(t, coldFieldCalls, discoveryFieldCalls.Load(), "warm discovery reuses rewritten metadata")
				}
				require.LessOrEqual(t, len(p.variants), test.capacity)
			}
			require.EqualValues(t, 3, discoveryProviders.Load()-beforeProviders)
			require.EqualValues(t, 3, discoveryParentProviders.Load()-beforeParents, "authorization owners still run for every operation")
		})
	}
}

func TestOutputDiscoveryWithoutAlternativesRetainsSelectedBehavior(t *testing.T) {
	preparations := 0
	p, err := NewOutput(reflect.TypeFor[policyInput](), 1, func(*shape.Representation) (*exec.OutputDiscoverySchemas, error) {
		preparations++
		return &exec.OutputDiscoverySchemas{}, nil
	})
	require.NoError(t, err)
	for range 2 {
		selected, branches, schemas, err := p.ResolveDiscoveryRepresentations(t.Context(), policyBinder(t, "hide"))
		require.NoError(t, err)
		require.NotNil(t, selected)
		require.Equal(t, "-", selected.Field(0).Tag.Get("json"))
		require.Nil(t, branches)
		require.Nil(t, schemas)
	}
	require.Zero(t, preparations)
	selected, branches, schemas, err := p.ResolveDiscoveryRepresentations(t.Context(), policyBinder(t, ""))
	require.Error(t, err, "retained selected metadata does not bypass caller binding")
	require.Nil(t, selected)
	require.Nil(t, branches)
	require.Nil(t, schemas)
}

func TestOutputDiscoveryReusesOwnerVariantsAndFreshParentPolicy(t *testing.T) {
	discoveryProviders.Store(0)
	discoveryParentProviders.Store(0)
	var preparations atomic.Int64
	p, err := NewOutput(reflect.TypeFor[discoveryEnvelope](), 8, func(metadata *shape.Representation) (*exec.OutputDiscoverySchemas, error) {
		preparations.Add(1)
		return &exec.OutputDiscoverySchemas{Schemas: map[string]map[string]any{"lc": {"signature": metadata.Signature()}}}, nil
	})
	require.NoError(t, err)
	for _, role := range []string{"user", "user", "admin", "user"} {
		_, branches, schemas, err := p.ResolveDiscoveryRepresentations(t.Context(), policyBinder(t, role))
		require.NoError(t, err)
		require.Len(t, branches, 2)
		require.Len(t, schemas, 2)
		for _, branch := range branches {
			row, err := branch.Lookup("Data.First")
			require.NoError(t, err)
			hidden := row.Elem().Field(1).Tag.Get("json") == "-"
			require.Equal(t, role != "admin", hidden, "parent authority survives every branch")
		}
	}
	require.EqualValues(t, 4, discoveryProviders.Load())
	require.EqualValues(t, 4, discoveryParentProviders.Load())
	require.EqualValues(t, 4, preparations.Load())
	for range 2 {
		_, _, _, err := p.ResolveDiscoveryRepresentations(t.Context(), policyBinder(t, "nocache"))
		require.NoError(t, err)
	}
	require.EqualValues(t, 8, preparations.Load(), "NoCache bypasses retained artifacts")
	before := preparations.Load()
	_, err = p.ResolveRepresentation(t.Context(), policyBinder(t, "invalid"))
	require.NoError(t, err)
	require.Equal(t, before, preparations.Load(), "calls never prepare discovery branches")
	selected, branches, schemas, err := p.ResolveDiscoveryRepresentations(t.Context(), policyBinder(t, "invalid"))
	require.ErrorContains(t, err, "non-nil leaf")
	require.Nil(t, selected)
	require.Nil(t, branches)
	require.Nil(t, schemas)
	for _, role := range []string{"unavailable", "branch-failure"} {
		selected, branches, schemas, err = p.ResolveDiscoveryRepresentations(t.Context(), policyBinder(t, role))
		require.ErrorIs(t, err, discoveryCause)
		require.Nil(t, selected)
		require.Nil(t, branches)
		require.Nil(t, schemas)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	selected, branches, schemas, err = p.ResolveDiscoveryRepresentations(ctx, policyBinder(t, "user"))
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, selected)
	require.Nil(t, branches)
	require.Nil(t, schemas)
}

func TestOutputDiscoveryConcurrentColdPreparationAndCapacity(t *testing.T) {
	var preparations atomic.Int64
	p, err := NewOutput(reflect.TypeFor[discoveryEnvelope](), 2, func(*shape.Representation) (*exec.OutputDiscoverySchemas, error) {
		preparations.Add(1)
		return &exec.OutputDiscoverySchemas{Schemas: map[string]map[string]any{"lc": {"type": "object"}}}, nil
	})
	require.NoError(t, err)
	binders := make([]*bindly.Injector, 8)
	for i := range binders {
		binders[i] = policyBinder(t, "user")
	}
	var wg sync.WaitGroup
	for _, binder := range binders {
		wg.Go(func() {
			_, _, _, err := p.ResolveDiscoveryRepresentations(t.Context(), binder)
			require.NoError(t, err)
		})
	}
	wg.Wait()
	require.EqualValues(t, 2, preparations.Load())
	for _, role := range []string{"admin", "user"} {
		_, _, _, err := p.ResolveDiscoveryRepresentations(t.Context(), policyBinder(t, role))
		require.NoError(t, err)
	}
	require.EqualValues(t, 6, preparations.Load(), "eviction prepares only the reloaded policy variants")
	for _, capacity := range []int{0, 1} {
		small, err := New(reflect.TypeFor[discoveryEnvelope](), capacity)
		require.NoError(t, err)
		_, err = small.ResolveRepresentation(t.Context(), policyBinder(t, "user"))
		require.NoError(t, err)
		_, _, _, err = small.ResolveDiscoveryRepresentations(t.Context(), policyBinder(t, "user"))
		require.ErrorContains(t, err, "capacity")
	}
}

func TestOutputDiscoveryComposesBoundedCompleteCombinations(t *testing.T) {
	p, err := New(reflect.TypeFor[discoveryEnvelope](), 8)
	require.NoError(t, err)
	_, branches, _, err := p.ResolveDiscoveryRepresentations(t.Context(), policyBinder(t, "cartesian"))
	require.NoError(t, err)
	require.Len(t, branches, 4)
	flags := map[string]int{}
	for _, branch := range branches {
		flags[branch.Field(1).Tag.Get("json")]++
		row, err := branch.Lookup("Data.First")
		require.NoError(t, err)
		require.Equal(t, "-", row.Elem().Field(1).Tag.Get("json"))
	}
	require.Equal(t, map[string]int{"": 2, "-": 2}, flags)
	small, err := New(reflect.TypeFor[discoveryEnvelope](), 3)
	require.NoError(t, err)
	_, _, _, err = small.ResolveDiscoveryRepresentations(t.Context(), policyBinder(t, "cartesian"))
	require.ErrorContains(t, err, "capacity 3")
}

func TestOutputDiscoveryPreparationFailureDoesNotPoisonVariant(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded, discoveryCause} {
		t.Run(cause.Error(), func(t *testing.T) {
			failed := true
			preparations := 0
			p, err := NewOutput(reflect.TypeFor[discoveryEnvelope](), 8, func(*shape.Representation) (*exec.OutputDiscoverySchemas, error) {
				preparations++
				if failed {
					return nil, fmt.Errorf("prepare fixture: %w", cause)
				}
				return &exec.OutputDiscoverySchemas{Schemas: map[string]map[string]any{"lc": {"type": "object"}}, Errors: map[string]error{"lu": discoveryCause}}, nil
			})
			require.NoError(t, err)
			_, _, _, err = p.ResolveDiscoveryRepresentations(t.Context(), policyBinder(t, "user"))
			require.ErrorIs(t, err, cause)
			failed = false
			for range 2 {
				_, _, artifacts, err := p.ResolveDiscoveryRepresentations(t.Context(), policyBinder(t, "user"))
				require.NoError(t, err)
				require.Len(t, artifacts, 2)
				for _, artifact := range artifacts {
					require.ErrorIs(t, artifact.Errors["lu"], discoveryCause)
					require.Equal(t, "object", artifact.Schemas["lc"]["type"])
				}
			}
			require.Equal(t, 3, preparations, "retry failed preparation, then prepare each successful branch once")
		})
	}
}
