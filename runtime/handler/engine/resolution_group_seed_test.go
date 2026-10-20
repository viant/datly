package engine

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
)

func TestReadOnlyDependencySeedAdmission(t *testing.T) {
	type nested struct{ Child *int }
	type Embedded struct{ Child *int }
	type input struct {
		URL    string
		Nested *nested
		Embedded
		Auth  string
		Ready bool
		Has   *struct{ URL, Nested, Child, Auth bool } `setMarker:"true"`
	}
	for _, tc := range []struct {
		name, member, path, marker string
		legacy, authorization      bool
		want                       string
	}{
		{name: "allowed", member: "Nested.Child", path: "URL"},
		{name: "member", member: "Nested.Child", path: "Nested.Child", want: "overlaps resolution group"},
		{name: "ancestor", member: "Nested.Child", path: "Nested", want: "overlaps resolution group"},
		{name: "descendant", member: "Nested", path: "Nested.Child", want: "overlaps resolution group"},
		{name: "promoted alias", member: "Child", path: "Embedded.Child", want: "overlaps resolution group"},
		{name: "marker ancestor", member: "Child", path: "Has", want: "overlaps resolution group"},
		{name: "marker write", member: "Ready", path: "URL", marker: "Ready", want: "overlaps resolution group"},
		{name: "authorization", member: "Child", path: "Auth", authorization: true, want: "authorization binding"},
		{name: "authorization marker", member: "Child", path: "Has.Auth", authorization: true, want: "authorization binding"},
		{name: "legacy", member: "Child", path: "URL", legacy: true, want: "dependency-mode"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			group := &bindly.ResolutionGroupSpec{Name: "reads", Mode: bindly.ResolutionGroupDependencies}
			if tc.legacy {
				group.Mode = ""
				group.After = []string{"URL"}
			}
			specs := []bindly.BindingSpec{{Path: tc.member, Location: bindstate.Location{Kind: "component", In: "GET:/child"}, ResolutionGroup: group}}
			if tc.member != "URL" {
				specs = append(specs, bindly.BindingSpec{Path: "URL", Location: bindstate.Location{Kind: "const", In: "url"}, MarkerField: tc.marker})
			}
			if tc.authorization {
				specs = append(specs, bindly.BindingSpec{Path: "Auth", Location: bindstate.Location{Kind: "header", In: "Authorization"}, ErrorCode: 401})
			}
			declared := false
			for _, s := range specs {
				declared = declared || s.Path == tc.path
			}
			if !declared {
				specs = append(specs, bindly.BindingSpec{Path: tc.path, Location: bindstate.Location{Kind: "state", In: "seed"}})
			}
			contract := testRouteInput(t, reflect.TypeFor[input](), specs...)
			err := ValidateGroupedSeedPaths(contract, []string{tc.path})
			if tc.want == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.want)
			}
		})
	}
}

func TestReadOnlyDependencySeedRequestRestrictions(t *testing.T) {
	type input struct {
		URL   string
		Child *int
	}
	contract := testRouteInput(t, reflect.TypeFor[input](),
		bindly.BindingSpec{Path: "URL", Location: bindstate.Location{Kind: "const", In: "url"}},
		bindly.BindingSpec{Path: "Child", Location: bindstate.Location{Kind: "component", In: "GET:/child"}, ResolutionGroup: &bindly.ResolutionGroupSpec{Name: "reads", Mode: bindly.ResolutionGroupDependencies}},
	)
	for _, tc := range []struct {
		name string
		edit func(*Request)
		want string
	}{
		{name: "allowed"},
		{name: "bound", edit: func(r *Request) { r.BoundInput = &input{} }, want: "fresh canonical"},
		{name: "replay", edit: func(r *Request) { r.Replay = &bindly.ReplayBinding{} }, want: "fresh canonical"},
		{name: "independent", edit: func(r *Request) { r.IndependentChildTransactions = true }, want: "fresh canonical"},
		{name: "custom", edit: func(r *Request) { r.CanonicalReader = false }, want: "canonical native reader"},
		{name: "group output", edit: func(r *Request) { r.ResolvedPaths = []string{"Child"} }, want: "overlaps resolution group"},
		{name: "unknown", edit: func(r *Request) { r.ResolvedPaths = []string{"Unknown"} }, want: "invalid or duplicate"},
		{name: "duplicate", edit: func(r *Request) { r.ResolvedPaths = []string{"URL", "URL"} }, want: "invalid or duplicate"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := Request{CanonicalReader: true, Input: contract, ResolvedInput: &input{URL: "snapshot"}, ResolvedPaths: []string{"URL"}}
			if tc.edit != nil {
				tc.edit(&request)
			}
			err := validateGroupedInputRequest(request)
			if tc.want == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.want)
			}
		})
	}
}
