package generate

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/viant/datly/internal/testharness"
	"github.com/viant/datly/spec"
	"github.com/viant/datly/transcribe/gobuild"
)

func sharedContractPersistence(t *testing.T, dir, owner string) *scaffoldPersistence {
	t.Helper()
	(testharness.GeneratedModule{Path: "example.com/contracts"}).Write(t, dir)
	plan := &Plan{Package: "example.com/contracts", GoPackage: "contracts", ComponentName: owner, RouterDest: strings.ToLower(owner) + "_router.go", FactoryExpression: "NewRequest", ExternalHandler: &ExternalHandler{Build: &gobuild.Context{Dir: dir}, GeneratedContracts: true, Package: "example.com/contracts", Name: "NewRequest"}, Routes: []RoutePlan{{Method: "POST", Path: "/" + strings.ToLower(owner)}}, Input: generatedContract("Input", "input.go"), Output: generatedContract("Output", "output.go")}
	plan.Input.Fields = []Field{{Name: "Data", Type: "[]string", Source: "body", Tag: `parameter:"Data,kind=body,in="`}}
	plan.Output.Fields = []Field{{Name: "Status", Type: "string", Source: "output", Tag: `parameter:"Status,kind=output,in=status"`}}
	files, user, removals, err := scaffoldArtifacts(dir, plan)
	require.NoError(t, err)
	return &scaffoldPersistence{dir: dir, owner: owner, files: files, userFiles: user, removals: removals, plan: plan}
}

func TestSharedNativeContractPristineProposalAndRetirement(t *testing.T) {
	dir := t.TempDir()
	first := sharedContractPersistence(t, dir, "First")
	forests, _, _, err := prepareScaffoldForests([]*scaffoldPersistence{first})
	require.NoError(t, err)
	require.NoError(t, publishScaffoldForests(forests))
	second := sharedContractPersistence(t, dir, "Second")
	pristine := append([]EmittedFile(nil), second.files...)
	preview, err := second.preview()
	require.NoError(t, err)
	require.Equal(t, pristine, second.files)
	require.Len(t, preview.retainedContracts, 3)
	for name := range preview.retainedContracts {
		require.False(t, preview.renames[name])
		require.Contains(t, preview.retainedContracts[name], "Datly for First;")
	}
	// A prior retained preview can never certify a new foreign edit against itself.
	file := filepath.Join(dir, "input.go")
	original, err := os.ReadFile(file)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(file, append(original, []byte("// changed after preview\n")...), 0600))
	before, err := readScaffoldSnapshot(dir)
	require.NoError(t, err)
	require.ErrorContains(t, preview.prepareCurrent(dir), "independently rendered proposal")
	_, _, _, err = prepareScaffoldForests([]*scaffoldPersistence{second})
	require.ErrorContains(t, err, "independently rendered proposal")
	after, err := readScaffoldSnapshot(dir)
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestSharedNativeContractFinalProjectedAndConcurrentEdit(t *testing.T) {
	for _, where := range []string{"target", "stage"} {
		t.Run(where, func(t *testing.T) {
			dir := t.TempDir()
			first := sharedContractPersistence(t, dir, "First")
			forests, _, _, err := prepareScaffoldForests([]*scaffoldPersistence{first})
			require.NoError(t, err)
			require.NoError(t, publishScaffoldForests(forests))
			second := sharedContractPersistence(t, dir, "Second")
			forests, _, _, err = prepareScaffoldForests([]*scaffoldPersistence{second})
			require.NoError(t, err)
			defer cleanupScaffoldForests(forests)
			root := dir
			if where == "stage" {
				root = forests[0].stage
			}
			file := filepath.Join(root, "input_setters.go")
			current, err := os.ReadFile(file)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(file, append(current, []byte("// latest edit\n")...), 0600))
			before, err := readScaffoldSnapshot(dir)
			require.NoError(t, err)
			require.Error(t, publishScaffoldForests(forests))
			after, err := readScaffoldSnapshot(dir)
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

func TestSharedNativeContractRoleScope(t *testing.T) {
	for _, scope := range []string{"shape", "reader", "PATCH", "linked", "mixed product", "no context"} {
		t.Run(scope, func(t *testing.T) {
			dir := t.TempDir()
			first := sharedContractPersistence(t, dir, "First")
			forests, _, _, err := prepareScaffoldForests([]*scaffoldPersistence{first})
			require.NoError(t, err)
			require.NoError(t, publishScaffoldForests(forests))
			second := sharedContractPersistence(t, dir, "Second")
			switch scope {
			case "shape":
				second.plan.ExternalHandler.InputShape = &spec.View{}
			case "reader":
				second.plan.Views = []ViewPlan{{Type: "Other"}}
			case "PATCH":
				second.plan.Routes[0].Method = "PATCH"
			case "linked":
				second.plan.Input.Ownership = ContractLinked
			case "no context":
				second.plan.ExternalHandler.Build = nil
			case "mixed product":
				second.plan.GeneratedTypes = []GeneratedTypePlan{{Destination: "input.go"}}
			}
			before, err := readScaffoldSnapshot(dir)
			require.NoError(t, err)
			_, _, _, err = prepareScaffoldForests([]*scaffoldPersistence{second})
			require.Error(t, err)
			after, err := readScaffoldSnapshot(dir)
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}
