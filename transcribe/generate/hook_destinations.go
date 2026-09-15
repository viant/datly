package generate

import (
	"fmt"
	"go/parser"
	"go/token"
	"sort"
)

// relocateContractHooks keeps create-once lifecycle methods beside their
// receiver. Interface assertions may remain in the component through its type
// aliases; they do not define methods on those aliases.
func (d *shapeDestinations) relocateContractHooks(p *Plan, groups map[string]*Plan) error {
	hook := p.HookScaffold
	if hook == nil || p.MutationHandler != nil {
		return nil
	}
	receivers := map[string][]string{}
	for _, contract := range []ContractPlan{p.Input, p.Output} {
		if contract.Ownership == ContractGenerated && !p.localShape(contract.Package) {
			receivers[contract.Package] = append(receivers[contract.Package], contract.Type)
		}
	}
	packages := make([]string, 0, len(receivers))
	for pkg := range receivers {
		packages = append(packages, pkg)
	}
	sort.Strings(packages)
	for _, pkg := range packages {
		target := groups[pkg]
		if target == nil {
			return fmt.Errorf("contract hooks have no destination package %s", pkg)
		}
		source, err := hookScaffoldFileText(p.PackageName(), hook)
		if err != nil {
			return err
		}
		projected, _, err := d.projectReceiverMethods(target, []byte(source), receivers[pkg])
		if err != nil {
			return err
		}
		selected, err := parser.ParseFile(token.NewFileSet(), "hooks.go", projected.Selected, parser.ParseComments)
		if err != nil {
			return err
		}
		remaining, err := parser.ParseFile(token.NewFileSet(), "hooks.go", projected.Remaining, parser.ParseComments)
		if err != nil {
			return err
		}
		target.HookScaffold = &HookScaffoldPlan{Destination: hook.Destination, File: selected, PackagePath: pkg, Catalog: hook.Catalog}
		hook.File = remaining
	}
	return nil
}
