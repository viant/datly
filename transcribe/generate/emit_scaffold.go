package generate

import (
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
)

type EmittedFile struct {
	Path    string
	Content string
}

func EmitScaffold(dir string, plan *Plan) ([]EmittedFile, error) {
	return emitScaffold(dir, plan, false)
}

// EmitScaffoldEphemeral emits a staging package without any ownership sidecar.
func EmitScaffoldEphemeral(dir string, plan *Plan) ([]EmittedFile, error) {
	return emitScaffold(dir, plan, true)
}

func emitScaffold(dir string, plan *Plan, ephemeral bool) ([]EmittedFile, error) {
	if plan != nil && plan.MutationHandler == nil && plan.lifecycleTargetError != nil {
		return nil, plan.lifecycleTargetError
	}
	packages, err := plan.packages(dir)
	if err != nil {
		return nil, err
	}
	if err = packages.validate(); err != nil {
		return nil, err
	}
	var result []EmittedFile
	for i, p := range packages.plans {
		files, userFiles, removals, err := scaffoldArtifacts(packages.dirs[i], p)
		if err != nil {
			return nil, err
		}
		persistence := &scaffoldPersistence{dir: packages.dirs[i], owner: p.ComponentName, files: files, userFiles: userFiles, removals: removals, plan: p, ephemeral: ephemeral}
		if err = persistence.Commit(); err != nil {
			return nil, err
		}
		result = append(result, persistence.files...)
	}
	return result, nil
}

// ValidateDestination checks every package in this component before writing.
func (p *Plan) ValidateDestination(dir string) error {
	if p != nil && p.MutationHandler == nil && p.lifecycleTargetError != nil {
		return p.lifecycleTargetError
	}
	packages, err := p.packages(dir)
	if err != nil {
		return err
	}
	return packages.validate()
}

// ValidateDestinationEphemeral validates a prospective generated layout and
// imports without applying persistence ownership checks. It is for read-only
// project validation; real transcription still calls ValidateDestination.
func (p *Plan) ValidateDestinationEphemeral(dir string) error {
	if p != nil && p.MutationHandler == nil && p.lifecycleTargetError != nil {
		return p.lifecycleTargetError
	}
	packages, err := p.packages(dir)
	if err != nil {
		return err
	}
	return packages.validateEphemeral()
}

func scaffoldArtifacts(dir string, plan *Plan) ([]EmittedFile, []EmittedFile, []string, error) {
	if plan == nil {
		return nil, nil, nil, fmt.Errorf("nil plan")
	}
	if err := plan.validateHookScaffold(); err != nil {
		return nil, nil, nil, err
	}
	if err := plan.validateGeneratedDestinations(); err != nil {
		return nil, nil, nil, err
	}
	packageName := plan.PackageName()
	var componentContent string
	var err error
	if plan.ShapesOnly {
	} else if plan.Static != nil {
		componentContent, err = plan.staticSource(packageName)
	} else {
		componentContent, err = componentFileText(packageName, plan)
	}
	if err != nil {
		return nil, nil, nil, err
	}
	viewDestinations := map[string]bool{}
	for _, view := range plan.Views {
		if view.Ownership == ViewGenerated && plan.localShape(view.Package) {
			viewDestinations[view.Destination] = true
		}
	}
	if plan.hasViewSupport() {
		viewDestinations[plan.ViewDest] = true
	}
	orderedViewDestinations := make([]string, 0, len(viewDestinations))
	for destination := range viewDestinations {
		orderedViewDestinations = append(orderedViewDestinations, destination)
	}
	sort.Strings(orderedViewDestinations)
	files := make([]EmittedFile, 0, len(orderedViewDestinations)+len(plan.GeneratedTypes)+5)
	if plan.EntitySupport != nil {
		content, err := plan.EntitySupport.source(packageName)
		if err != nil {
			return nil, nil, nil, err
		}
		files = append(files, EmittedFile{Path: filepath.Join(dir, plan.EntitySupport.Destination), Content: content})
		if invariants := activeEntityInvariants(plan); len(invariants) > 0 {
			files = append(files, EmittedFile{Path: filepath.Join(dir, plan.Generation.File("invariants", "invariants.go")), Content: invariantIndexSource(packageName, invariants)})
		}
	}
	if strings.TrimSpace(plan.Settings.Mutation) != "" {
		existing := map[string]bool{}
		for _, file := range files {
			relative, _ := filepath.Rel(dir, file.Path)
			existing[filepath.Clean(relative)] = true
		}
		for _, role := range []string{"mutation", "links", "frames", "previous", "layout", "actions", "mutation_output", "validation"} {
			destination := plan.Generation.File(role, role+".go")
			if existing[filepath.Clean(destination)] {
				continue
			}
			files = append(files, EmittedFile{Path: filepath.Join(dir, destination), Content: supportRoleSource(packageName, role)})
		}
	}
	if resources := plan.Resources; resources != nil {
		files = append(files, EmittedFile{Path: filepath.Join(dir, resources.Destination), Content: resources.source(packageName)})
		for _, file := range resources.Files {
			files = append(files, EmittedFile{Path: filepath.Join(dir, file.Path), Content: file.Content})
		}
	}
	for _, destination := range orderedViewDestinations {
		files = append(files, EmittedFile{
			Path: filepath.Join(dir, destination), Content: viewFileForDestination(packageName, plan, destination),
		})
	}
	if !plan.ShapesOnly {
		files = append(files, EmittedFile{Path: filepath.Join(dir, plan.RouterDest), Content: componentContent})
	}
	if len(plan.Aliases) > 0 {
		files = append(files, EmittedFile{Path: filepath.Join(dir, plan.Generation.File("types", "types.go")), Content: plan.aliasSource()})
	}
	if plan.ReadIndexes != nil {
		content, err := plan.ReadIndexes.Source.source(packageName)
		if err != nil {
			return nil, nil, nil, err
		}
		files = append(files, EmittedFile{Path: filepath.Join(dir, plan.ReadIndexes.Source.Destination), Content: content})
	}
	if plan.Input.Ownership == ContractGenerated && plan.localShape(plan.Input.Package) {
		files = append(files, EmittedFile{
			Path: filepath.Join(dir, plan.Input.Destination),
			Content: inputStructFile(packageName,
				fmt.Sprintf("// %s is the generated input scaffold for %s.", plan.Input.Type, plan.ComponentName),
				plan.Input.Type, plan.Input.Fields, plan.Imports,
			),
		})
		if setters := inputSetterFile(packageName, plan.Input.Type, plan.Input.Fields, plan.Imports); setters != "" {
			files = append(files, EmittedFile{Path: filepath.Join(dir, plan.Generation.File("input_setters", "input_setters.go")), Content: setters})
		}
	}
	if plan.Output.Ownership == ContractGenerated && plan.localShape(plan.Output.Package) {
		files = append(files, EmittedFile{
			Path: filepath.Join(dir, plan.Output.Destination),
			Content: structFileWithImports(packageName,
				fmt.Sprintf("// %s is the generated output scaffold for %s.", plan.Output.Type, plan.ComponentName),
				plan.Output.Type, plan.Output.Fields, plan.Imports,
			),
		})
	}
	for _, generated := range plan.GeneratedTypes {
		content, err := generatedTypeFileText(packageName, generated)
		if err != nil {
			return nil, nil, nil, err
		}
		files = append(files, EmittedFile{Path: filepath.Join(dir, generated.Destination), Content: content})
	}
	if plan.GoHandler != nil {
		content, err := goHandlerFileText(packageName, plan.GoHandler)
		if err != nil {
			return nil, nil, nil, err
		}
		files = append(files, EmittedFile{
			Path: filepath.Join(dir, plan.GoHandler.Destination), Content: content,
		})
	}
	if plan.ContractHandler != nil {
		content, err := contractHandlerFileText(packageName, plan.ContractHandler)
		if err != nil {
			return nil, nil, nil, err
		}
		files = append(files, EmittedFile{
			Path: filepath.Join(dir, plan.ContractHandler.Destination), Content: content,
		})
	}
	if plan.MutationHandler != nil {
		content, err := plan.MutationHandler.source(packageName)
		if err != nil {
			return nil, nil, nil, err
		}
		files = append(files, EmittedFile{Path: filepath.Join(dir, plan.MutationHandler.Destination), Content: content})
		for _, source := range plan.MutationHandler.Support {
			content, err := source.source(packageName)
			if err != nil {
				return nil, nil, nil, err
			}
			files = append(files, EmittedFile{Path: filepath.Join(dir, source.Destination), Content: content})
		}
	}
	if plan.FactoryLink != nil {
		content, err := plan.FactoryLink.source(packageName, plan)
		if err != nil {
			return nil, nil, nil, err
		}
		files = append(files, EmittedFile{Path: filepath.Join(dir, plan.FactoryLink.Destination), Content: content})
	}
	if plan.VeltyHandler != nil {
		content, err := veltyHandlerFileText(packageName, plan)
		if err != nil {
			return nil, nil, nil, err
		}
		files = append(files,
			EmittedFile{Path: filepath.Join(dir, plan.VeltyHandler.GoDestination), Content: content},
			EmittedFile{Path: filepath.Join(dir, plan.VeltyHandler.ResourceDestination), Content: plan.VeltyHandler.Template},
		)
	}
	var userFiles []EmittedFile
	if plan.HookScaffold != nil {
		content, err := hookScaffoldFileText(packageName, plan.HookScaffold)
		if err != nil {
			return nil, nil, nil, err
		}
		userFiles = append(userFiles, EmittedFile{
			Path: filepath.Join(dir, plan.HookScaffold.Destination), Content: content,
		})
	}
	var removals []string
	invariantDestination := plan.Generation.File("invariants", "invariants.go")
	hasInvariantArtifact := false
	for _, file := range files {
		relative, _ := filepath.Rel(dir, file.Path)
		if filepath.Clean(relative) == filepath.Clean(invariantDestination) {
			hasInvariantArtifact = true
			break
		}
	}
	if !hasInvariantArtifact {
		removals = append(removals, invariantDestination)
	}
	if plan.Input.Ownership == ContractLinked {
		removals = append(removals, plan.Input.Destination)
	}
	if plan.Output.Ownership == ContractLinked {
		removals = append(removals, plan.Output.Destination)
	}
	return files, userFiles, removals, nil
}

func activeEntityInvariants(plan *Plan) []EntityInvariant {
	support := plan.EntitySupport
	if support == nil || len(support.Invariants) == 0 {
		return nil
	}
	groups := map[string]map[string]bool{}
	for _, view := range plan.Views {
		if strings.HasPrefix(view.Name, "Current") {
			continue
		}
		for _, field := range view.Fields {
			if group := reflect.StructTag(field.Tag).Get("invariant"); group != "" {
				if groups[view.Identity] == nil {
					groups[view.Identity] = map[string]bool{}
				}
				groups[view.Identity][group] = true
			}
		}
	}
	result := make([]EntityInvariant, 0, len(support.Invariants))
	for _, invariant := range support.Invariants {
		if groups[invariant.Identity][strings.TrimSpace(invariant.Group)] {
			result = append(result, invariant)
		}
	}
	return result
}

func invariantIndexSource(packageName string, invariants []EntityInvariant) string {
	var b strings.Builder
	b.WriteString("package ")
	b.WriteString(packageName)
	b.WriteString("\n\n// Generated invariant contracts. Implementations live with their entity receiver.\n")
	for _, invariant := range invariants {
		if group := strings.TrimSpace(invariant.Group); group != "" {
			b.WriteString("// Backfill")
			b.WriteString(group)
			b.WriteString("IfNeeded\n")
		}
	}
	return b.String()
}

func supportRoleSource(packageName, role string) string {
	return "package " + packageName + "\n\n// Generated " + role + " support is executed by Datly's universal mutation writer.\n"
}
