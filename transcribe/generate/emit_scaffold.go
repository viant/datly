package generate

import (
	"fmt"
	"path/filepath"
	"sort"
)

type EmittedFile struct {
	Path    string
	Content string
}

func EmitScaffold(dir string, plan *Plan) ([]EmittedFile, error) {
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
		persistence := &scaffoldPersistence{dir: packages.dirs[i], owner: p.ComponentName, files: files, userFiles: userFiles, removals: removals, plan: p}
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
	if plan.Input.Ownership == ContractLinked {
		removals = append(removals, plan.Input.Destination)
	}
	if plan.Output.Ownership == ContractLinked {
		removals = append(removals, plan.Output.Destination)
	}
	return files, userFiles, removals, nil
}
