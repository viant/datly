package xgen

import (
	"bytes"
	"context"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/viant/datly/repository/shape/dql/shape"
	shapeload "github.com/viant/datly/repository/shape/load"
	"github.com/viant/datly/repository/shape/typectx"
	utypes "github.com/viant/datly/utils/types"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/extension"
	"github.com/viant/datly/view/state"
	viewtags "github.com/viant/datly/view/tags"
	"github.com/viant/tagly/format/text"
	"github.com/viant/xreflect"
	"github.com/viant/xunsafe"
	"reflect"
)

// ComponentCodegen generates Go source code for a complete component package:
// Input struct, Output struct, entity view structs, init() registration,
// //go:embed directive, and DefineComponent function.
//
// This is the shape pipeline equivalent of repository.Component.GenerateOutputCode.
type ComponentCodegen struct {
	Component    *shapeload.Component
	Resource     *view.Resource
	TypeContext  *typectx.Context
	ProjectDir   string
	PackageDir   string
	PackageName  string
	PackagePath  string
	FileName     string // defaults to <component_name>.go
	WithEmbed    bool   // generate //go:embed and EmbedFS method
	WithContract bool   // generate DefineComponent function
	WithRegister *bool  // generate init() with core.RegisterType (default: true)
}

// ComponentCodegenResult captures generation outputs.
type ComponentCodegenResult struct {
	FilePath       string
	PackageDir     string
	PackagePath    string
	PackageName    string
	Types          []string
	GeneratedFiles []string
	InputFilePath  string
	OutputFilePath string
	ViewFilePath   string
	RouterFilePath string
	VeltyFilePath  string
	Embeds         map[string]string // SQL file name → SQL content
}

type codegenSelectorHolder struct {
	FieldName     string
	QuerySelector string
	Type          reflect.Type
}

// Generate produces the component Go source file.
func (g *ComponentCodegen) Generate() (*ComponentCodegenResult, error) {
	if g.Component == nil {
		return nil, fmt.Errorf("shape codegen: nil component")
	}
	if g.Resource == nil {
		return nil, fmt.Errorf("shape codegen: nil resource")
	}

	projectDir := g.ProjectDir
	if projectDir == "" {
		return nil, fmt.Errorf("shape codegen: project dir required")
	}

	packageDir := g.PackageDir
	if packageDir == "" && g.TypeContext != nil {
		packageDir = g.TypeContext.PackageDir
	}
	if packageDir == "" {
		return nil, fmt.Errorf("shape codegen: package dir required")
	}
	if !filepath.IsAbs(packageDir) {
		packageDir = filepath.Join(projectDir, packageDir)
	}

	packageName := g.PackageName
	if packageName == "" && g.TypeContext != nil {
		packageName = g.TypeContext.PackageName
	}
	if packageName == "" {
		packageName = filepath.Base(packageDir)
	}

	packagePath := g.PackagePath
	if packagePath == "" && g.TypeContext != nil {
		packagePath = g.TypeContext.PackagePath
	}

	componentName := g.componentName()
	inputTypeName := g.inputTypeName(componentName)
	outputTypeName := g.outputTypeName(componentName)
	rootViewTypeName := g.rootViewTypeName(componentName)
	embedURI := text.CaseFormatUpperCamel.Format(componentName, text.CaseFormatLowerUnderscore)
	explicitOutputParams := cloneCodegenParameters(g.Component.OutputParameters())
	hasExplicitOutput := len(explicitOutputParams) > 0

	defaultFileName := g.FileName
	if defaultFileName == "" {
		defaultFileName = embedURI + ".go"
	}
	outputFileName := g.resolveOutputDestFileName(defaultFileName)
	inputFileName := g.resolveInputDestFileName(outputFileName)
	viewFileName := g.resolveViewDestFileName(outputFileName)
	routerFileName := g.resolveRouterDestFileName("")

	shapeFragment, err := g.generateShapeFragment(projectDir, packageDir, packageName, packagePath)
	if err != nil {
		return nil, err
	}

	// Build Input/Output types using state.Parameters.ReflectType
	lookupType := g.componentLookupType(packagePath)

	var inputType, outputType reflect.Type
	var selectorHolders []codegenSelectorHolder
	inputParams := state.Parameters(nil)
	if params := g.codegenInputParameters(); len(params) > 0 || strings.TrimSpace(g.Component.URI) != "" {
		normalized := params
		inputParams, selectorHolders = g.partitionInputParametersForCodegen(normalized, packagePath, lookupType)
		normalizeBodyInputTypesForCodegen(inputParams, packagePath, lookupType)
		inputOpts := []state.ReflectOption{state.WithSetMarker(), state.WithTypeName(inputTypeName)}
		if g.componentUsesVelty() {
			inputOpts = append(inputOpts, state.WithVelty(true))
		}
		rt, err := inputParams.ReflectType(packagePath, lookupType, inputOpts...)
		if err == nil && rt != nil {
			inputType = rt
		}
	}

	// Build output parameters — use explicit ones or synthesize defaults for readers
	outputParams := cloneCodegenParameters(explicitOutputParams)
	if !hasExplicitOutput {
		outputParams = g.defaultOutputParameters(componentName)
	}
	g.syncOutputSummarySchemasForCodegen(outputParams)
	// Resolve wildcard output types to the view entity type
	g.resolveOutputWildcardTypes(outputParams, componentName)
	if len(outputParams) > 0 {
		rt, err := outputParams.ReflectType(packagePath, lookupType)
		if err == nil && rt != nil {
			outputType = rt
		}
	}

	shapeTypeNames := map[string]bool{}
	if shapeFragment != nil {
		for _, typeName := range shapeFragment.Types {
			typeName = strings.TrimSpace(typeName)
			if typeName != "" {
				shapeTypeNames[typeName] = true
			}
		}
	}

	inputHelpers := collectNamedHelperTypes(inputType, packagePath, shapeTypeNames)
	selectorHelpers := []namedHelperType{}
	selectorTypeImports := []string{}
	for _, holder := range selectorHolders {
		if holder.Type == nil {
			continue
		}
		selectorHelpers = append(selectorHelpers, collectNamedHelperTypes(holder.Type, packagePath, shapeTypeNames)...)
		selectorTypeImports = mergeImportPaths(selectorTypeImports, collectTypeImports(holder.Type, packagePath))
	}
	outputHelpers := collectNamedHelperTypes(outputType, packagePath, shapeTypeNames)
	mutableSupport := g.mutableSupport(inputType)
	emitResponseImport := g.outputUsesResponse(outputParams) || mutableSupport != nil
	mutableOutputImports := []string{}
	if mutableSupport != nil {
		mutableOutputImports = append(mutableOutputImports, "github.com/viant/xdatly/handler/validator")
	}

	var initBuilder strings.Builder
	// init() registration
	initBuilder.WriteString("func init() {\n")
	if g.withRegister() {
		registryPackage := strings.TrimSpace(packagePath)
		if registryPackage == "" {
			registryPackage = packageName
		}
		registered := map[string]bool{}
		if inputType != nil {
			initBuilder.WriteString(fmt.Sprintf("\tcore.RegisterType(%q, %q, reflect.TypeOf(%s{}), checksum.GeneratedTime)\n",
				registryPackage, inputTypeName, inputTypeName))
			registered[inputTypeName] = true
		}
		initBuilder.WriteString(fmt.Sprintf("\tcore.RegisterType(%q, %q, reflect.TypeOf(%s{}), checksum.GeneratedTime)\n",
			registryPackage, outputTypeName, outputTypeName))
		registered[outputTypeName] = true
		if shapeFragment != nil {
			for _, typeName := range shapeFragment.Types {
				typeName = strings.TrimSpace(typeName)
				if typeName == "" || registered[typeName] {
					continue
				}
				initBuilder.WriteString(fmt.Sprintf("\tcore.RegisterType(%q, %q, reflect.TypeOf(%s{}), checksum.GeneratedTime)\n",
					registryPackage, typeName, typeName))
				registered[typeName] = true
			}
		}
		for _, helper := range inputHelpers {
			if helper.TypeName == "" || registered[helper.TypeName] {
				continue
			}
			initBuilder.WriteString(fmt.Sprintf("\tcore.RegisterType(%q, %q, reflect.TypeOf(%s{}), checksum.GeneratedTime)\n",
				registryPackage, helper.TypeName, helper.TypeName))
			registered[helper.TypeName] = true
		}
		for _, helper := range outputHelpers {
			if helper.TypeName == "" || registered[helper.TypeName] {
				continue
			}
			initBuilder.WriteString(fmt.Sprintf("\tcore.RegisterType(%q, %q, reflect.TypeOf(%s{}), checksum.GeneratedTime)\n",
				registryPackage, helper.TypeName, helper.TypeName))
			registered[helper.TypeName] = true
		}
		for _, helper := range selectorHelpers {
			if helper.TypeName == "" || registered[helper.TypeName] {
				continue
			}
			initBuilder.WriteString(fmt.Sprintf("\tcore.RegisterType(%q, %q, reflect.TypeOf(%s{}), checksum.GeneratedTime)\n",
				registryPackage, helper.TypeName, helper.TypeName))
			registered[helper.TypeName] = true
		}
	}
	initBuilder.WriteString("}\n\n")

	var inputBuilder strings.Builder
	if inputType != nil || g.WithContract {
		inputBuilder.WriteString(fmt.Sprintf("type %s struct {\n", inputTypeName))
		if inputType != nil {
			inputBuilder.WriteString(structFieldsSource(inputType))
		}
		if mutableSupport != nil {
			mutableSupport.renderInputFields(&inputBuilder)
		}
		inputBuilder.WriteString("}\n\n")
	}
	for _, helper := range inputHelpers {
		inputBuilder.WriteString(helper.Decl)
	}
	if g.WithEmbed && inputType != nil {
		inputBuilder.WriteString(fmt.Sprintf("func (i *%s) EmbedFS() *embed.FS {\n", inputTypeName))
		inputBuilder.WriteString(fmt.Sprintf("\treturn &%sFS\n", componentName))
		inputBuilder.WriteString("}\n\n")
	}

	var outputBuilder strings.Builder
	var routerBuilder strings.Builder
	if g.WithEmbed {
		outputBuilder.WriteString(fmt.Sprintf("//go:embed %s/*.sql\n", embedURI))
		outputBuilder.WriteString(fmt.Sprintf("var %sFS embed.FS\n\n", componentName))
	}
	outputRenderParams := cloneCodegenParameters(explicitOutputParams)
	if !hasExplicitOutput {
		outputRenderParams = g.defaultOutputParameters(componentName)
	}
	g.resolveOutputWildcardTypes(outputRenderParams, componentName)
	g.renderOutputStruct(&outputBuilder, outputTypeName, rootViewTypeName, embedURI, outputRenderParams, outputType, mutableSupport)
	for _, helper := range outputHelpers {
		outputBuilder.WriteString(helper.Decl)
	}
	if g.WithContract {
		g.renderComponentHolder(&routerBuilder, componentName, inputTypeName, outputTypeName, selectorHolders)
		for _, helper := range selectorHelpers {
			routerBuilder.WriteString(helper.Decl)
		}
		g.renderDefineComponent(&outputBuilder, componentName, inputTypeName, outputTypeName)
	}

	viewDecls := ""
	viewImports := []string{}
	if shapeFragment != nil {
		viewDecls = strings.TrimSpace(shapeFragment.TypeDecls)
		viewImports = shapeFragment.Imports
	}

	outputParts := []string{initBuilder.String(), outputBuilder.String()}
	routerParts := []string{}
	if strings.TrimSpace(routerBuilder.String()) != "" {
		if routerFileName == "" || routerFileName == outputFileName {
			outputParts = append(outputParts, routerBuilder.String())
		} else {
			routerParts = append(routerParts, routerBuilder.String())
		}
	}
	inputParts := []string{inputBuilder.String()}
	viewParts := []string{}
	if viewDecls != "" {
		viewParts = append(viewParts, viewDecls+"\n")
	}

	if err := os.MkdirAll(packageDir, 0o755); err != nil {
		return nil, err
	}
	outputDest := filepath.Join(packageDir, outputFileName)
	inputDest := outputDest
	viewDest := outputDest
	routerDest := ""
	inputInitDest := ""
	inputValidateDest := ""
	if inputFileName != "" {
		inputDest = filepath.Join(packageDir, inputFileName)
	}
	if viewFileName != "" {
		viewDest = filepath.Join(packageDir, viewFileName)
	}
	if routerFileName != "" {
		routerDest = filepath.Join(packageDir, routerFileName)
	}
	if mutableSupport != nil && inputDest != "" {
		base := "input"
		if inputFileName != "" && inputFileName != outputFileName {
			base = strings.TrimSuffix(filepath.Base(inputDest), filepath.Ext(inputDest))
		} else if g.Component != nil && g.Component.Directives != nil {
			if dest := strings.TrimSpace(g.Component.Directives.InputDest); dest != "" {
				base = strings.TrimSuffix(filepath.Base(dest), filepath.Ext(dest))
			}
		}
		if strings.TrimSpace(base) == "" {
			base = "input"
		}
		inputInitDest = filepath.Join(packageDir, base+"_init.go")
		inputValidateDest = filepath.Join(packageDir, base+"_validate.go")
	}
	var generatedFiles []string
	appendGenerated := func(dest string) {
		dest = strings.TrimSpace(dest)
		if dest == "" {
			return
		}
		for _, candidate := range generatedFiles {
			if candidate == dest {
				return
			}
		}
		generatedFiles = append(generatedFiles, dest)
	}
	split := outputFileName != inputFileName || outputFileName != viewFileName || (routerFileName != "" && routerFileName != outputFileName)
	var writeErr error
	if !split {
		imports := mergeImportPaths(
			g.buildImports(g.WithContract && (routerFileName == "" || routerFileName == outputFileName), emitResponseImport),
			viewImports,
			collectTypeImports(inputType, packagePath),
			collectTypeImports(outputType, packagePath),
			selectorTypeImports,
			helperImports(inputHelpers),
			helperImports(selectorHelpers),
			helperImports(outputHelpers),
			mutableOutputImports,
		)
		combined := append(append(outputParts, inputParts...), viewParts...)
		writeErr = g.writeSectionFile(outputDest, packageName, imports, combined...)
		if writeErr == nil {
			appendGenerated(outputDest)
			outputFileName = outputDest
		}
	} else {
		outputImports := mergeImportPaths(
			g.buildImports(g.WithContract && (routerFileName == "" || routerFileName == outputFileName), emitResponseImport),
			collectTypeImports(outputType, packagePath),
			helperImports(outputHelpers),
			mutableOutputImports,
		)
		if routerFileName == "" || routerFileName == outputFileName {
			outputImports = mergeImportPaths(outputImports, selectorTypeImports, helperImports(selectorHelpers))
		}
		if viewFileName == outputFileName {
			outputImports = mergeImportPaths(outputImports, viewImports)
		}
		if writeErr = g.writeSectionFile(outputDest, packageName, outputImports, outputParts...); writeErr != nil {
			return nil, writeErr
		}
		appendGenerated(outputDest)
		if inputFileName == outputFileName {
			if writeErr = g.appendSectionFile(outputDest, inputParts...); writeErr != nil {
				return nil, writeErr
			}
		} else if strings.TrimSpace(strings.Join(inputParts, "")) != "" {
			inputImports := []string{}
			if g.WithEmbed && inputType != nil {
				inputImports = append(inputImports, "embed")
			}
			inputImports = mergeImportPaths(inputImports, collectTypeImports(inputType, packagePath), helperImports(inputHelpers))
			if viewFileName == inputFileName {
				inputImports = mergeImportPaths(inputImports, viewImports)
			}
			if writeErr = g.writeSectionFile(inputDest, packageName, inputImports, inputParts...); writeErr != nil {
				return nil, writeErr
			}
			appendGenerated(inputDest)
		}
		if len(viewParts) > 0 {
			if viewFileName == outputFileName {
				if writeErr = g.appendSectionFile(outputDest, viewParts...); writeErr != nil {
					return nil, writeErr
				}
			} else if viewFileName == inputFileName {
				if _, statErr := os.Stat(inputDest); statErr == nil {
					if writeErr = g.appendSectionFile(inputDest, viewParts...); writeErr != nil {
						return nil, writeErr
					}
				} else {
					if writeErr = g.writeSectionFile(inputDest, packageName, viewImports, viewParts...); writeErr != nil {
						return nil, writeErr
					}
					appendGenerated(inputDest)
				}
			} else {
				if writeErr = g.writeSectionFile(viewDest, packageName, viewImports, viewParts...); writeErr != nil {
					return nil, writeErr
				}
				appendGenerated(viewDest)
			}
		}
		if len(routerParts) > 0 {
			routerImports := mergeImportPaths(g.buildRouterImports(), selectorTypeImports, helperImports(selectorHelpers))
			if writeErr = g.writeSectionFile(routerDest, packageName, routerImports, routerParts...); writeErr != nil {
				return nil, writeErr
			}
			appendGenerated(routerDest)
		}
		outputFileName = outputDest
	}
	if writeErr != nil {
		return nil, writeErr
	}
	if mutableSupport != nil {
		if writeErr = g.writeSectionFile(inputInitDest, packageName, []string{"context", "github.com/viant/xdatly/handler"}, mutableSupport.renderInputInit(inputTypeName, outputTypeName)); writeErr != nil {
			return nil, writeErr
		}
		appendGenerated(inputInitDest)
		if writeErr = g.writeSectionFile(inputValidateDest, packageName, []string{"context", "github.com/viant/xdatly/handler", "github.com/viant/xdatly/handler/validator"}, mutableSupport.renderInputValidate(inputTypeName, outputTypeName)); writeErr != nil {
			return nil, writeErr
		}
		appendGenerated(inputValidateDest)
	}
	veltyDest := ""
	if mutableSupport != nil && !g.componentUsesHandler() {
		var veltyBody string
		var ok bool
		veltyBody, ok, writeErr = g.renderMutableDSQL(inputType)
		if writeErr != nil {
			return nil, writeErr
		}
		if ok {
			veltyDest = filepath.Join(packageDir, text.CaseFormatUpperCamel.Format(componentName, text.CaseFormatLowerUnderscore), "patch.sql")
			if writeErr = os.MkdirAll(filepath.Dir(veltyDest), 0o755); writeErr != nil {
				return nil, writeErr
			}
			if writeErr = os.WriteFile(veltyDest, []byte(veltyBody), 0o644); writeErr != nil {
				return nil, writeErr
			}
			appendGenerated(veltyDest)
			for _, helperFile := range g.mutableHelperSQLFiles(mutableSupport) {
				if strings.TrimSpace(helperFile.Path) == "" || strings.TrimSpace(helperFile.Content) == "" {
					continue
				}
				if writeErr = os.MkdirAll(filepath.Dir(helperFile.Path), 0o755); writeErr != nil {
					return nil, writeErr
				}
				if writeErr = os.WriteFile(helperFile.Path, []byte(helperFile.Content), 0o644); writeErr != nil {
					return nil, writeErr
				}
				appendGenerated(helperFile.Path)
			}
		}
	}
	if writeErr = g.writeGeneratedSQLFiles(packageDir, appendGenerated); writeErr != nil {
		return nil, writeErr
	}

	var typeNames []string
	if inputType != nil {
		typeNames = append(typeNames, inputTypeName)
	}
	if outputType != nil {
		typeNames = append(typeNames, outputTypeName)
	}
	if shapeFragment != nil {
		typeNames = append(typeNames, shapeFragment.Types...)
	}

	return &ComponentCodegenResult{
		FilePath:       outputFileName,
		PackageDir:     packageDir,
		PackagePath:    packagePath,
		PackageName:    packageName,
		Types:          typeNames,
		GeneratedFiles: generatedFiles,
		InputFilePath:  inputDest,
		OutputFilePath: outputDest,
		ViewFilePath:   viewDest,
		RouterFilePath: routerDest,
		VeltyFilePath:  veltyDest,
	}, nil
}

func (g *ComponentCodegen) writeGeneratedSQLFiles(packageDir string, appendGenerated func(dest string)) error {
	if g == nil || g.Resource == nil {
		return nil
	}
	written := map[string]bool{}
	visited := map[*view.View]bool{}
	var visitView func(aView *view.View) error
	visitView = func(aView *view.View) error {
		if aView == nil || visited[aView] {
			return nil
		}
		visited[aView] = true
		if err := g.writeGeneratedSQLFile(packageDir, aView.Template, appendGenerated, written); err != nil {
			return err
		}
		if aView.Template != nil && aView.Template.Summary != nil {
			if err := g.writeGeneratedSummarySQLFile(packageDir, aView.Template.Summary, appendGenerated, written); err != nil {
				return err
			}
		}
		for _, relation := range aView.With {
			if relation == nil || relation.Of == nil {
				continue
			}
			if err := visitView(&relation.Of.View); err != nil {
				return err
			}
		}
		return nil
	}
	if root := g.rootResourceView(); root != nil {
		return visitView(root)
	}
	for _, candidate := range g.Resource.Views {
		if err := visitView(candidate); err != nil {
			return err
		}
	}
	return nil
}

func (g *ComponentCodegen) writeGeneratedSQLFile(packageDir string, tmpl *view.Template, appendGenerated func(dest string), written map[string]bool) error {
	if tmpl == nil {
		return nil
	}
	sqlURI := strings.TrimSpace(tmpl.SourceURL)
	if sqlURI == "" {
		return nil
	}
	content, err := g.resolveSQLContent(sqlURI, tmpl.Source)
	if err != nil {
		return err
	}
	if strings.TrimSpace(content) == "" {
		return nil
	}
	return writeGeneratedSQLFile(packageDir, sqlURI, content, appendGenerated, written)
}

func (g *ComponentCodegen) writeGeneratedSummarySQLFile(packageDir string, summary *view.TemplateSummary, appendGenerated func(dest string), written map[string]bool) error {
	if summary == nil {
		return nil
	}
	sqlURI := strings.TrimSpace(summary.SourceURL)
	if sqlURI == "" {
		return nil
	}
	content, err := g.resolveSQLContent(sqlURI, summary.Source)
	if err != nil {
		return err
	}
	if strings.TrimSpace(content) == "" {
		return nil
	}
	return writeGeneratedSQLFile(packageDir, sqlURI, content, appendGenerated, written)
}

func writeGeneratedSQLFile(packageDir, sqlURI, content string, appendGenerated func(dest string), written map[string]bool) error {
	sqlURI = strings.TrimSpace(sqlURI)
	if sqlURI == "" || strings.Contains(sqlURI, "://") || strings.HasPrefix(sqlURI, "/") {
		return nil
	}
	dest := filepath.Join(packageDir, filepath.FromSlash(sqlURI))
	if written[dest] {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(dest, []byte(content), 0o644); err != nil {
		return err
	}
	written[dest] = true
	appendGenerated(dest)
	return nil
}

func (g *ComponentCodegen) resolveSQLContent(sqlURI, fallback string) (string, error) {
	if g != nil && g.Resource != nil {
		if content, err := g.Resource.LoadText(context.Background(), sqlURI); err == nil && strings.TrimSpace(content) != "" {
			return content, nil
		}
	}
	fallback = strings.TrimSpace(fallback)
	if fallback != "" {
		return fallback, nil
	}
	return "", nil
}

func normalizeBodyInputTypesForCodegen(params state.Parameters, pkgPath string, lookupType xreflect.LookupType) {
	for _, param := range params {
		if param == nil || param.In == nil || param.In.Kind != state.KindRequestBody || param.Schema == nil {
			continue
		}
		if param.Schema.Cardinality != state.One {
			continue
		}
		rType := param.Schema.Type()
		if rType == nil {
			if resolved, err := utypes.LookupType(lookupType, param.Schema.DataType, xreflect.WithPackage(param.Schema.Package)); err == nil && resolved != nil {
				rType = resolved
			} else if resolved, err := utypes.LookupType(lookupType, param.Schema.DataType, xreflect.WithPackage(pkgPath)); err == nil && resolved != nil {
				rType = resolved
			}
		}
		if rType != nil && rType.Kind() == reflect.Struct {
			param.Schema.SetType(reflect.PtrTo(rType))
		}
	}
}

func (g *ComponentCodegen) refreshSummarySchemasForCodegen() {
	if g == nil || g.Resource == nil {
		return
	}
	visited := map[*view.View]bool{}
	for _, aView := range g.Resource.Views {
		g.refreshViewSummarySchemasForCodegen(context.Background(), aView, visited)
	}
}

func (g *ComponentCodegen) refreshViewSummarySchemasForCodegen(ctx context.Context, aView *view.View, visited map[*view.View]bool) {
	if aView == nil || visited[aView] {
		return
	}
	visited[aView] = true
	if aView.Template != nil && aView.Template.Summary != nil {
		_ = aView.Template.Init(ctx, g.Resource, aView)
	}
	for _, rel := range aView.With {
		if rel == nil || rel.Of == nil {
			continue
		}
		g.refreshViewSummarySchemasForCodegen(ctx, &rel.Of.View, visited)
	}
}

func (g *ComponentCodegen) syncOutputSummarySchemasForCodegen(params state.Parameters) {
	root := g.rootResourceView()
	if root == nil || root.Template == nil || root.Template.Summary == nil || root.Template.Summary.Schema == nil {
		return
	}
	for _, param := range params {
		if param == nil || param.In == nil || param.In.Name != "summary" {
			continue
		}
		param.Schema = root.Template.Summary.Schema.Clone()
	}
}

func normalizeInputParametersForCodegen(params state.Parameters, resource *view.Resource, uri string) state.Parameters {
	result := make(state.Parameters, 0, len(params)+4)
	seenPath := map[string]bool{}
	var stateResource state.Resource
	if resource != nil {
		stateResource = view.NewResources(resource, &view.View{})
	}
	for _, item := range params {
		if item == nil {
			continue
		}
		cloned := *item
		schema := normalizeInputSchemaForCodegen(item.Name, item.In, item.Required != nil && *item.Required, item.Schema, resource)
		cloned.Schema = schema
		if cloned.Schema != nil && stateResource != nil {
			_ = cloned.Schema.Init(stateResource)
			if cloned.In != nil && cloned.In.Kind == state.KindRequestBody && cloned.Schema.Cardinality == state.One {
				normalizeBodySchemaPointerForCodegen(cloned.Schema)
			}
		}
		if cloned.Output != nil {
			output := *cloned.Output
			if cloned.Output.Schema != nil {
				output.Schema = cloned.Output.Schema.Clone()
			}
			cloned.Output = &output
			if stateResource != nil && cloned.Schema != nil && cloned.Schema.Type() != nil {
				_ = cloned.Output.Init(stateResource, cloned.Schema.Type())
			}
		}
		if in := item.In; in != nil && in.Kind == state.KindView {
			viewName := strings.TrimSpace(item.Name)
			if name := strings.TrimSpace(in.Name); name != "" {
				viewName = name
			}
			if v := lookupInputView(resource, viewName); v != nil {
				cloned.Tag = mergeViewSQLTag(cloned.Tag, v)
			}
		}
		cloned.Tag = ensureCodegenTypeNameTag(cloned.Tag, cloned.Schema)
		if in := cloned.In; in != nil && in.Kind == state.KindPath {
			key := strings.ToLower(strings.TrimSpace(in.Name))
			if key == "" {
				key = strings.ToLower(strings.TrimSpace(cloned.Name))
			}
			if key != "" {
				seenPath[key] = true
			}
		}
		result = append(result, &cloned)
	}
	for _, name := range extractCodegenRoutePathParams(uri) {
		key := strings.ToLower(strings.TrimSpace(name))
		if key == "" || seenPath[key] {
			continue
		}
		fieldName := name
		result = append(result, &state.Parameter{
			Name: fieldName,
			In:   state.NewPathLocation(name),
			Schema: &state.Schema{
				DataType:    "string",
				Cardinality: state.One,
			},
		})
		seenPath[key] = true
	}
	return result
}

func (g *ComponentCodegen) codegenInputParameters() state.Parameters {
	if g == nil || g.Component == nil {
		return nil
	}
	params := cloneCodegenParameters(g.Component.InputParameters())
	params = g.mergeMutableTemplateInputParametersForCodegen(params)
	return normalizeInputParametersForCodegen(params, g.Resource, g.Component.URI)
}

func (g *ComponentCodegen) mergeMutableTemplateInputParametersForCodegen(params state.Parameters) state.Parameters {
	if g == nil || !g.componentUsesVelty() {
		return params
	}
	root := g.rootResourceView()
	if root == nil || root.Template == nil || !root.Template.UseParameterStateType || len(root.Template.Parameters) == 0 {
		return params
	}
	result := cloneCodegenParameters(params)
	seen := map[string]bool{}
	for _, item := range result {
		if item == nil {
			continue
		}
		seen[codegenParameterKey(item)] = true
	}
	for _, item := range root.Template.Parameters {
		if item == nil {
			continue
		}
		key := codegenParameterKey(item)
		if seen[key] {
			continue
		}
		cloned := *item
		if item.Schema != nil {
			cloned.Schema = item.Schema.Clone()
		}
		if item.Output != nil {
			output := *item.Output
			if item.Output.Schema != nil {
				output.Schema = item.Output.Schema.Clone()
			}
			cloned.Output = &output
		}
		result = append(result, &cloned)
		seen[key] = true
	}
	return result
}

func exportedCodegenParamName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return ""
	}
	return strings.ToUpper(name[:1]) + name[1:]
}

func extractCodegenRoutePathParams(uri string) []string {
	uri = strings.TrimSpace(uri)
	if uri == "" {
		return nil
	}
	var result []string
	seen := map[string]bool{}
	for {
		start := strings.IndexByte(uri, '{')
		if start == -1 {
			break
		}
		uri = uri[start+1:]
		end := strings.IndexByte(uri, '}')
		if end == -1 {
			break
		}
		name := strings.TrimSpace(uri[:end])
		uri = uri[end+1:]
		if name == "" {
			continue
		}
		key := strings.ToLower(name)
		if seen[key] {
			continue
		}
		seen[key] = true
		result = append(result, name)
	}
	return result
}

func cloneCodegenParameters(params state.Parameters) state.Parameters {
	if len(params) == 0 {
		return nil
	}
	result := make(state.Parameters, 0, len(params))
	for _, item := range params {
		if item == nil {
			continue
		}
		cloned := *item
		if item.Schema != nil {
			cloned.Schema = item.Schema.Clone()
		}
		if item.Output != nil {
			output := *item.Output
			if item.Output.Schema != nil {
				output.Schema = item.Output.Schema.Clone()
			}
			cloned.Output = &output
		}
		result = append(result, &cloned)
	}
	return result
}

func (g *ComponentCodegen) partitionInputParametersForCodegen(params state.Parameters, packagePath string, lookupType xreflect.LookupType) (state.Parameters, []codegenSelectorHolder) {
	if len(params) == 0 {
		return nil, nil
	}
	selectorByKey := map[string]string{}
	if g != nil && g.Component != nil {
		for _, item := range g.Component.Input {
			if item == nil || strings.TrimSpace(item.QuerySelector) == "" {
				continue
			}
			selectorByKey[codegenParameterKey(&item.Parameter)] = strings.TrimSpace(item.QuerySelector)
		}
	}

	business := make(state.Parameters, 0, len(params))
	grouped := map[string]state.Parameters{}
	order := []string{}
	for _, item := range params {
		if item == nil {
			continue
		}
		querySelector := selectorByKey[codegenParameterKey(item)]
		if querySelector == "" {
			business = append(business, item)
			continue
		}
		if _, ok := grouped[querySelector]; !ok {
			order = append(order, querySelector)
		}
		grouped[querySelector] = append(grouped[querySelector], item)
	}

	if len(order) == 0 {
		return business, nil
	}

	holders := make([]codegenSelectorHolder, 0, len(order))
	usedNames := map[string]bool{}
	for i, querySelector := range order {
		group := grouped[querySelector]
		holderType, err := group.ReflectType(packagePath, lookupType)
		if err != nil || holderType == nil {
			business = append(business, group...)
			continue
		}
		holders = append(holders, codegenSelectorHolder{
			FieldName:     selectorHolderFieldName(querySelector, i, len(order), usedNames),
			QuerySelector: querySelector,
			Type:          holderType,
		})
	}
	return business, holders
}

func codegenParameterKey(param *state.Parameter) string {
	if param == nil {
		return ""
	}
	kind := ""
	inName := ""
	if param.In != nil {
		kind = strings.ToLower(strings.TrimSpace(string(param.In.Kind)))
		inName = strings.ToLower(strings.TrimSpace(param.In.Name))
	}
	return strings.ToLower(strings.TrimSpace(param.Name)) + "|" + kind + "|" + inName
}

func selectorHolderFieldName(querySelector string, index, total int, used map[string]bool) string {
	name := "ViewSelect"
	if total > 1 {
		base := toUpperCamel(querySelector)
		if base != "" {
			name = base + "Select"
		} else {
			name = fmt.Sprintf("ViewSelect%d", index+1)
		}
	}
	candidate := name
	if used == nil {
		return candidate
	}
	for suffix := 2; used[candidate]; suffix++ {
		candidate = fmt.Sprintf("%s%d", name, suffix)
	}
	used[candidate] = true
	return candidate
}

func normalizeInputSchemaForCodegen(paramName string, in *state.Location, required bool, schema *state.Schema, resource *view.Resource) *state.Schema {
	var cloned state.Schema
	if schema != nil {
		cloned = exportedSchemaCopy(schema)
	}
	kind := state.Kind("")
	if in != nil {
		kind = in.Kind
	}
	if kind == state.KindView {
		if viewSchema := lookupViewSchemaForInput(resource, in, paramName); viewSchema != nil {
			base := exportedSchemaCopy(viewSchema)
			if explicit := strings.TrimSpace(cloned.Name); explicit != "" && strings.TrimSpace(base.Name) == "" {
				base.Name = explicit
			}
			if explicit := strings.TrimSpace(cloned.DataType); explicit != "" && !isDynamicTypeName(explicit) && strings.TrimSpace(base.DataType) == "" {
				base.DataType = explicit
			}
			if explicit := strings.TrimSpace(cloned.Package); explicit != "" && strings.TrimSpace(base.Package) == "" {
				base.Package = explicit
			}
			if explicit := strings.TrimSpace(cloned.PackagePath); explicit != "" && strings.TrimSpace(base.PackagePath) == "" {
				base.PackagePath = explicit
			}
			if explicit := strings.TrimSpace(cloned.ModulePath); explicit != "" && strings.TrimSpace(base.ModulePath) == "" {
				base.ModulePath = explicit
			}
			if explicit := cloned.Cardinality; explicit != "" {
				base.Cardinality = explicit
			}
			cloned = base
		}
	}
	if cloned.Cardinality == "" {
		if kind == state.KindView {
			if required {
				cloned.Cardinality = state.One
			} else {
				cloned.Cardinality = state.Many
			}
		} else {
			cloned.Cardinality = state.One
		}
	}
	if kind != state.KindView && strings.TrimSpace(cloned.DataType) == "" {
		cloned.DataType = "string"
	}
	if kind == state.KindRequestBody && cloned.Cardinality == state.One {
		normalizeBodySchemaPointerForCodegen(&cloned)
	}
	return &cloned
}

func normalizeBodySchemaPointerForCodegen(schema *state.Schema) {
	if schema == nil {
		return
	}
	if rType := schema.Type(); rType != nil {
		for rType.Kind() == reflect.Slice || rType.Kind() == reflect.Array {
			return
		}
		if rType.Kind() == reflect.Ptr {
			return
		}
		if rType.Kind() == reflect.Struct {
			schema.SetType(reflect.PtrTo(rType))
		}
	}
	dataType := strings.TrimSpace(schema.DataType)
	if dataType == "" || strings.HasPrefix(dataType, "*") || strings.HasPrefix(dataType, "[]") {
		return
	}
	if strings.HasPrefix(dataType, "struct {") || strings.HasPrefix(dataType, "interface{") || dataType == "string" || dataType == "int" || dataType == "bool" || dataType == "float64" {
		return
	}
	schema.DataType = "*" + dataType
}

func exportedSchemaCopy(schema *state.Schema) state.Schema {
	if schema == nil {
		return state.Schema{}
	}
	result := state.Schema{
		Package:     schema.Package,
		PackagePath: schema.PackagePath,
		ModulePath:  schema.ModulePath,
		Name:        schema.Name,
		DataType:    schema.DataType,
		Cardinality: schema.Cardinality,
		Methods:     append([]reflect.Method(nil), schema.Methods...),
	}
	if rType := schema.Type(); rType != nil {
		result.SetType(rType)
		if schema.Package != "" {
			result.Package = schema.Package
		}
		if schema.PackagePath != "" {
			result.PackagePath = schema.PackagePath
		}
		if schema.ModulePath != "" {
			result.ModulePath = schema.ModulePath
		}
	}
	return result
}

func lookupViewSchemaForInput(resource *view.Resource, in *state.Location, paramName string) *state.Schema {
	if v := lookupInputView(resource, strings.TrimSpace(paramName)); v != nil && v.Schema != nil {
		return v.Schema
	}
	if in != nil {
		if v := lookupInputView(resource, strings.TrimSpace(in.Name)); v != nil && v.Schema != nil {
			return v.Schema
		}
	}
	return nil
}

func lookupInputView(resource *view.Resource, name string) *view.View {
	if resource == nil {
		return nil
	}
	name = normalizeViewLookupName(name)
	if name == "" {
		return nil
	}
	for _, item := range resource.Views {
		if item == nil {
			continue
		}
		candidates := []string{
			item.Name,
			item.Reference.Ref,
		}
		if item.Schema != nil {
			candidates = append(candidates, item.Schema.Name)
		}
		for _, candidate := range candidates {
			if normalizeViewLookupName(candidate) == name {
				return item
			}
		}
	}
	return nil
}

func normalizeViewLookupName(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if value == "" {
		return ""
	}
	var ret strings.Builder
	ret.Grow(len(value))
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			ret.WriteRune(r)
		}
	}
	return ret.String()
}

func mergeViewSQLTag(existing string, aView *view.View) string {
	tag := buildViewMetadataTag(aView, true, true)
	if tag == nil {
		return existing
	}
	return string(tag.UpdateTag(reflect.StructTag(existing)))
}

func buildViewMetadataTag(aView *view.View, includeName bool, includeSQL bool) *viewtags.Tag {
	if aView == nil {
		return nil
	}
	result := &viewtags.Tag{}
	tagView := &viewtags.View{}
	if includeName {
		tagView.Name = strings.TrimSpace(aView.Name)
	}
	if table := strings.TrimSpace(aView.Table); isStableTableName(table) {
		tagView.Table = table
	}
	if aView.Template != nil && aView.Template.Summary != nil {
		tagView.SummaryURI = strings.TrimSpace(aView.Template.Summary.SourceURL)
	}
	if aView.Groupable {
		value := true
		tagView.Groupable = &value
	}
	if aView.Batch != nil && aView.Batch.Size > 0 && aView.Batch.Size != 10000 {
		tagView.Batch = aView.Batch.Size
	}
	if aView.RelationalConcurrency != nil && aView.RelationalConcurrency.Number > 0 && aView.RelationalConcurrency.Number != 1 {
		tagView.RelationalConcurrency = aView.RelationalConcurrency.Number
	}
	if aView.PublishParent {
		tagView.PublishParent = true
	}
	if aView.Partitioned != nil {
		tagView.PartitionerType = aView.Partitioned.DataType
		tagView.PartitionedConcurrency = aView.Partitioned.Concurrency
	}
	if aView.MatchStrategy != "" && aView.MatchStrategy != view.ReadMatched {
		tagView.Match = string(aView.MatchStrategy)
	}
	if aView.Cache != nil {
		tagView.Cache = strings.TrimSpace(aView.Cache.Reference.Ref)
	}
	if aView.Connector != nil && aView.Connector.Ref != "" {
		tagView.Connector = aView.Connector.Ref
	}
	if selector := aView.Selector; selector != nil {
		if ns := strings.TrimSpace(selector.Namespace); ns != "" {
			tagView.SelectorNamespace = ns
		}
		if selector.NoLimit || selector.Limit != 0 {
			limit := selector.Limit
			tagView.Limit = &limit
		}
		if constraints := selector.Constraints; constraints != nil {
			if constraints.Criteria {
				value := true
				tagView.SelectorCriteria = &value
			}
			if constraints.Projection {
				value := true
				tagView.SelectorProjection = &value
			}
			if constraints.OrderBy {
				value := true
				tagView.SelectorOrderBy = &value
			}
			if constraints.Offset {
				value := true
				tagView.SelectorOffset = &value
			}
			if constraints.Page != nil {
				value := *constraints.Page
				tagView.SelectorPage = &value
			}
			if len(constraints.Filterable) > 0 {
				tagView.SelectorFilterable = append([]string(nil), constraints.Filterable...)
			}
			if len(constraints.OrderByColumn) > 0 {
				tagView.SelectorOrderByColumns = map[string]string{}
				for key, value := range constraints.OrderByColumn {
					tagView.SelectorOrderByColumns[key] = value
				}
			}
		}
	}
	if aView.Tag != "" {
		tagView.CustomTag = aView.Tag
	}
	if tagView.Name != "" || tagView.Table != "" || tagView.SummaryURI != "" || tagView.CustomTag != "" || tagView.Connector != "" ||
		tagView.Cache != "" || tagView.Limit != nil || tagView.Match != "" || tagView.Batch > 0 ||
		tagView.PublishParent || tagView.PartitionerType != "" || tagView.RelationalConcurrency > 0 ||
		tagView.Groupable != nil || tagView.SelectorNamespace != "" || tagView.SelectorCriteria != nil ||
		tagView.SelectorProjection != nil || tagView.SelectorOrderBy != nil || tagView.SelectorOffset != nil ||
		tagView.SelectorPage != nil || len(tagView.SelectorFilterable) > 0 || len(tagView.SelectorOrderByColumns) > 0 {
		result.View = tagView
	}
	if includeSQL && aView.Template != nil {
		if sourceURL := strings.TrimSpace(aView.Template.SourceURL); sourceURL != "" {
			result.SQL = viewtags.NewViewSQL("", sourceURL)
		}
	}
	if result.View == nil && result.SQL.URI == "" && result.SQL.SQL == "" {
		return nil
	}
	return result
}

func removeTagKeys(tag string, keys ...string) string {
	tag = strings.TrimSpace(tag)
	if tag == "" {
		return ""
	}
	for _, key := range keys {
		var updated string
		updated, _ = xreflect.RemoveTag(tag, key)
		tag = strings.TrimSpace(updated)
	}
	return tag
}

func ensureCodegenTypeNameTag(tag string, schema *state.Schema) string {
	if schema == nil {
		return strings.TrimSpace(tag)
	}
	typeName := strings.TrimSpace(schema.Name)
	if typeName == "" {
		return strings.TrimSpace(tag)
	}
	tag = removeTagKeys(tag, "typeName")
	tag = strings.TrimSpace(tag)
	if tag == "" {
		return fmt.Sprintf(`typeName:"%s"`, typeName)
	}
	return tag + ` typeName:"` + typeName + `"`
}

func isDynamicTypeName(name string) bool {
	n := strings.TrimSpace(strings.ToLower(name))
	n = strings.ReplaceAll(n, " ", "")
	switch n {
	case "", "interface{}", "any", "*interface{}", "[]interface{}", "[]any":
		return true
	}
	return false
}

func (g *ComponentCodegen) componentLookupType(packagePath string) xreflect.LookupType {
	localTypes := map[string]reflect.Type{}
	if g != nil && g.Resource != nil {
		for _, aView := range g.Resource.Views {
			if aView == nil {
				continue
			}
			typeName := ""
			if aView.Schema != nil {
				typeName = strings.TrimSpace(aView.Schema.Name)
			}
			if typeName == "" {
				typeName = toUpperCamel(strings.TrimSpace(aView.Name)) + "View"
			}
			if typeName == "" {
				continue
			}
			var rType reflect.Type
			if aView.Schema != nil && aView.Schema.Type() != nil {
				rType = aView.Schema.Type()
			}
			if rType == nil && len(aView.Columns) > 0 {
				rType = buildStructType(columnsFromView(aView), g.viewUsesVelty(aView))
			}
			if rType == nil {
				continue
			}
			key := strings.ToLower(typeName)
			localTypes[key] = rType
			if summary := summaryTemplateOf(aView); summary != nil && summary.Schema != nil {
				if summaryType := summary.Schema.Type(); summaryType != nil {
					summaryName := strings.TrimSpace(summary.Schema.Name)
					if summaryName == "" {
						summaryName = strings.TrimSpace(summary.Name)
					}
					if summaryName != "" {
						localTypes[strings.ToLower(summaryName)] = summaryType
					}
				}
			}
		}
	}
	return func(name string, opts ...xreflect.Option) (reflect.Type, error) {
		base := normalizeLookupTypeName(name)
		if base != "" {
			if rType, ok := localTypes[strings.ToLower(base)]; ok {
				return rType, nil
			}
			if packagePath != "" {
				if linked := xunsafe.LookupType(packagePath + "/" + base); linked != nil {
					return linked, nil
				}
			}
		}
		if builtin, ok := builtinTypeByName(name); ok {
			return builtin, nil
		}
		if builtin, ok := builtinTypeByName(base); ok {
			return builtin, nil
		}
		return nil, fmt.Errorf("type %s not found", name)
	}
}

func builtinTypeByName(name string) (reflect.Type, bool) {
	name = strings.TrimSpace(name)
	if name == "" {
		return nil, false
	}
	if strings.HasPrefix(name, "[]") {
		if elem, ok := builtinTypeByName(strings.TrimPrefix(name, "[]")); ok {
			return reflect.SliceOf(elem), true
		}
	}
	if strings.HasPrefix(name, "*") {
		if elem, ok := builtinTypeByName(strings.TrimPrefix(name, "*")); ok {
			return reflect.PtrTo(elem), true
		}
	}
	switch name {
	case "string":
		return reflect.TypeOf(""), true
	case "bool":
		return reflect.TypeOf(true), true
	case "int":
		return reflect.TypeOf(int(0)), true
	case "int8":
		return reflect.TypeOf(int8(0)), true
	case "int16":
		return reflect.TypeOf(int16(0)), true
	case "int32":
		return reflect.TypeOf(int32(0)), true
	case "int64":
		return reflect.TypeOf(int64(0)), true
	case "uint":
		return reflect.TypeOf(uint(0)), true
	case "uint8":
		return reflect.TypeOf(uint8(0)), true
	case "uint16":
		return reflect.TypeOf(uint16(0)), true
	case "uint32":
		return reflect.TypeOf(uint32(0)), true
	case "uint64":
		return reflect.TypeOf(uint64(0)), true
	case "float32":
		return reflect.TypeOf(float32(0)), true
	case "float64":
		return reflect.TypeOf(float64(0)), true
	case "time.Time":
		return reflect.TypeOf(time.Time{}), true
	}
	return nil, false
}

func normalizeLookupTypeName(name string) string {
	name = strings.TrimSpace(name)
	for strings.HasPrefix(name, "*") || strings.HasPrefix(name, "[]") {
		if strings.HasPrefix(name, "*") {
			name = strings.TrimPrefix(name, "*")
			continue
		}
		name = strings.TrimPrefix(name, "[]")
	}
	if idx := strings.LastIndex(name, "."); idx != -1 {
		name = name[idx+1:]
	}
	return strings.TrimSpace(name)
}

func columnsFromView(aView *view.View) []columnDescriptor {
	result := make([]columnDescriptor, 0, len(aView.Columns))
	for _, col := range aView.Columns {
		if col == nil {
			continue
		}
		result = append(result, columnDescriptor{
			name:     strings.TrimSpace(col.Name),
			dataType: strings.TrimSpace(col.DataType),
			nullable: col.Nullable,
		})
	}
	return result
}

func toUpperCamel(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	var b strings.Builder
	capNext := true
	for _, r := range s {
		if r == '_' || r == '-' || r == ' ' || r == '.' || r == '/' {
			capNext = true
			continue
		}
		if capNext {
			b.WriteRune(unicode.ToUpper(r))
			capNext = false
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

type shapeFragment struct {
	Types     []string
	Imports   []string
	TypeDecls string
}

func (g *ComponentCodegen) generateShapeFragment(projectDir, packageDir, packageName, packagePath string) (*shapeFragment, error) {
	if g == nil || g.Resource == nil || len(g.Resource.Views) == 0 {
		return &shapeFragment{}, nil
	}
	shapeDoc := resourceToShapeDocument(g.Resource, g.TypeContext)
	applyShapeDocViewTypeOverrides(shapeDoc.Root, g.Component)
	shapeCfg := &Config{
		ProjectDir:  projectDir,
		PackageDir:  packageDir,
		PackageName: packageName,
		PackagePath: packagePath,
	}
	if overrides := collectViewTypeOverrides(g.Component); len(overrides) > 0 {
		shapeCfg.ViewTypeNamer = func(ctx ViewTypeContext) string {
			if value := strings.TrimSpace(overrides[strings.ToLower(strings.TrimSpace(ctx.ViewName))]); value != "" {
				return value
			}
			return ""
		}
	}
	hydrateConfigFromTypeContext(shapeDoc, shapeCfg)
	applyDefaults(shapeCfg)
	return g.renderSemanticShapeFragment(shapeCfg, packagePath)
}

func (g *ComponentCodegen) renderSemanticShapeFragment(shapeCfg *Config, packagePath string) (*shapeFragment, error) {
	viewDescriptorsByName := map[string]viewDescriptor{}
	shapeDoc := resourceToShapeDocument(g.Resource, g.TypeContext)
	for _, item := range extractViews(shapeDoc.Root) {
		viewDescriptorsByName[strings.ToLower(strings.TrimSpace(asString(item.name)))] = item
	}
	typeNames := make([]string, 0, len(g.Resource.Views))
	registered := map[string]bool{}
	imports := map[string]bool{}
	var decls strings.Builder
	for _, aView := range g.Resource.Views {
		if aView == nil {
			continue
		}
		typeName := g.resourceViewTypeName(shapeCfg, aView)
		if typeName == "" || registered[typeName] {
			continue
		}
		mutable := false
		if descriptor, ok := viewDescriptorsByName[strings.ToLower(strings.TrimSpace(aView.Name))]; ok {
			mutable = descriptor.mutable
		}
		viewDecl, viewImports, err := g.renderSemanticViewDecl(shapeCfg, aView, packagePath, mutable)
		if err != nil {
			return nil, err
		}
		if strings.TrimSpace(viewDecl) == "" {
			continue
		}
		registered[typeName] = true
		typeNames = append(typeNames, typeName)
		decls.WriteString(viewDecl)
		decls.WriteString("\n")
		for _, imp := range viewImports {
			imports[imp] = true
		}
		for _, summary := range g.summaryTypeDecls(aView, packagePath) {
			if registered[summary.name] {
				continue
			}
			registered[summary.name] = true
			typeNames = append(typeNames, summary.name)
			decls.WriteString(summary.decl)
			decls.WriteString("\n")
			for _, imp := range summary.imports {
				imports[imp] = true
			}
		}

		if descriptor, ok := viewDescriptorsByName[strings.ToLower(strings.TrimSpace(aView.Name))]; ok && descriptor.mutable {
			structType := buildHasType(columnsFromView(aView))
			if structType != nil {
				hasTypeName := typeName + "Has"
				if !registered[hasTypeName] {
					registered[hasTypeName] = true
					typeNames = append(typeNames, hasTypeName)
					decls.WriteString(fmt.Sprintf("type %s struct {\n", hasTypeName))
					decls.WriteString(structFieldsSource(structType))
					decls.WriteString("}\n\n")
				}
			}
		}
	}
	mergedImports := make([]string, 0, len(imports))
	for imp := range imports {
		mergedImports = append(mergedImports, imp)
	}
	sort.Strings(mergedImports)
	return &shapeFragment{
		Types:     typeNames,
		Imports:   mergeImportPaths(mergedImports),
		TypeDecls: strings.TrimSpace(decls.String()),
	}, nil
}

type emittedTypeDecl struct {
	name    string
	decl    string
	imports []string
}

func (g *ComponentCodegen) summaryTypeDecls(aView *view.View, currentPackage string) []emittedTypeDecl {
	if aView == nil {
		return nil
	}
	seen := map[string]bool{}
	var result []emittedTypeDecl
	appendSummary := func(summary *view.TemplateSummary) {
		if summary == nil || summary.Schema == nil {
			return
		}
		name := strings.TrimSpace(summary.Schema.Name)
		rType := ensureCodegenStructType(summary.Schema.Type())
		if name == "" || rType == nil || seen[name] {
			return
		}
		seen[name] = true
		result = append(result, emittedTypeDecl{
			name:    name,
			decl:    fmt.Sprintf("type %s struct {\n%s}\n\n", name, structFieldsSource(rType)),
			imports: collectTypeImports(rType, currentPackage),
		})
	}
	appendSummary(summaryTemplateOf(aView))
	for _, rel := range aView.With {
		child := g.semanticView(g.resolveRelationView(rel))
		appendSummary(summaryTemplateOf(child))
	}
	return result
}

func summaryTemplateOf(aView *view.View) *view.TemplateSummary {
	if aView == nil || aView.Template == nil {
		return nil
	}
	return aView.Template.Summary
}

func (g *ComponentCodegen) resourceViewTypeName(shapeCfg *Config, aView *view.View) string {
	if aView == nil {
		return ""
	}
	descriptor := viewDescriptor{
		name:       aView.Name,
		schemaName: "",
		columns:    columnsFromView(aView),
	}
	if aView.Schema != nil {
		descriptor.schemaName = aView.Schema.Name
	}
	return viewTypeName(shapeCfg, descriptor)
}

func (g *ComponentCodegen) renderSemanticViewDecl(shapeCfg *Config, aView *view.View, currentPackage string, mutable bool) (string, []string, error) {
	aView = g.semanticView(aView)
	typeName := g.resourceViewTypeName(shapeCfg, aView)
	if typeName == "" {
		return "", nil, nil
	}
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("type %s struct {\n", typeName))
	imports := map[string]bool{}
	emittedFields := map[string]bool{}
	appendField := func(fieldName, fieldSrc string, fieldImports []string) {
		if strings.TrimSpace(fieldName) == "" {
			fieldName = renderedFieldName(fieldSrc)
		}
		fieldName = strings.TrimSpace(fieldName)
		if fieldName == "" || emittedFields[fieldName] || strings.TrimSpace(fieldSrc) == "" {
			return
		}
		emittedFields[fieldName] = true
		builder.WriteString(fieldSrc)
		for _, imp := range fieldImports {
			imports[imp] = true
		}
	}
	renderedScalar := false
	for _, column := range aView.Columns {
		fieldSrc, fieldImports := g.renderColumnField(aView, column, currentPackage)
		if fieldSrc == "" {
			continue
		}
		renderedScalar = true
		appendField("", fieldSrc, fieldImports)
	}
	if !renderedScalar {
		for _, field := range g.renderScalarFallbackFields(aView, currentPackage) {
			fieldName := strings.TrimSpace(strings.Split(strings.TrimSpace(field.src), " ")[0])
			appendField(fieldName, field.src, field.imports)
		}
	}
	for _, rel := range aView.With {
		fieldSrc, fieldImports := g.renderRelationField(shapeCfg, aView, rel, currentPackage)
		if fieldSrc == "" {
			continue
		}
		appendField(strings.TrimSpace(rel.Holder), fieldSrc, fieldImports)
		if metaSrc, metaImports := g.renderRelationSummaryField(shapeCfg, rel, currentPackage); metaSrc != "" {
			fieldName := ""
			if rel.Of.Template != nil && rel.Of.Template.Summary != nil {
				fieldName = state.StructFieldName(text.CaseFormatUpperCamel, rel.Of.Template.Summary.Name)
			}
			appendField(fieldName, metaSrc, metaImports)
		}
	}
	if aView.SelfReference != nil {
		if holder := strings.TrimSpace(aView.SelfReference.Holder); holder != "" {
			builder.WriteString(fmt.Sprintf("\t%s []interface{} `sqlx:\"-\"`\n", holder))
		}
	}
	if mutable {
		hasTypeName := typeName + "Has"
		builder.WriteString(fmt.Sprintf("\tHas *%s `setMarker:\"true\" format:\"-\" sqlx:\"-\" diff:\"-\" json:\"-\" typeName:\"%s\"`\n", hasTypeName, hasTypeName))
	}
	builder.WriteString("}\n\n")
	resultImports := make([]string, 0, len(imports))
	for imp := range imports {
		resultImports = append(resultImports, imp)
	}
	sort.Strings(resultImports)
	return builder.String(), resultImports, nil
}

func (g *ComponentCodegen) renderColumnField(aView *view.View, column *view.Column, currentPackage string) (string, []string) {
	if aView == nil || column == nil {
		return "", nil
	}
	fieldName := column.FieldName()
	if strings.TrimSpace(fieldName) == "" {
		caseFormat := aView.CaseFormat
		if !caseFormat.IsDefined() {
			caseFormat = text.CaseFormatLowerUnderscore
		}
		fieldName = state.StructFieldName(caseFormat, column.Name)
	}
	rType := column.ColumnType()
	if rType == nil {
		if builtin, ok := builtinTypeByName(column.DataType); ok {
			rType = builtin
		} else if g != nil && g.Resource != nil {
			if lookup := g.Resource.LookupType(); lookup != nil {
				if resolved, err := utypes.LookupType(lookup, column.DataType); err == nil && resolved != nil {
					rType = resolved
				}
			}
			if rType == nil && extension.Config != nil && extension.Config.Types != nil {
				if resolved, err := utypes.LookupType(extension.Config.Types.Lookup, column.DataType); err == nil && resolved != nil {
					rType = resolved
				}
			}
		}
	}
	if rType == nil {
		rType = reflect.TypeOf((*interface{})(nil)).Elem()
	}
	rType = g.normalizeColumnType(column, rType)
	tag := g.columnFieldTag(aView, column)
	explicitType := strings.TrimSpace(column.DataType)
	if strings.HasPrefix(explicitType, "map[") || strings.HasPrefix(explicitType, "[]map[") {
		return fmt.Sprintf("\t%s %s `%s`\n", fieldName, explicitType, tag), nil
	}
	return fmt.Sprintf("\t%s %s `%s`\n", fieldName, goTypeString(rType), tag), collectTypeImports(rType, currentPackage)
}

func (g *ComponentCodegen) renderRelationField(shapeCfg *Config, parent *view.View, rel *view.Relation, currentPackage string) (string, []string) {
	if rel == nil {
		return "", nil
	}
	holder := strings.TrimSpace(rel.Holder)
	if holder == "" {
		return "", nil
	}
	childTypeName := g.relationTypeName(shapeCfg, rel)
	if childTypeName == "" {
		return "", nil
	}
	typeExpr := "*" + childTypeName
	if rel.Cardinality == state.Many {
		typeExpr = "[]*" + childTypeName
	}
	tag := g.relationFieldTag(parent, rel)
	return fmt.Sprintf("\t%s %s `%s`\n", holder, typeExpr, tag), nil
}

func (g *ComponentCodegen) renderRelationSummaryField(shapeCfg *Config, rel *view.Relation, currentPackage string) (string, []string) {
	child := g.semanticView(g.resolveRelationView(rel))
	if child == nil || child.Template == nil || child.Template.Summary == nil || child.Template.Summary.Schema == nil {
		return "", nil
	}
	meta := child.Template.Summary
	fieldName := state.StructFieldName(text.CaseFormatUpperCamel, meta.Name)
	if strings.TrimSpace(fieldName) == "" {
		return "", nil
	}
	typeName := strings.TrimSpace(meta.Schema.Name)
	if typeName == "" {
		return "", nil
	}
	typeExpr := "*" + typeName
	tag := fmt.Sprintf(`json:",omitempty" yaml:",omitempty" sqlx:"-" typeName:"%s"`, typeName)
	return fmt.Sprintf("\t%s %s `%s`\n", fieldName, typeExpr, tag), collectTypeImports(meta.Schema.Type(), currentPackage)
}

func (g *ComponentCodegen) relationTypeName(shapeCfg *Config, rel *view.Relation) string {
	if rel == nil {
		return ""
	}
	for _, name := range []string{
		strings.TrimSpace(rel.Of.View.Name),
		strings.TrimSpace(rel.Of.View.Reference.Ref),
		strings.TrimSpace(rel.Name),
		strings.TrimSpace(rel.Holder),
	} {
		if name == "" {
			continue
		}
		if spec := g.typeSpec("view:" + strings.ToLower(strings.TrimSpace(name))); spec != nil && strings.TrimSpace(spec.TypeName) != "" {
			return strings.TrimSpace(spec.TypeName)
		}
	}
	if candidate := g.semanticView(g.resolveRelationView(rel)); candidate != nil {
		if typeName := strings.TrimSpace(g.resourceViewTypeName(shapeCfg, candidate)); typeName != "" {
			return typeName
		}
	}
	if rel.Of.Schema != nil && strings.TrimSpace(rel.Of.Schema.Name) != "" {
		return strings.TrimSpace(rel.Of.Schema.Name)
	}
	refNames := []string{
		strings.TrimSpace(rel.Of.View.Name),
		strings.TrimSpace(rel.Of.View.Reference.Ref),
		strings.TrimSpace(rel.Name),
	}
	for _, refName := range refNames {
		if refName == "" {
			continue
		}
		for _, candidate := range g.Resource.Views {
			if candidate == nil {
				continue
			}
			if strings.EqualFold(strings.TrimSpace(candidate.Name), refName) || strings.EqualFold(strings.TrimSpace(candidate.Reference.Ref), refName) {
				return g.resourceViewTypeName(shapeCfg, candidate)
			}
		}
	}
	return ""
}

func (g *ComponentCodegen) generatedIndexColumn(aView *view.View) (*view.Column, string, reflect.Type, bool) {
	if aView == nil {
		return nil, "", nil, false
	}
	var candidate *view.Column
	for _, column := range aView.Columns {
		if column == nil {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(column.FieldName()), "Id") || strings.EqualFold(strings.TrimSpace(column.Name), "ID") {
			candidate = column
			break
		}
	}
	if candidate == nil {
		return nil, "", nil, false
	}
	fieldName := strings.TrimSpace(candidate.FieldName())
	if fieldName == "" {
		caseFormat := aView.CaseFormat
		if !caseFormat.IsDefined() {
			caseFormat = text.CaseFormatLowerUnderscore
		}
		fieldName = state.StructFieldName(caseFormat, candidate.Name)
	}
	rType := candidate.ColumnType()
	if rType == nil {
		if builtin, ok := builtinTypeByName(candidate.DataType); ok {
			rType = builtin
		}
	}
	if rType == nil {
		return nil, "", nil, false
	}
	rType = g.normalizeColumnType(candidate, rType)
	return candidate, fieldName, rType, true
}

func (g *ComponentCodegen) columnFieldTag(aView *view.View, column *view.Column) string {
	tag := strings.TrimSpace(column.Tag)
	cleaned, _ := xreflect.RemoveTag(tag, "velty")
	tag = strings.TrimSpace(cleaned)
	groupable := column.Groupable
	if aView != nil && aView.ColumnsConfig != nil {
		if cfg := aView.ColumnsConfig[column.Name]; cfg != nil {
			if cfg.Groupable != nil {
				groupable = *cfg.Groupable
			}
			if cfg.Tag != nil {
				configTag := strings.TrimSpace(strings.Trim(*cfg.Tag, ` `))
				if configTag != "" && !strings.Contains(tag, configTag) {
					if tag != "" {
						tag += " "
					}
					tag += configTag
				}
			}
		}
	}
	if aView != nil && containsFold(aView.Exclude, column.Name) && !strings.Contains(tag, `internal:"true"`) {
		if tag != "" {
			tag += " "
		}
		tag += `internal:"true"`
	}
	if groupable && !strings.Contains(tag, `groupable:"`) {
		if tag != "" {
			tag += " "
		}
		tag += `groupable:"true"`
	}
	sqlxValue := strings.TrimSpace(column.Name)
	if column.Codec != nil && strings.TrimSpace(column.DataType) != "" {
		sqlxValue += ",type=" + strings.TrimSpace(column.DataType)
	}
	if sqlxValue != "" && !strings.Contains(tag, `sqlx:"`) {
		if tag != "" {
			tag += " "
		}
		tag += fmt.Sprintf(`sqlx:"%s"`, sqlxValue)
	}
	if g.resourceViewUsesVelty(aView) && !strings.Contains(tag, `velty:"`) {
		caseFormat := aView.CaseFormat
		if !caseFormat.IsDefined() {
			caseFormat = text.CaseFormatLowerUnderscore
		}
		if tag != "" {
			tag += " "
		}
		tag += fmt.Sprintf(`velty:"%s"`, generateVeltyTagValue(column.Name, caseFormat))
	}
	return normalizeGeneratedTagOrder(strings.TrimSpace(tag))
}

func (g *ComponentCodegen) viewUsesVelty(aView *view.View) bool {
	if aView == nil {
		return false
	}
	switch aView.Mode {
	case view.ModeExec:
		return true
	default:
		return false
	}
}

func (g *ComponentCodegen) resourceViewUsesVelty(aView *view.View) bool {
	if aView == nil || g == nil || g.Component == nil || !g.componentUsesVelty() || g.componentUsesHandler() {
		return false
	}
	if g.viewUsesVelty(aView) {
		return true
	}
	matches := func(value string) bool {
		value = strings.TrimSpace(value)
		if value == "" {
			return false
		}
		return strings.EqualFold(strings.TrimSpace(aView.Name), value) ||
			strings.EqualFold(strings.TrimSpace(aView.Reference.Ref), value)
	}
	for _, input := range g.Component.Input {
		if input == nil || input.In == nil || input.In.Kind != state.KindView {
			continue
		}
		if matches(input.In.Name) || matches(input.Name) {
			return true
		}
	}
	return false
}

func (g *ComponentCodegen) componentUsesMutableHelpers() bool {
	if g == nil || g.Component == nil || !g.componentUsesVelty() || g.componentUsesHandler() {
		return false
	}
	hasBody := false
	hasView := false
	for _, input := range g.Component.Input {
		if input == nil || input.In == nil {
			continue
		}
		switch input.In.Kind {
		case state.KindRequestBody:
			hasBody = true
		case state.KindView:
			hasView = true
		}
	}
	return hasBody && hasView
}

func (g *ComponentCodegen) componentUsesVelty() bool {
	if g == nil {
		return false
	}
	if g.componentUsesHandler() {
		return false
	}
	if g.Resource != nil && g.Component != nil {
		rootViewName := strings.TrimSpace(g.Component.RootView)
		if rootViewName != "" {
			if rootView, _ := g.Resource.View(rootViewName); rootView != nil {
				return g.viewUsesVelty(rootView)
			}
		}
	}
	if g.Component == nil {
		return false
	}
	method := strings.ToUpper(strings.TrimSpace(g.Component.Method))
	return method != "" && method != "GET"
}

func (g *ComponentCodegen) componentUsesHandler() bool {
	if g == nil || g.Component == nil {
		return false
	}
	for _, route := range g.Component.ComponentRoutes {
		if route != nil && strings.TrimSpace(route.Handler) != "" {
			return true
		}
	}
	if g.Resource != nil {
		if rootViewName := strings.TrimSpace(g.Component.RootView); rootViewName != "" {
			if rootView, _ := g.Resource.View(rootViewName); rootView != nil && rootView.Mode == view.ModeHandler {
				return true
			}
		}
	}
	return false
}

func normalizeGeneratedTagOrder(tag string) string {
	tag = strings.TrimSpace(tag)
	if tag == "" {
		return tag
	}
	ordered := make([]string, 0, 4)
	for _, key := range []string{"sqlx", "internal", "groupable", "velty", "json"} {
		value := reflect.StructTag(tag).Get(key)
		if value == "" {
			continue
		}
		ordered = append(ordered, fmt.Sprintf(`%s:%q`, key, value))
		var updated string
		updated, _ = xreflect.RemoveTag(tag, key)
		tag = strings.TrimSpace(updated)
	}
	if tag != "" {
		ordered = append(ordered, tag)
	}
	return strings.Join(ordered, " ")
}

func (g *ComponentCodegen) relationFieldTag(parent *view.View, rel *view.Relation) string {
	child := g.semanticView(g.resolveRelationView(rel))
	if child == nil {
		return ""
	}
	tag := &viewtags.Tag{}
	if metadata := buildViewMetadataTag(child, false, false); metadata != nil {
		tag.View = metadata.View
	}
	if relTag := strings.TrimSpace(child.Tag); relTag != "" {
		if tag.View == nil {
			tag.View = &viewtags.View{}
		}
		tag.View.CustomTag = relTag
	}
	if parent != nil && parent.Cache != nil {
		if tag.View == nil {
			tag.View = &viewtags.View{}
		}
		tag.View.Cache = parent.Cache.Ref
	}
	if parent != nil && parent.Connector != nil && child.Connector != nil && child.Connector.Ref != parent.Connector.Ref {
		if tag.View == nil {
			tag.View = &viewtags.View{}
		}
		tag.View.Connector = child.Connector.Ref
	}
	tag.LinkOn = g.relationLinkTag(parent, child, rel)
	if child.Template != nil {
		tag.SQL = viewtags.NewViewSQL("", strings.TrimSpace(child.Template.SourceURL))
	}
	return string(tag.UpdateTag(``))
}

func (g *ComponentCodegen) normalizeColumnType(column *view.Column, rType reflect.Type) reflect.Type {
	if column == nil || rType == nil {
		return rType
	}
	for rType.Kind() == reflect.Ptr {
		if column.Nullable {
			return rType
		}
		rType = rType.Elem()
	}
	if column.Nullable && rType.Kind() != reflect.Interface && rType.Kind() != reflect.Slice && rType.Kind() != reflect.Map {
		return reflect.PtrTo(rType)
	}
	return rType
}

type renderedField struct {
	src     string
	imports []string
}

func renderedFieldName(src string) string {
	src = strings.TrimSpace(src)
	if src == "" {
		return ""
	}
	parts := strings.Fields(src)
	if len(parts) == 0 {
		return ""
	}
	return strings.TrimSpace(parts[0])
}

func (g *ComponentCodegen) renderScalarFallbackFields(aView *view.View, currentPackage string) []renderedField {
	aView = g.semanticView(aView)
	rType := g.resourceViewStructType(aView.Name)
	rType = ensureCodegenStructType(rType)
	if rType == nil && aView != nil {
		rType = ensureCodegenStructType(aView.ComponentType())
		if rType == nil && aView.Schema != nil {
			rType = ensureCodegenStructType(aView.Schema.Type())
		}
	}
	if rType == nil {
		return nil
	}
	var result []renderedField
	for i := 0; i < rType.NumField(); i++ {
		field := rType.Field(i)
		if !field.IsExported() {
			continue
		}
		if strings.TrimSpace(field.Tag.Get("view")) != "" || strings.TrimSpace(field.Tag.Get("on")) != "" {
			continue
		}
		result = append(result, renderedField{
			src:     fmt.Sprintf("\t%s %s `%s`\n", field.Name, goTypeString(field.Type), string(field.Tag)),
			imports: collectTypeImports(field.Type, currentPackage),
		})
	}
	return result
}

func (g *ComponentCodegen) resolveRelationView(rel *view.Relation) *view.View {
	if rel == nil {
		return nil
	}
	names := []string{
		strings.TrimSpace(rel.Of.View.Reference.Ref),
		strings.TrimSpace(rel.Of.View.Name),
		strings.TrimSpace(rel.Name),
	}
	for _, name := range names {
		if name == "" || g == nil || g.Resource == nil {
			continue
		}
		for _, candidate := range g.Resource.Views {
			if candidate == nil {
				continue
			}
			if strings.EqualFold(strings.TrimSpace(candidate.Name), name) || strings.EqualFold(strings.TrimSpace(candidate.Reference.Ref), name) {
				return candidate
			}
		}
	}
	return &rel.Of.View
}

func (g *ComponentCodegen) semanticView(aView *view.View) *view.View {
	if aView == nil {
		return nil
	}
	merged := *aView
	if merged.ColumnsConfig == nil {
		merged.ColumnsConfig = map[string]*view.ColumnConfig{}
	}
	if g == nil || g.Resource == nil {
		return &merged
	}
	for _, parent := range g.Resource.Views {
		if parent == nil {
			continue
		}
		for _, rel := range parent.With {
			if rel == nil {
				continue
			}
			if !g.matchesViewRef(&merged, rel) {
				continue
			}
			g.mergeViewSemantics(&merged, &rel.Of.View)
		}
	}
	return &merged
}

func (g *ComponentCodegen) matchesViewRef(target *view.View, rel *view.Relation) bool {
	if target == nil || rel == nil {
		return false
	}
	candidates := []string{
		strings.TrimSpace(target.Name),
		strings.TrimSpace(target.Reference.Ref),
	}
	refs := []string{
		strings.TrimSpace(rel.Of.View.Name),
		strings.TrimSpace(rel.Of.View.Reference.Ref),
		strings.TrimSpace(rel.Name),
	}
	for _, candidate := range candidates {
		if candidate == "" {
			continue
		}
		for _, ref := range refs {
			if ref != "" && strings.EqualFold(candidate, ref) {
				return true
			}
		}
	}
	return false
}

func (g *ComponentCodegen) mergeViewSemantics(dst, src *view.View) {
	if dst == nil || src == nil {
		return
	}
	if len(dst.Columns) == 0 && len(src.Columns) > 0 {
		dst.Columns = src.Columns
	}
	if len(dst.Exclude) == 0 && len(src.Exclude) > 0 {
		dst.Exclude = append(dst.Exclude, src.Exclude...)
	}
	if dst.ColumnsConfig == nil {
		dst.ColumnsConfig = map[string]*view.ColumnConfig{}
	}
	for key, cfg := range src.ColumnsConfig {
		if _, ok := dst.ColumnsConfig[key]; !ok {
			dst.ColumnsConfig[key] = cfg
		}
	}
	if dst.Template == nil && src.Template != nil {
		dst.Template = src.Template
	}
	if dst.Template != nil && src.Template != nil {
		if strings.TrimSpace(dst.Template.Source) == "" {
			dst.Template.Source = src.Template.Source
		}
		if strings.TrimSpace(dst.Template.SourceURL) == "" {
			dst.Template.SourceURL = src.Template.SourceURL
		}
		if src.Template.Summary != nil {
			if dst.Template.Summary == nil {
				dst.Template.Summary = src.Template.Summary
			} else {
				if strings.TrimSpace(dst.Template.Summary.Name) == "" {
					dst.Template.Summary.Name = src.Template.Summary.Name
				}
				if dst.Template.Summary.Kind == "" {
					dst.Template.Summary.Kind = src.Template.Summary.Kind
				}
				if strings.TrimSpace(dst.Template.Summary.Source) == "" {
					dst.Template.Summary.Source = src.Template.Summary.Source
				}
				if strings.TrimSpace(dst.Template.Summary.SourceURL) == "" {
					dst.Template.Summary.SourceURL = src.Template.Summary.SourceURL
				}
				if src.Template.Summary.Schema != nil && (dst.Template.Summary.Schema == nil || dst.Template.Summary.Schema.Type() == nil) {
					dst.Template.Summary.Schema = src.Template.Summary.Schema
				}
			}
		}
	}
	if !isStableTableName(dst.Table) && isStableTableName(src.Table) {
		dst.Table = src.Table
	}
	if dst.Schema == nil && src.Schema != nil {
		dst.Schema = src.Schema
		return
	}
	if dst.Schema != nil && src.Schema != nil {
		if dst.Schema.Type() == nil && src.Schema.Type() != nil {
			dst.Schema.SetType(src.Schema.Type())
		}
		if strings.TrimSpace(dst.Schema.Name) == "" {
			dst.Schema.Name = src.Schema.Name
		}
	}
}

func (g *ComponentCodegen) relationLinkTag(parent, child *view.View, rel *view.Relation) viewtags.LinkOn {
	if rel == nil {
		return nil
	}
	result := make([]string, 0, len(rel.On))
	for i, parentLink := range rel.On {
		if parentLink == nil {
			continue
		}
		var childLink *view.Link
		if i < len(rel.Of.On) {
			childLink = rel.Of.On[i]
		}
		left := g.encodeRelationEndpoint(parent, parentLink)
		right := g.encodeRelationEndpoint(child, childLink)
		if left != "" && right != "" {
			result = append(result, left+"="+right)
		}
	}
	return result
}

func (g *ComponentCodegen) encodeRelationEndpoint(owner *view.View, link *view.Link) string {
	if link == nil {
		return ""
	}
	column := stripNamespace(link.Column)
	field := strings.TrimSpace(link.Field)
	if field == "" {
		caseFormat := text.CaseFormatLowerUnderscore
		if owner != nil && owner.CaseFormat.IsDefined() {
			caseFormat = owner.CaseFormat
		}
		field = state.StructFieldName(caseFormat, column)
	}
	if field == "" {
		return column
	}
	if column == "" {
		return field
	}
	return field + ":" + column
}

func stripNamespace(value string) string {
	value = strings.TrimSpace(value)
	if idx := strings.LastIndex(value, "."); idx != -1 {
		return strings.TrimSpace(value[idx+1:])
	}
	return value
}

func looksLikeSQL(value string) bool {
	value = strings.TrimSpace(strings.ToUpper(value))
	return strings.Contains(value, "SELECT ") || strings.Contains(value, "\n") || strings.Contains(value, "(")
}

func isStableTableName(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" || looksLikeSQL(value) {
		return false
	}
	for _, r := range value {
		switch {
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '_' || r == '.':
		default:
			return false
		}
	}
	return true
}

func containsFold(items []string, candidate string) bool {
	candidate = strings.TrimSpace(candidate)
	for _, item := range items {
		if strings.EqualFold(strings.TrimSpace(item), candidate) {
			return true
		}
	}
	return false
}

func generateVeltyTagValue(columnName string, caseFormat text.CaseFormat) string {
	names := columnName
	if fieldName := state.StructFieldName(caseFormat, columnName); fieldName != names {
		names += "|" + fieldName
	}
	return "names=" + names
}

func goTypeString(rType reflect.Type) string {
	if rType == nil {
		return "interface{}"
	}
	switch rType.Kind() {
	case reflect.Ptr:
		return "*" + goTypeString(rType.Elem())
	case reflect.Slice:
		return "[]" + goTypeString(rType.Elem())
	case reflect.Array:
		return fmt.Sprintf("[%d]%s", rType.Len(), goTypeString(rType.Elem()))
	case reflect.Map:
		return fmt.Sprintf("map[%s]%s", goTypeString(rType.Key()), goTypeString(rType.Elem()))
	}
	if rType.Name() != "" {
		if pkg := strings.TrimSpace(rType.PkgPath()); pkg != "" {
			prefix := filepath.Base(pkg)
			if prefix != "" && prefix != "." {
				return prefix + "." + rType.Name()
			}
		}
		return rType.Name()
	}
	return rType.String()
}

func (g *ComponentCodegen) resourceViewStructType(name any) reflect.Type {
	if g == nil || g.Resource == nil {
		return nil
	}
	viewName := strings.TrimSpace(asString(name))
	if viewName == "" {
		return nil
	}
	for _, aView := range g.Resource.Views {
		if aView == nil || !strings.EqualFold(strings.TrimSpace(aView.Name), viewName) {
			continue
		}
		rType := aView.ComponentType()
		if rType == nil && aView.Schema != nil {
			rType = aView.Schema.Type()
		}
		rType = ensureCodegenStructType(rType)
		if rebuilt := rebuildResourceViewStructType(rType, columnsFromView(aView), g.resourceViewUsesVelty(aView)); rebuilt != nil {
			rType = rebuilt
		}
		if augmented := g.augmentResourceViewStructType(aView, rType); augmented != nil {
			return augmented
		}
		return rType
	}
	return nil
}

func ensureCodegenStructType(rType reflect.Type) reflect.Type {
	if rType == nil {
		return nil
	}
	for rType.Kind() == reflect.Ptr || rType.Kind() == reflect.Slice || rType.Kind() == reflect.Array {
		rType = rType.Elem()
	}
	if rType.Kind() != reflect.Struct {
		return nil
	}
	return rType
}

func shouldRenderResourceViewType(rType reflect.Type, columns []columnDescriptor) bool {
	rType = ensureCodegenStructType(rType)
	if rType == nil {
		return false
	}
	if resourceViewNeedsRebuild(rType, columns, false) {
		return true
	}
	return rType.NumField() > len(columns)
}

func rebuildResourceViewStructType(rType reflect.Type, columns []columnDescriptor, includeVelty bool) reflect.Type {
	rType = ensureCodegenStructType(rType)
	if rType == nil {
		if len(columns) == 0 {
			return nil
		}
		return reflect.StructOf(buildStructFields(columns, includeVelty))
	}
	if !resourceViewNeedsRebuild(rType, columns, includeVelty) {
		return rType
	}
	fields := buildStructFields(columns, includeVelty)
	if len(fields) == 0 {
		return rType
	}
	used := map[string]bool{}
	for _, field := range fields {
		used[field.Name] = true
	}
	for i := 0; i < rType.NumField(); i++ {
		field := rType.Field(i)
		if isPlaceholderProjectionField(field) {
			continue
		}
		if used[field.Name] {
			continue
		}
		fields = append(fields, field)
		used[field.Name] = true
	}
	return reflect.StructOf(fields)
}

func (g *ComponentCodegen) augmentResourceViewStructType(aView *view.View, rType reflect.Type) reflect.Type {
	rType = ensureCodegenStructType(rType)
	if aView == nil || rType == nil {
		return rType
	}
	fields := make([]reflect.StructField, 0, rType.NumField()+len(aView.With)+1)
	used := map[string]bool{}
	for i := 0; i < rType.NumField(); i++ {
		field := rType.Field(i)
		fields = append(fields, field)
		used[field.Name] = true
	}
	if aView.SelfReference != nil {
		if holder := strings.TrimSpace(aView.SelfReference.Holder); holder != "" && !used[holder] {
			fields = append(fields, reflect.StructField{
				Name: holder,
				Type: reflect.TypeOf([]interface{}{}),
				Tag:  `sqlx:"-"`,
			})
			used[holder] = true
		}
	}
	for _, rel := range aView.With {
		if rel == nil {
			continue
		}
		holder := strings.TrimSpace(rel.Holder)
		if holder == "" || used[holder] {
			continue
		}
		fieldType := g.relationHolderType(rel)
		if fieldType == nil {
			continue
		}
		tagParts := []string{}
		if table := strings.TrimSpace(rel.Of.View.Table); table != "" {
			tagParts = append(tagParts, fmt.Sprintf(`view:",table=%s"`, table))
		} else {
			tagParts = append(tagParts, `view:""`)
		}
		tagParts = append(tagParts, `sqlx:"-"`)
		fields = append(fields, reflect.StructField{
			Name: holder,
			Type: fieldType,
			Tag:  reflect.StructTag(strings.Join(tagParts, " ")),
		})
		used[holder] = true
	}
	if len(fields) == rType.NumField() {
		return rType
	}
	return reflect.StructOf(fields)
}

func (g *ComponentCodegen) relationHolderType(rel *view.Relation) reflect.Type {
	if rel == nil {
		return nil
	}
	childType := ensureCodegenStructType(rel.Of.View.ComponentType())
	if childType == nil && rel.Of.Schema != nil {
		childType = ensureCodegenStructType(rel.Of.Schema.Type())
	}
	if childType == nil && g != nil && g.Resource != nil {
		refNames := []string{
			strings.TrimSpace(rel.Of.View.Name),
			strings.TrimSpace(rel.Of.View.Reference.Ref),
			strings.TrimSpace(rel.Name),
		}
		for _, refName := range refNames {
			if refName == "" {
				continue
			}
			childType = g.resourceViewStructType(refName)
			if childType != nil {
				break
			}
		}
	}
	if childType == nil {
		return nil
	}
	childPtr := childType
	if childPtr.Kind() != reflect.Ptr {
		childPtr = reflect.PtrTo(childType)
	}
	if rel.Cardinality == state.One {
		return childPtr
	}
	return reflect.SliceOf(childPtr)
}

func resourceViewNeedsRebuild(rType reflect.Type, columns []columnDescriptor, includeVelty bool) bool {
	rType = ensureCodegenStructType(rType)
	if rType == nil {
		return false
	}
	if len(columns) == 0 {
		return false
	}
	for i := 0; i < rType.NumField(); i++ {
		field := rType.Field(i)
		if isPlaceholderProjectionField(field) {
			return true
		}
		if includeVelty && field.Tag.Get("sqlx") != "" && field.Tag.Get("sqlx") != "-" && field.Tag.Get("velty") == "" {
			return true
		}
		if !includeVelty && field.Tag.Get("velty") != "" {
			return true
		}
	}
	return false
}

func isPlaceholderProjectionField(field reflect.StructField) bool {
	if tag := strings.TrimSpace(field.Tag.Get("view")); tag != "" {
		return false
	}
	if tag := strings.TrimSpace(field.Tag.Get("sql")); tag != "" {
		return false
	}
	sqlxTag := field.Tag.Get("sqlx")
	sqlxName := sqlxTagName(sqlxTag)
	if sqlxName == "" || sqlxName == "-" {
		return false
	}
	name := strings.TrimSpace(field.Name)
	if strings.HasPrefix(strings.ToLower(name), "col") && strings.HasPrefix(strings.ToLower(sqlxName), "col_") {
		return true
	}
	return false
}

func sqlxTagName(tag string) string {
	tag = strings.TrimSpace(tag)
	if tag == "" {
		return ""
	}
	if strings.HasPrefix(tag, "name=") {
		tag = strings.TrimPrefix(tag, "name=")
	}
	if idx := strings.Index(tag, ","); idx != -1 {
		tag = tag[:idx]
	}
	return strings.TrimSpace(tag)
}

func collectTypeImports(rType reflect.Type, currentPackage string) []string {
	seen := map[string]bool{}
	var result []string
	var visit func(reflect.Type)
	visit = func(t reflect.Type) {
		if t == nil {
			return
		}
		for t.Kind() == reflect.Ptr || t.Kind() == reflect.Slice || t.Kind() == reflect.Array || t.Kind() == reflect.Map {
			if t.Kind() == reflect.Map {
				visit(t.Key())
			}
			t = t.Elem()
			if t == nil {
				return
			}
		}
		if pkg := strings.TrimSpace(t.PkgPath()); pkg != "" && pkg != currentPackage {
			if !seen[pkg] {
				seen[pkg] = true
				result = append(result, pkg)
			}
			if t.Name() != "" {
				return
			}
		}
		if t.Kind() != reflect.Struct {
			return
		}
		for i := 0; i < t.NumField(); i++ {
			visit(t.Field(i).Type)
		}
	}
	visit(rType)
	sort.Strings(result)
	return result
}

func applyShapeDocViewTypeOverrides(root map[string]any, component *shapeload.Component) {
	if root == nil || component == nil || len(component.TypeSpecs) == 0 {
		return
	}
	resourceMap, _ := root["Resource"].(map[string]any)
	if resourceMap == nil {
		return
	}
	views, _ := resourceMap["Views"].([]any)
	if len(views) == 0 {
		return
	}
	for _, raw := range views {
		viewMap, _ := raw.(map[string]any)
		if viewMap == nil {
			continue
		}
		name, _ := viewMap["Name"].(string)
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		spec, ok := component.TypeSpecs["view:"+name]
		if !ok || spec == nil || strings.TrimSpace(spec.TypeName) == "" {
			continue
		}
		schemaMap, _ := viewMap["Schema"].(map[string]any)
		if schemaMap == nil {
			schemaMap = map[string]any{}
		}
		schemaMap["Name"] = strings.TrimSpace(spec.TypeName)
		viewMap["Schema"] = schemaMap
	}
}

func collectViewTypeOverrides(component *shapeload.Component) map[string]string {
	if component == nil || len(component.TypeSpecs) == 0 {
		return nil
	}
	ret := map[string]string{}
	for key, spec := range component.TypeSpecs {
		if spec == nil || spec.Role != shapeload.TypeRoleView {
			continue
		}
		typeName := strings.TrimSpace(spec.TypeName)
		if typeName == "" {
			continue
		}
		alias := strings.TrimSpace(spec.Alias)
		if alias == "" && strings.HasPrefix(key, "view:") {
			alias = strings.TrimPrefix(key, "view:")
		}
		if alias == "" {
			continue
		}
		ret[strings.ToLower(alias)] = typeName
	}
	if len(ret) == 0 {
		return nil
	}
	return ret
}

func mergeImportPaths(groups ...[]string) []string {
	var result []string
	seen := map[string]bool{}
	for _, group := range groups {
		for _, item := range group {
			item = strings.TrimSpace(item)
			if item == "" || seen[item] {
				continue
			}
			seen[item] = true
			result = append(result, item)
		}
	}
	return result
}

func extractTypeDeclsAndImports(source string) ([]string, string, error) {
	fset := token.NewFileSet()
	fileNode, err := parser.ParseFile(fset, "", source, parser.ParseComments)
	if err != nil {
		return nil, "", err
	}
	var imports []string
	for _, spec := range fileNode.Imports {
		pathValue := strings.Trim(spec.Path.Value, `"`)
		if spec.Name != nil && spec.Name.Name != "" && spec.Name.Name != "." && spec.Name.Name != "_" {
			imports = append(imports, spec.Name.Name+` "`+pathValue+`"`)
			continue
		}
		imports = append(imports, pathValue)
	}

	var body bytes.Buffer
	for _, decl := range fileNode.Decls {
		if typeDecl, ok := decl.(*ast.GenDecl); ok && typeDecl.Tok == token.TYPE {
			if err := format.Node(&body, fset, typeDecl); err != nil {
				return nil, "", err
			}
			body.WriteString("\n\n")
		}
	}
	return imports, strings.TrimSpace(body.String()), nil
}

// renderOutputStruct writes the output struct definition.
// For reader components, it generates the standard pattern:
//
//	type XxxOutput struct {
//	    response.Status `parameter:",kind=output,in=status" json:",omitempty"`
//	    Data []*XxxView  `parameter:",kind=output,in=view" view:"xxx" sql:"uri=xxx/xxx.sql"`
//	}
func (g *ComponentCodegen) renderOutputStruct(builder *strings.Builder, outputTypeName, viewTypeName, embedURI string, outputParams state.Parameters, outputType reflect.Type, mutableSupport *mutableComponentSupport) {
	rootView := g.Component.RootView
	rootViewMetadata := g.rootResourceView()

	builder.WriteString(fmt.Sprintf("type %s struct {\n", outputTypeName))

	// Check if there's an explicit status parameter
	hasStatus := false
	hasViolations := false
	for _, p := range outputParams {
		if p == nil {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(p.Name), "Violations") {
			hasViolations = true
		}
		if p.In != nil && p.In.Name == "status" {
			hasStatus = true
		}
	}
	if !hasStatus && (len(outputParams) > 0 || g.shouldDefaultReaderOutput() || mutableSupport != nil) {
		builder.WriteString("\tresponse.Status `parameter:\",kind=output,in=status\" json:\",omitempty\"`\n")
	}

	for _, p := range outputParams {
		if p == nil || p.In == nil {
			continue
		}
		switch p.In.Name {
		case "view":
			cardinality := string(p.Schema.Cardinality)
			typePrefix := "[]*"
			if cardinality == string(state.One) {
				typePrefix = "*"
			}
			fieldName := p.Name
			if fieldName == "" || fieldName == "Output" {
				fieldName = "Data"
			}
			tag := strings.TrimSpace(p.Tag)
			if !strings.Contains(tag, `parameter:"`) {
				tag = strings.TrimSpace(tag + ` parameter:",kind=output,in=view"`)
			}
			if !strings.Contains(tag, `view:"`) {
				tag = strings.TrimSpace(tag + fmt.Sprintf(` view:"%s"`, rootView))
			}
			if !strings.Contains(tag, `sql:"`) {
				tag = strings.TrimSpace(tag + fmt.Sprintf(` sql:"uri=%s/%s.sql"`, embedURI, rootView))
			}
			if rootViewMetadata != nil {
				tag = mergeViewSQLTag(tag, rootViewMetadata)
			}
			if !strings.Contains(tag, `anonymous:"`) && p.Tag != "" && strings.Contains(p.Tag, "anonymous") {
				tag += ` anonymous:"true"`
			}
			builder.WriteString(fmt.Sprintf("\t%s %s%s `%s`\n", fieldName, typePrefix, viewTypeName, tag))
		case "status":
			builder.WriteString(fmt.Sprintf("\tresponse.Status `parameter:\",kind=output,in=status\" json:\",omitempty\"`\n"))
		default:
			// Other output parameters (meta, etc.)
			typeName := "interface{}"
			if p.Schema != nil && p.Schema.Name != "" {
				typeName = p.Schema.Name
			}
			builder.WriteString(fmt.Sprintf("\t%s %s `parameter:\",kind=output,in=%s\"`\n", p.Name, typeName, p.In.Name))
		}
	}
	if mutableSupport != nil && !hasViolations {
		builder.WriteString("\tViolations validator.Violations `json:\",omitempty\"`\n")
	}

	builder.WriteString("}\n\n")
	if mutableSupport != nil {
		builder.WriteString(fmt.Sprintf("func (o *%s) setError(err error) {\n", outputTypeName))
		builder.WriteString("\to.Status.Message = err.Error()\n")
		builder.WriteString("\to.Status.Status = \"error\"\n")
		builder.WriteString("}\n\n")
	}
}

// resolveOutputWildcardTypes resolves output parameters with wildcard type `?` or empty
// schema to the view entity type. The legacy translator does this in updateParameterWithComponentOutputType.
func (g *ComponentCodegen) resolveOutputWildcardTypes(params state.Parameters, componentName string) {
	viewType := componentName + "View"
	rootView := g.rootResourceView()
	for _, p := range params {
		if p == nil || p.In == nil {
			continue
		}
		if p.In.Kind != state.KindOutput {
			continue
		}
		if p.Schema == nil {
			p.Schema = &state.Schema{}
		}
		// Only view outputs default to the root view shape. Summary/status outputs
		// must keep their own materialized schema types.
		if p.In.Name == "view" && (p.Schema.Name == "" || p.Schema.DataType == "" || p.Schema.DataType == "?") {
			p.Schema.Name = viewType
			p.Schema.DataType = "*" + viewType
			if p.Schema.Cardinality == "" {
				p.Schema.Cardinality = state.Many
			}
		}
		// Add view tag if missing
		if p.In.Name == "view" && !strings.Contains(p.Tag, "view:") {
			rootViewName := g.Component.RootView
			p.Tag += fmt.Sprintf(` view:"%s"`, rootViewName)
		}
		if p.In.Name == "view" && rootView != nil {
			p.Tag = mergeViewSQLTag(p.Tag, rootView)
		}
	}
}

// defaultOutputParameters creates the default output parameters for a reader component:
// - Data: the main view data (anonymous, kind=output, in=view)
// - Status: response status (anonymous, kind=output, in=status)
// This mirrors internal/translator output.go ensureOutputParameters.
func (g *ComponentCodegen) defaultOutputParameters(componentName string) state.Parameters {
	if !g.shouldDefaultReaderOutput() {
		return nil
	}
	rootView := g.Component.RootView
	viewType := componentName + "View"

	// Data parameter — references the root view
	dataParam := &state.Parameter{
		Name: "Data",
		In:   state.NewOutputLocation("view"),
		Tag:  fmt.Sprintf(`anonymous:"true" view:"%s"`, rootView),
		Schema: &state.Schema{
			Name:        viewType,
			DataType:    "*" + viewType,
			Cardinality: state.Many,
		},
	}

	// Status parameter — response.Status
	statusParam := &state.Parameter{
		Name:   "Status",
		In:     state.NewOutputLocation("status"),
		Tag:    `anonymous:"true" json:",omitempty"`,
		Schema: &state.Schema{DataType: "response.Status"},
	}

	return state.Parameters{dataParam, statusParam}
}

func (g *ComponentCodegen) shouldDefaultReaderOutput() bool {
	if g == nil || g.Resource == nil || g.Component == nil {
		return true
	}
	rootViewName := strings.TrimSpace(g.Component.RootView)
	if rootViewName == "" {
		return true
	}
	rootView, _ := g.Resource.View(rootViewName)
	if rootView == nil {
		return true
	}
	switch rootView.Mode {
	case view.ModeExec, view.ModeHandler:
		return false
	default:
		return true
	}
}

func (g *ComponentCodegen) withRegister() bool {
	if g.WithRegister == nil {
		return true // default enabled
	}
	return *g.WithRegister
}

func (g *ComponentCodegen) typeSpec(key string) *shapeload.TypeSpec {
	if g == nil || g.Component == nil || g.Component.TypeSpecs == nil {
		return nil
	}
	return g.Component.TypeSpecs[key]
}

func (g *ComponentCodegen) inputTypeName(componentName string) string {
	if spec := g.typeSpec("input"); spec != nil && strings.TrimSpace(spec.TypeName) != "" {
		return strings.TrimSpace(spec.TypeName)
	}
	return componentName + "Input"
}

func (g *ComponentCodegen) outputTypeName(componentName string) string {
	if spec := g.typeSpec("output"); spec != nil && strings.TrimSpace(spec.TypeName) != "" {
		return strings.TrimSpace(spec.TypeName)
	}
	return componentName + "Output"
}

func (g *ComponentCodegen) rootViewTypeName(componentName string) string {
	rootView := strings.TrimSpace(componentName)
	if g.Component != nil && strings.TrimSpace(g.Component.RootView) != "" {
		rootView = strings.TrimSpace(g.Component.RootView)
	}
	if spec := g.typeSpec("view:" + rootView); spec != nil && strings.TrimSpace(spec.TypeName) != "" {
		return strings.TrimSpace(spec.TypeName)
	}
	return componentName + "View"
}

func (g *ComponentCodegen) rootViewSourceURL() string {
	if g == nil || g.Resource == nil {
		return ""
	}
	rootView := ""
	if g.Component != nil {
		rootView = strings.TrimSpace(g.Component.RootView)
	}
	if rootView != "" {
		if aView, _ := g.Resource.View(rootView); aView != nil && aView.Template != nil {
			return strings.TrimSpace(aView.Template.SourceURL)
		}
	}
	if len(g.Resource.Views) == 0 || g.Resource.Views[0] == nil || g.Resource.Views[0].Template == nil {
		return ""
	}
	return strings.TrimSpace(g.Resource.Views[0].Template.SourceURL)
}

func (g *ComponentCodegen) rootResourceView() *view.View {
	if g == nil || g.Resource == nil {
		return nil
	}
	rootView := ""
	if g.Component != nil {
		rootView = strings.TrimSpace(g.Component.RootView)
	}
	if rootView != "" {
		if aView, _ := g.Resource.View(rootView); aView != nil {
			return aView
		}
	}
	if len(g.Resource.Views) == 0 {
		return nil
	}
	return g.Resource.Views[0]
}

func (g *ComponentCodegen) rootSummarySourceURL() string {
	if g == nil || g.Resource == nil {
		return ""
	}
	rootView := ""
	var candidate *view.View
	if g.Component != nil {
		rootView = strings.TrimSpace(g.Component.RootView)
	}
	if rootView != "" {
		if aView, _ := g.Resource.View(rootView); aView != nil {
			candidate = aView
		}
	}
	if candidate == nil {
		if len(g.Resource.Views) == 0 || g.Resource.Views[0] == nil {
			return ""
		}
		candidate = g.Resource.Views[0]
	}
	if candidate.Template == nil || candidate.Template.Summary == nil {
		return ""
	}
	if sourceURL := strings.TrimSpace(candidate.Template.Summary.SourceURL); sourceURL != "" {
		return sourceURL
	}
	return path.Join(text.CaseFormatUpperCamel.Format(g.componentName(), text.CaseFormatLowerUnderscore), strings.ToLower(candidate.Name)+"_summary.sql")
}

func (g *ComponentCodegen) resolveOutputDestFileName(defaultName string) string {
	if spec := g.typeSpec("output"); spec != nil {
		if dest := strings.TrimSpace(spec.Dest); dest != "" {
			return dest
		}
	}
	if g.Component != nil && g.Component.Directives != nil {
		if dest := strings.TrimSpace(g.Component.Directives.OutputDest); dest != "" {
			return dest
		}
	}
	return defaultName
}

func (g *ComponentCodegen) resolveInputDestFileName(defaultName string) string {
	if spec := g.typeSpec("input"); spec != nil {
		if dest := strings.TrimSpace(spec.Dest); dest != "" {
			return dest
		}
	}
	if g.Component != nil && g.Component.Directives != nil {
		if dest := strings.TrimSpace(g.Component.Directives.InputDest); dest != "" {
			return dest
		}
	}
	return defaultName
}

func (g *ComponentCodegen) resolveViewDestFileName(defaultName string) string {
	if root := strings.TrimSpace(g.Component.RootView); root != "" {
		if spec := g.typeSpec("view:" + root); spec != nil {
			if dest := strings.TrimSpace(spec.Dest); dest != "" {
				return dest
			}
		}
	}
	if g.Component != nil && g.Component.TypeSpecs != nil {
		for _, spec := range g.Component.TypeSpecs {
			if spec == nil || spec.Role != shapeload.TypeRoleView {
				continue
			}
			if dest := strings.TrimSpace(spec.Dest); dest != "" {
				return dest
			}
		}
	}
	if g.Component != nil && g.Component.Directives != nil {
		if dest := strings.TrimSpace(g.Component.Directives.Dest); dest != "" {
			return dest
		}
	}
	return defaultName
}

func (g *ComponentCodegen) resolveRouterDestFileName(defaultName string) string {
	if g.Component != nil && g.Component.Directives != nil {
		if dest := strings.TrimSpace(g.Component.Directives.RouterDest); dest != "" {
			return dest
		}
	}
	return defaultName
}

func (g *ComponentCodegen) writeSectionFile(dest, packageName string, imports []string, sections ...string) error {
	var builder strings.Builder
	builder.WriteString("package " + packageName + "\n\n")
	if len(imports) > 0 {
		builder.WriteString("import (\n")
		for _, imp := range imports {
			imp = strings.TrimSpace(imp)
			if imp == "" {
				continue
			}
			if strings.Contains(imp, " ") {
				builder.WriteString("\t" + imp + "\n")
			} else {
				builder.WriteString("\t\"" + imp + "\"\n")
			}
		}
		builder.WriteString(")\n\n")
	}
	builder.WriteString("// Code generated by datly transcribe. DO NOT EDIT.\n\n")
	for _, section := range sections {
		if strings.TrimSpace(section) == "" {
			continue
		}
		builder.WriteString(section)
		if !strings.HasSuffix(section, "\n\n") {
			builder.WriteString("\n")
		}
	}
	return writeAtomic(dest, []byte(dedupeGeneratedStructFields(builder.String())), 0o644)
}

func (g *ComponentCodegen) appendSectionFile(dest string, sections ...string) error {
	data, err := os.ReadFile(dest)
	if err != nil {
		return err
	}
	var builder strings.Builder
	builder.Write(data)
	if len(data) > 0 && !strings.HasSuffix(string(data), "\n") {
		builder.WriteString("\n")
	}
	for _, section := range sections {
		if strings.TrimSpace(section) == "" {
			continue
		}
		builder.WriteString("\n")
		builder.WriteString(section)
		if !strings.HasSuffix(section, "\n") {
			builder.WriteString("\n")
		}
	}
	return writeAtomic(dest, []byte(dedupeGeneratedStructFields(builder.String())), 0o644)
}

func dedupeGeneratedStructFields(source string) string {
	lines := strings.Split(source, "\n")
	var result []string
	inStruct := false
	fieldNames := map[string]bool{}
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "type ") && strings.HasSuffix(trimmed, "struct {") {
			inStruct = true
			fieldNames = map[string]bool{}
			result = append(result, line)
			continue
		}
		if inStruct {
			if trimmed == "}" {
				inStruct = false
				fieldNames = nil
				result = append(result, line)
				continue
			}
			if name := generatedFieldName(line); name != "" {
				if fieldNames[name] {
					continue
				}
				fieldNames[name] = true
			}
		}
		result = append(result, line)
	}
	return strings.Join(result, "\n")
}

func generatedFieldName(line string) string {
	trimmed := strings.TrimLeft(line, "\t ")
	if trimmed == "" {
		return ""
	}
	r, size := utf8.DecodeRuneInString(trimmed)
	if r == utf8.RuneError || !unicode.IsUpper(r) {
		return ""
	}
	rest := trimmed[size:]
	var b strings.Builder
	b.WriteRune(r)
	for _, rr := range rest {
		if rr == ' ' || rr == '\t' {
			break
		}
		if unicode.IsLetter(rr) || unicode.IsDigit(rr) || rr == '_' {
			b.WriteRune(rr)
			continue
		}
		return ""
	}
	return b.String()
}

func (g *ComponentCodegen) componentName() string {
	name := ""
	if g.Component != nil {
		name = g.Component.RootView
	}
	if name == "" && g.Resource != nil && len(g.Resource.Views) > 0 {
		name = g.Resource.Views[0].Name
	}
	if name == "" {
		name = "Component"
	}
	return state.SanitizeTypeName(name)
}

type namedHelperType struct {
	TypeName string
	Decl     string
	Imports  []string
}

func collectNamedHelperTypes(rType reflect.Type, currentPackage string, skip map[string]bool) []namedHelperType {
	if rType == nil {
		return nil
	}
	skip = cloneTypeNameSet(skip)
	seen := map[string]bool{}
	importSet := map[string]bool{}
	var result []namedHelperType
	var visitType func(reflect.Type)
	var visitField func(reflect.StructField)

	visitField = func(field reflect.StructField) {
		typeName := strings.TrimSpace(field.Tag.Get("typeName"))
		baseType := unwrapAnonymousStructType(field.Type)
		if typeName != "" && baseType != nil && baseType.Name() == "" && !skip[typeName] && !seen[typeName] {
			seen[typeName] = true
			skip[typeName] = true
			imports := map[string]bool{}
			for _, imp := range collectTypeImports(baseType, currentPackage) {
				imports[imp] = true
				importSet[imp] = true
			}
			result = append(result, namedHelperType{
				TypeName: typeName,
				Decl:     fmt.Sprintf("type %s struct {\n%s}\n\n", typeName, structFieldsSource(baseType)),
				Imports:  sortedImportSet(imports),
			})
		}
		visitType(field.Type)
	}

	visitType = func(t reflect.Type) {
		if t == nil {
			return
		}
		for t.Kind() == reflect.Ptr || t.Kind() == reflect.Slice || t.Kind() == reflect.Array {
			t = t.Elem()
			if t == nil {
				return
			}
		}
		if t.Kind() == reflect.Map {
			visitType(t.Key())
			visitType(t.Elem())
			return
		}
		if t.Kind() != reflect.Struct {
			return
		}
		for i := 0; i < t.NumField(); i++ {
			visitField(t.Field(i))
		}
	}

	visitType(rType)
	return result
}

func helperImports(items []namedHelperType) []string {
	imports := map[string]bool{}
	for _, item := range items {
		for _, imp := range item.Imports {
			if strings.TrimSpace(imp) != "" {
				imports[imp] = true
			}
		}
	}
	return sortedImportSet(imports)
}

func cloneTypeNameSet(src map[string]bool) map[string]bool {
	if len(src) == 0 {
		return map[string]bool{}
	}
	ret := make(map[string]bool, len(src))
	for key, value := range src {
		ret[key] = value
	}
	return ret
}

func sortedImportSet(src map[string]bool) []string {
	if len(src) == 0 {
		return nil
	}
	ret := make([]string, 0, len(src))
	for key := range src {
		ret = append(ret, key)
	}
	sort.Strings(ret)
	return ret
}

func unwrapAnonymousStructType(rType reflect.Type) reflect.Type {
	if rType == nil {
		return nil
	}
	for rType.Kind() == reflect.Ptr || rType.Kind() == reflect.Slice || rType.Kind() == reflect.Array {
		rType = rType.Elem()
		if rType == nil {
			return nil
		}
	}
	if rType.Kind() != reflect.Struct {
		return nil
	}
	return rType
}

func (g *ComponentCodegen) outputUsesResponse(outputParams state.Parameters) bool {
	for _, p := range outputParams {
		if p == nil || p.In == nil {
			continue
		}
		if p.In.Name == "status" {
			return true
		}
	}
	return len(outputParams) > 0 || g.shouldDefaultReaderOutput()
}

func (g *ComponentCodegen) buildImports(includeRouter bool, includeResponse bool) []string {
	var imports []string
	needsReflect := g.withRegister() || g.WithContract
	if needsReflect {
		imports = append(imports,
			"reflect",
		)
	}
	if g.withRegister() {
		imports = append(imports, "github.com/viant/xdatly/types/core")
		checksumPkg := "github.com/viant/xdatly/types/custom/checksum"
		if g.PackagePath != "" {
			if idx := strings.LastIndex(g.PackagePath, "/pkg/"); idx != -1 {
				checksumPkg = g.PackagePath[:idx] + "/pkg/checksum"
			}
		}
		checksumParent, _ := path.Split(checksumPkg)
		if !strings.HasSuffix(strings.TrimSuffix(checksumParent, "/"), "dependency") {
			checksumPkg = path.Join(checksumParent, "dependency", "checksum")
		}
		imports = append(imports, checksumPkg)
	}
	if includeResponse {
		imports = append(imports, "github.com/viant/xdatly/handler/response")
	}
	if g.WithEmbed {
		imports = append(imports, "embed")
	}
	if g.WithContract {
		imports = append(imports,
			"fmt",
			"context",
			"github.com/viant/datly/view",
			"github.com/viant/datly/repository",
			"github.com/viant/datly/repository/contract",
			"github.com/viant/datly",
		)
	}
	if includeRouter {
		imports = append(imports, "github.com/viant/xdatly")
	}
	return imports
}

func (g *ComponentCodegen) buildRouterImports() []string {
	return []string{"github.com/viant/xdatly"}
}

func (g *ComponentCodegen) renderComponentHolder(builder *strings.Builder, componentName, inputTypeName, outputTypeName string, selectorHolders []codegenSelectorHolder) {
	method := strings.TrimSpace(g.Component.Method)
	if method == "" {
		method = "GET"
	}
	uri := strings.TrimSpace(g.Component.URI)
	if uri == "" {
		uri = "/"
	}
	tag := fmt.Sprintf(`component:",path=%s,method=%s`, uri, method)
	if connectorRef := strings.TrimSpace(g.rootConnectorRef()); connectorRef != "" {
		tag += fmt.Sprintf(`,connector=%s`, connectorRef)
	}
	if marshaller := strings.TrimSpace(g.rootMarshaller()); marshaller != "" {
		tag += fmt.Sprintf(`,marshaller=%s`, marshaller)
	}
	if handlerRef := strings.TrimSpace(g.rootHandlerRef()); handlerRef != "" {
		tag += fmt.Sprintf(`,handler=%s`, handlerRef)
	}
	if viewTypeName := strings.TrimSpace(g.rootViewTypeName(componentName)); viewTypeName != "" {
		tag += fmt.Sprintf(`,view=%s`, viewTypeName)
	}
	if sourceURL := strings.TrimSpace(g.rootViewSourceURL()); sourceURL != "" {
		tag += fmt.Sprintf(`,source=%s`, sourceURL)
	}
	if summaryURL := strings.TrimSpace(g.rootSummarySourceURL()); summaryURL != "" {
		tag += fmt.Sprintf(`,summary=%s`, summaryURL)
	}
	if reportTag := g.reportComponentTag(); reportTag != "" {
		tag += reportTag
	}
	tag += `"`
	builder.WriteString(fmt.Sprintf("type %sRouter struct {\n", componentName))
	builder.WriteString(fmt.Sprintf("\t%s xdatly.Component[%s, %s] `%s`\n", componentName, inputTypeName, outputTypeName, tag))
	for _, holder := range selectorHolders {
		if holder.Type == nil || strings.TrimSpace(holder.QuerySelector) == "" || strings.TrimSpace(holder.FieldName) == "" {
			continue
		}
		builder.WriteString(fmt.Sprintf("\t%s struct {\n", holder.FieldName))
		builder.WriteString(indentSource(structFieldsSource(holder.Type), "\t\t"))
		builder.WriteString(fmt.Sprintf("\t} `querySelector:%q`\n", holder.QuerySelector))
	}
	builder.WriteString("}\n\n")
}

func (g *ComponentCodegen) renderDefineComponent(builder *strings.Builder, componentName, inputTypeName, outputTypeName string) {
	method := strings.TrimSpace(g.Component.Method)
	if method == "" {
		method = "GET"
	}
	uri := strings.TrimSpace(g.Component.URI)
	if uri == "" {
		uri = "/"
	}
	connectorRef := strings.TrimSpace(g.rootConnectorRef())
	pathVar := componentName + "PathURI"
	builder.WriteString(fmt.Sprintf("var %s = %q\n\n", pathVar, uri))
	builder.WriteString(fmt.Sprintf("func Define%sComponent(ctx context.Context, srv *datly.Service) error {\n", componentName))
	builder.WriteString("\taComponent, err := repository.NewComponent(\n")
	builder.WriteString(fmt.Sprintf("\t\tcontract.NewPath(%q, %s),\n", method, pathVar))
	builder.WriteString("\t\trepository.WithResource(srv.Resource()),\n")
	builder.WriteString("\t\trepository.WithContract(\n")
	if g.WithEmbed {
		builder.WriteString(fmt.Sprintf("\t\t\treflect.TypeOf(%s{}),\n", inputTypeName))
		builder.WriteString(fmt.Sprintf("\t\t\treflect.TypeOf(%s{}), &%sFS", outputTypeName, componentName))
	} else {
		builder.WriteString(fmt.Sprintf("\t\t\treflect.TypeOf(%s{}),\n", inputTypeName))
		builder.WriteString(fmt.Sprintf("\t\t\treflect.TypeOf(%s{}), nil", outputTypeName))
	}
	if connectorRef != "" {
		builder.WriteString(fmt.Sprintf(`, view.WithConnectorRef(%q)`, connectorRef))
	}
	builder.WriteString(")")
	if reportOption := g.reportComponentOption(); reportOption != "" {
		builder.WriteString(",\n")
		builder.WriteString("\t\t")
		builder.WriteString(reportOption)
	}
	builder.WriteString(")\n\n")
	builder.WriteString("\tif err != nil {\n")
	builder.WriteString(fmt.Sprintf("\t\treturn fmt.Errorf(\"failed to create %s component: %%w\", err)\n", componentName))
	builder.WriteString("\t}\n")
	builder.WriteString("\tif err := srv.AddComponent(ctx, aComponent); err != nil {\n")
	builder.WriteString(fmt.Sprintf("\t\treturn fmt.Errorf(\"failed to add %s component: %%w\", err)\n", componentName))
	builder.WriteString("\t}\n")
	builder.WriteString("\treturn nil\n")
	builder.WriteString("}\n\n")
}

func (g *ComponentCodegen) reportComponentTag() string {
	if g.Component == nil || g.Component.Report == nil || !g.Component.Report.Enabled {
		return ""
	}
	report := g.Component.Report
	tag := ",report=true"
	if value := strings.TrimSpace(report.Input); value != "" {
		tag += fmt.Sprintf(",reportInput=%s", value)
	}
	if value := strings.TrimSpace(report.Dimensions); value != "" {
		tag += fmt.Sprintf(",reportDimensions=%s", value)
	}
	if value := strings.TrimSpace(report.Measures); value != "" {
		tag += fmt.Sprintf(",reportMeasures=%s", value)
	}
	if value := strings.TrimSpace(report.Filters); value != "" {
		tag += fmt.Sprintf(",reportFilters=%s", value)
	}
	if value := strings.TrimSpace(report.OrderBy); value != "" {
		tag += fmt.Sprintf(",reportOrderBy=%s", value)
	}
	if value := strings.TrimSpace(report.Limit); value != "" {
		tag += fmt.Sprintf(",reportLimit=%s", value)
	}
	if value := strings.TrimSpace(report.Offset); value != "" {
		tag += fmt.Sprintf(",reportOffset=%s", value)
	}
	return tag
}

func (g *ComponentCodegen) reportComponentOption() string {
	if g.Component == nil || g.Component.Report == nil || !g.Component.Report.Enabled {
		return ""
	}
	report := g.Component.Report
	parts := []string{"Enabled: true"}
	if value := strings.TrimSpace(report.Input); value != "" {
		parts = append(parts, fmt.Sprintf("Input: %q", value))
	}
	if value := strings.TrimSpace(report.Dimensions); value != "" {
		parts = append(parts, fmt.Sprintf("Dimensions: %q", value))
	}
	if value := strings.TrimSpace(report.Measures); value != "" {
		parts = append(parts, fmt.Sprintf("Measures: %q", value))
	}
	if value := strings.TrimSpace(report.Filters); value != "" {
		parts = append(parts, fmt.Sprintf("Filters: %q", value))
	}
	if value := strings.TrimSpace(report.OrderBy); value != "" {
		parts = append(parts, fmt.Sprintf("OrderBy: %q", value))
	}
	if value := strings.TrimSpace(report.Limit); value != "" {
		parts = append(parts, fmt.Sprintf("Limit: %q", value))
	}
	if value := strings.TrimSpace(report.Offset); value != "" {
		parts = append(parts, fmt.Sprintf("Offset: %q", value))
	}
	return fmt.Sprintf("repository.WithReport(&repository.Report{%s})", strings.Join(parts, ", "))
}

func (g *ComponentCodegen) rootConnectorRef() string {
	if g.Resource == nil {
		return ""
	}
	root := strings.TrimSpace(g.Component.RootView)
	for _, aView := range g.Resource.Views {
		if aView == nil || aView.Connector == nil {
			continue
		}
		if root != "" && aView.Name == root {
			if aView.Connector.Ref != "" {
				return aView.Connector.Ref
			}
			return aView.Connector.Name
		}
	}
	for _, aView := range g.Resource.Views {
		if aView != nil && aView.Connector != nil {
			if aView.Connector.Ref != "" {
				return aView.Connector.Ref
			}
			return aView.Connector.Name
		}
	}
	return ""
}

func (g *ComponentCodegen) rootMarshaller() string {
	if g == nil || g.Component == nil {
		return ""
	}
	for _, route := range g.Component.ComponentRoutes {
		if route == nil {
			continue
		}
		if marshaller := strings.TrimSpace(route.Marshaller); marshaller != "" {
			return marshaller
		}
	}
	return ""
}

func (g *ComponentCodegen) rootHandlerRef() string {
	if g == nil || g.Component == nil {
		return ""
	}
	for _, route := range g.Component.ComponentRoutes {
		if route == nil {
			continue
		}
		if handler := strings.TrimSpace(route.Handler); handler != "" {
			return handler
		}
	}
	return ""
}

func structFieldsSource(rType reflect.Type) string {
	if rType == nil {
		return ""
	}
	for rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}
	if rType.Kind() != reflect.Struct {
		return ""
	}
	var b strings.Builder
	for i := 0; i < rType.NumField(); i++ {
		f := rType.Field(i)
		if !f.IsExported() {
			continue
		}
		typeExpr := sourceFieldTypeExpr(f)
		b.WriteString("\t" + f.Name + " " + typeExpr)
		if f.Tag != "" {
			b.WriteString(" `" + string(f.Tag) + "`")
		}
		b.WriteString("\n")
	}
	return b.String()
}

func indentSource(source, prefix string) string {
	source = strings.TrimRight(source, "\n")
	if source == "" {
		return ""
	}
	lines := strings.Split(source, "\n")
	for i, line := range lines {
		lines[i] = prefix + line
	}
	return strings.Join(lines, "\n") + "\n"
}

func sourceFieldTypeExpr(field reflect.StructField) string {
	typeName := strings.TrimSpace(field.Tag.Get("typeName"))
	if typeName == "" {
		return field.Type.String()
	}
	return rewriteFieldTypeExpr(field.Type, typeName)
}

func rewriteFieldTypeExpr(rType reflect.Type, typeName string) string {
	if rType == nil {
		return typeName
	}
	switch rType.Kind() {
	case reflect.Ptr:
		return "*" + rewriteFieldTypeExpr(rType.Elem(), typeName)
	case reflect.Slice:
		return "[]" + rewriteFieldTypeExpr(rType.Elem(), typeName)
	case reflect.Array:
		return fmt.Sprintf("[%d]%s", rType.Len(), rewriteFieldTypeExpr(rType.Elem(), typeName))
	case reflect.Map:
		return "map[" + rType.Key().String() + "]" + rewriteFieldTypeExpr(rType.Elem(), typeName)
	case reflect.Struct:
		if rType.Name() == "" {
			return typeName
		}
	}
	return rType.String()
}

func resourceToCodegenDoc(resource *view.Resource, typeCtx *typectx.Context) *shape.Document {
	root := map[string]any{}
	var views []any
	for _, v := range resource.Views {
		if v == nil {
			continue
		}
		viewMap := map[string]any{
			"Name":  v.Name,
			"Table": v.Table,
			"Mode":  string(v.Mode),
		}
		if v.Schema != nil {
			schema := map[string]any{}
			if v.Schema.Name != "" {
				schema["Name"] = v.Schema.Name
			}
			viewMap["Schema"] = schema
		}
		if len(v.Columns) > 0 {
			var cols []any
			for _, c := range v.Columns {
				if c == nil {
					continue
				}
				cols = append(cols, map[string]any{
					"Name":     c.Name,
					"DataType": c.DataType,
					"Nullable": c.Nullable,
				})
			}
			viewMap["Columns"] = cols
		}
		views = append(views, viewMap)
	}
	root["Resource"] = map[string]any{"Views": views}
	return &shape.Document{Root: root, TypeContext: typeCtx}
}
