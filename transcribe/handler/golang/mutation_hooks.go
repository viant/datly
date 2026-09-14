package golang

import (
	"fmt"
	"go/ast"
	"go/token"
	"strconv"
	"strings"

	"github.com/viant/datly/spec"
	plan "github.com/viant/datly/transcribe/handler/ast"
)

// MutationHookRole names a typed target-language frame, not a second policy tree.
type MutationHookRole struct {
	Identity         string
	Path             plan.FieldPath
	FrameType, Field string
}

// MutationHookAsset is a partial generated product. Its declarations/imports
// compose into the existing handler artifact; it does not implement Program.
type MutationHookAsset struct {
	File                 *ast.File
	TypeName, FramesType string
	Roles                []MutationHookRole
}

// MutationHookSupport lowers only dependency preparation and typed Init/Validate
// calls. The surrounding Program owns matching/current projection and populates
// frames with distinct DB Previous/PreviousFields and captured OriginalPresence.
func MutationHookSupport(value *plan.Plan, config Config, supplied ...*MutationFrameLayout) (*MutationHookAsset, error) {
	if strings.TrimSpace(config.PackagePath) == "" {
		return nil, fmt.Errorf("mutation hook support requires canonical target PackagePath")
	}
	l := &lowerer{plan: value, config: config}
	if err := l.prepare(); err != nil {
		return nil, err
	}
	if len(supplied) > 1 {
		return nil, fmt.Errorf("mutation hook support accepts one frame layout")
	}
	var layout *MutationFrameLayout
	emitFrames := len(supplied) == 0
	if emitFrames {
		var err error
		layout, err = l.mutationFrames()
		if err != nil {
			return nil, err
		}
	} else {
		layout = supplied[0]
		if layout == nil || layout.File == nil || layout.TypeName == "" {
			return nil, fmt.Errorf("complete mutation frame layout is required")
		}
	}
	prefix := "_" + lowerInitial(l.factory)
	e := &mutationHookEmitter{l: l, layout: layout, emitFrames: emitFrames, asset: &MutationHookAsset{TypeName: prefix + "MutationHooks", FramesType: layout.TypeName}}
	names := []string{e.asset.TypeName}
	if !emitFrames {
		names = append(names, e.asset.FramesType)
		for _, role := range layout.Roles {
			names = append(names, role.FrameType)
		}
	}
	for _, name := range names {
		if err := l.reserveDeclaration(name); err != nil {
			return nil, err
		}
	}
	e.receiver = l.availableAlias("hooks")
	e.context = l.availableAlias("ctx")
	e.binder = l.availableAlias("binder")
	e.frames = l.availableAlias("frames")
	e.frame = l.availableAlias("frame")
	l.pathsByAlias[l.fmtAlias] = "fmt"
	if err := e.collect(value.Root, nil); err != nil {
		return nil, err
	}
	if len(e.roles) == 0 {
		return nil, nil
	}
	file, err := e.file()
	if err != nil {
		return nil, err
	}
	e.asset.File = file
	return e.asset, nil
}

type mutationHookRole struct {
	record, parent              *recordLowering
	hook                        ast.Expr
	frameType, field, hookField string
	bind                        bool
}
type mutationHookEmitter struct {
	l                                        *lowerer
	asset                                    *MutationHookAsset
	roles                                    []mutationHookRole
	layout                                   *MutationFrameLayout
	emitFrames                               bool
	receiver, context, binder, frames, frame string
}

func (e *mutationHookEmitter) collect(record *plan.RecordPlan, parent *recordLowering) error {
	if record == nil || record.Auxiliary {
		return nil
	}
	actual := e.l.recordByPlan[record]
	if record.Entity != nil && !record.Entity.Hooks.IsZero() {
		hook, err := e.hookType(record.Entity.Hooks)
		if err != nil {
			return err
		}
		index := strconv.Itoa(actual.order)
		frame, err := e.layout.role(record)
		if err != nil {
			return err
		}
		role := mutationHookRole{record: actual, parent: parent, hook: hook, frameType: frame.FrameType, field: frame.Field, hookField: "hook" + index, bind: record.Entity.HooksBind}
		e.roles = append(e.roles, role)
		e.asset.Roles = append(e.asset.Roles, MutationHookRole{Identity: record.Identity, Path: append(plan.FieldPath(nil), record.InputPath...), FrameType: role.frameType, Field: role.field})
	}
	for _, relation := range record.Relations {
		if relation != nil {
			if err := e.collect(relation.Child, actual); err != nil {
				return err
			}
		}
	}
	return nil
}

func (e *mutationHookEmitter) hookType(ref spec.TypeRef) (ast.Expr, error) {
	if ref.Pointer || ref.Cardinality != "" {
		return nil, fmt.Errorf("hook type must be a concrete unwrapped type")
	}
	return e.l.typeReference(ref)
}

func (e *mutationHookEmitter) file() (*ast.File, error) {
	file := &ast.File{Name: ast.NewIdent(e.l.config.Package)}
	hookFields := []*ast.Field{namedField("prepareAttempted", ast.NewIdent("bool")), namedField("prepared", ast.NewIdent("bool")), namedField("initAttempted", ast.NewIdent("bool")), namedField("initialized", ast.NewIdent("bool")), namedField("validateAttempted", ast.NewIdent("bool"))}
	for _, name := range []string{"validated", "afterSequenceAttempted", "afterSequenceComplete", "afterQueueAttempted", "finalizeAttempted"} {
		hookFields = append(hookFields, namedField(name, ast.NewIdent("bool")))
	}
	if e.emitFrames {
		for _, declaration := range e.layout.File.Decls {
			if group, ok := declaration.(*ast.GenDecl); ok && group.Tok == token.IMPORT {
				continue
			}
			file.Decls = append(file.Decls, declaration)
		}
	}
	for _, role := range e.roles {
		if err := e.l.markExpressionImports(role.record.value.base); err != nil {
			return nil, err
		}
		if role.parent != nil {
			if err := e.l.markExpressionImports(role.parent.value.base); err != nil {
				return nil, err
			}
		}
		hookFields = append(hookFields, namedField(role.hookField, &ast.StarExpr{X: role.hook}))
	}
	file.Decls = append(file.Decls, &ast.GenDecl{Tok: token.TYPE, Specs: []ast.Spec{&ast.TypeSpec{Name: ast.NewIdent(e.asset.TypeName), Type: &ast.StructType{Fields: &ast.FieldList{List: hookFields}}}}})
	file.Decls = append(file.Decls, e.prepare(), e.phase("Init"), e.phase("Validate"))
	file.Decls = append(file.Decls, e.optionalPhase("AfterSequence", "AfterSequenceHook", "validated", "afterSequenceAttempted", "afterSequenceComplete"), e.optionalPhase("AfterQueue", "AfterQueueHook", "afterSequenceComplete", "afterQueueAttempted", ""))
	file.Decls = append(file.Decls, e.finalize())
	file.Decls = append([]ast.Decl{(&entityEmitter{l: e.l}).importDeclaration(file)}, file.Decls...)
	return file, nil
}
