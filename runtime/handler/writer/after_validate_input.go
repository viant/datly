package writer

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/viant/datly/runtime/handler/engine"
	xhandler "github.com/viant/xdatly/handler"
)

func hasAfterValidateInput(root *Record) bool {
	if root == nil || root.HookType == nil {
		return false
	}
	_, ok := reflect.PointerTo(root.HookType).MethodByName("AfterValidateInput")
	return ok
}

func validateAfterValidateInputHooks(root *Record, inputType, outputType reflect.Type) error {
	seen := map[*Record]bool{}
	var visit func(*Record) error
	visit = func(record *Record) error {
		if record == nil || seen[record] {
			return nil
		}
		seen[record] = true
		if record.HookType != nil {
			method := reflect.New(record.HookType).MethodByName("AfterValidateInput")
			if method.IsValid() {
				if record != root || root.Auxiliary && len(root.Relations) == 0 {
					return fmt.Errorf("AfterValidateInput is only supported on the writer root")
				}
				typ := method.Type()
				if typ.IsVariadic() || typ.NumIn() != 3 || typ.NumOut() != 1 || typ.In(0) != reflect.TypeFor[context.Context]() || typ.In(1) != reflect.PointerTo(inputType) || typ.In(2) != reflect.PointerTo(outputType) || typ.Out(0) != reflect.TypeFor[error]() {
					return fmt.Errorf("AfterValidateInput requires canonical context.Context, *Input, *Output and error")
				}
			}
		}
		for _, relation := range record.Relations {
			if hasAfterValidateInput(root) && len(relation.Field) != 1 {
				return fmt.Errorf("AfterValidateInput requires direct canonical relation holders")
			}
			if err := visit(relation.Child); err != nil {
				return err
			}
		}
		return nil
	}
	return visit(root)
}

// A preparation snapshot retains native frames rather than recapturing client
// facts. Every relation may become an ordered subsequence of its original
// occurrences; scalar state and the root occurrence list remain fixed.
type preparationNode struct {
	entity   reflect.Value
	frame    *Frame
	scalar   string
	children [][]*preparationNode
}

func preparationRows(rows reflect.Value) []reflect.Value {
	if rows.Kind() == reflect.Pointer {
		if rows.IsNil() {
			return nil
		}
		return []reflect.Value{reflect.ValueOf(rows.Interface())}
	}
	result := make([]reflect.Value, rows.Len())
	for i := range result {
		result[i] = reflect.ValueOf(rows.Index(i).Interface())
	}
	return result
}

func preparationScalar(record *Record, entity reflect.Value) (string, error) {
	if entity.IsNil() {
		return "nil", nil
	}
	var values []reflect.Value
	for i := 0; i < entity.Elem().NumField(); i++ {
		relation := false
		for _, edge := range record.Relations {
			if len(edge.Field) == 1 && edge.Field[0] == i {
				relation = true
				break
			}
		}
		if !relation {
			values = append(values, entity.Elem().Field(i))
		}
	}
	return immutableValues(values)
}

func (p *Program) preparationEvidence() (string, error) {
	input := reflect.ValueOf(p.input).Elem()
	var values []reflect.Value
	for i := 0; i < input.NumField(); i++ {
		if i != p.metadata.InputField {
			values = append(values, input.Field(i))
		}
	}
	seen := map[*Record]bool{}
	var visit func(*Record)
	visit = func(record *Record) {
		if record == nil || seen[record] {
			return
		}
		seen[record] = true
		if record.CurrentField >= 0 {
			values = append(values, input.Field(record.CurrentField))
		}
		values = append(values, reflect.ValueOf(p.previousFields[record]))
		for _, edge := range record.Relations {
			visit(edge.Child)
		}
	}
	visit(p.metadata.Root)
	for _, frame := range p.frames.Rows {
		values = append(values, frame.Previous, frame.ExpectedToken, reflect.ValueOf(frame.Action))
		if frame.Previous.IsValid() && !frame.Previous.IsNil() {
			values = append(values, reflect.ValueOf(p.typeFields[frame.Previous.Elem().Type()]))
		}
		if frame.Original != nil {
			values = append(values, reflect.ValueOf(frame.Original.Available()))
			for _, field := range frame.Record.Fields {
				values = append(values, reflect.ValueOf(frame.Original.Has(field.Name)))
			}
		}
	}
	return immutableValues(values)
}

func (p *Program) snapshotPreparation() ([]*preparationNode, error) {
	frames := map[frameIdentity]*Frame{}
	for _, frame := range p.frames.Rows {
		// Slice compaction must never rebind a retained frame to a moved slot.
		frame.Entity = reflect.ValueOf(frame.Entity.Interface())
		key := identityOfFrame(frame)
		if frames[key] != nil {
			return nil, fmt.Errorf("AfterValidateInput cannot filter ambiguous duplicate associations")
		}
		frames[key] = frame
	}
	var capture func(*Record, reflect.Value) ([]*preparationNode, error)
	capture = func(record *Record, rows reflect.Value) ([]*preparationNode, error) {
		var nodes []*preparationNode
		for _, entity := range preparationRows(rows) {
			scalar, err := preparationScalar(record, entity)
			if err != nil {
				return nil, err
			}
			node := &preparationNode{entity: entity, scalar: scalar}
			if !entity.IsNil() {
				node.frame = frames[frameIdentity{record: record, pointer: entity.Pointer()}]
				if node.frame == nil {
					return nil, fmt.Errorf("AfterValidateInput found an unframed occurrence")
				}
				for _, edge := range record.Relations {
					children, err := capture(edge.Child, entity.Elem().FieldByIndex(edge.Field))
					if err != nil {
						return nil, err
					}
					node.children = append(node.children, children)
				}
			}
			nodes = append(nodes, node)
		}
		return nodes, nil
	}
	return capture(p.metadata.Root, reflect.ValueOf(p.input).Elem().Field(p.metadata.InputField))
}

// Verify the complete graph before changing any native bookkeeping. Retained
// frames preserve Previous, Original, live presence overlays and lifecycle state.
func (p *Program) refreshPreparation(nodes []*preparationNode, apply bool) error {
	var unchanged func([]*preparationNode) error
	unchanged = func(nodes []*preparationNode) error {
		for _, node := range nodes {
			if node.frame != nil {
				scalar, err := preparationScalar(node.frame.Record, node.entity)
				if err != nil {
					return err
				}
				if scalar != node.scalar {
					return fmt.Errorf("AfterValidateInput changed identity, values or presence")
				}
			}
			for _, children := range node.children {
				if err := unchanged(children); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := unchanged(nodes); err != nil {
		return err
	}
	retained := map[*Frame]bool{}
	type location struct {
		position int
		indexed  bool
		path     string
	}
	locations := map[*Frame]location{}
	var verify func(*Record, reflect.Value, []*preparationNode, *Frame) error
	verify = func(record *Record, rows reflect.Value, before []*preparationNode, parent *Frame) error {
		after := preparationRows(rows)
		if parent == nil && len(after) != len(before) {
			return fmt.Errorf("AfterValidateInput changed root occurrences")
		}
		next := 0
		for pos, entity := range after {
			for next < len(before) && before[next].entity.Pointer() != entity.Pointer() {
				next++
			}
			if next == len(before) || parent == nil && next != pos {
				return fmt.Errorf("AfterValidateInput added, reordered or reparented an occurrence")
			}
			node := before[next]
			next++
			scalar, err := preparationScalar(record, entity)
			if err != nil {
				return err
			}
			if scalar != node.scalar {
				return fmt.Errorf("AfterValidateInput changed retained identity, values or presence")
			}
			if entity.IsNil() {
				continue
			}
			frame := node.frame
			if frame.Parent != parent {
				return fmt.Errorf("AfterValidateInput changed parent scope")
			}
			retained[frame] = true
			path := record.Path
			if parent != nil {
				path = locations[parent].path + "." + parent.Record.EntityType.FieldByIndex(relationFor(parent.Record, record).Field).Name
			} else {
				path = reflect.TypeOf(p.input).Elem().Field(p.metadata.InputField).Name
			}
			if rows.Kind() == reflect.Slice {
				path += fmt.Sprintf("[%d]", pos)
			}
			locations[frame] = location{pos, rows.Kind() == reflect.Slice, path}
			for i, edge := range record.Relations {
				if err := verify(edge.Child, entity.Elem().FieldByIndex(edge.Field), node.children[i], frame); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if err := verify(p.metadata.Root, reflect.ValueOf(p.input).Elem().Field(p.metadata.InputField), nodes, nil); err != nil {
		return err
	}
	if !apply {
		return nil
	}
	var frames []*Frame
	owners := map[frameIdentity]*Frame{}
	for _, frame := range p.frames.Rows {
		if !retained[frame] {
			continue
		}
		at := locations[frame]
		frame.holderPosition, frame.holderIndexed, frame.Location = at.position, at.indexed, at.path
		frames = append(frames, frame)
		owners[identityOfFrame(frame)] = frame
	}
	p.frames = &MutationFrames{Rows: frames}
	p.graph = nil
	for key, facts := range p.actionPolicyFacts {
		owner := owners[key]
		if owner == nil {
			delete(p.actionPolicyFacts, key)
			continue
		}
		if facts.owner != nil {
			copy := *owner
			facts.owner = &copy
		}
	}
	if err := p.validateAuxiliaryTopology(); err != nil {
		return err
	}
	if err := p.reconcileLinks(false); err != nil {
		return err
	}
	for _, frame := range p.frames.Rows {
		if err := p.applyInvariants(frame); err != nil {
			return err
		}
		if err := p.checkConcurrency(frame); err != nil {
			return err
		}
		if !frame.Record.Auxiliary && frame.Action == xhandler.WriteInsert {
			if err := validateInsertIdentity(frame.Record, frame.Entity.Elem(), frame.Parent); err != nil {
				return err
			}
		}
	}
	return p.orderFramesByReferences()
}

func (p *Program) callAfterValidateInput(ctx context.Context, binder xhandler.Binder, validator xhandler.Validator) (err error) {
	if !hasAfterValidateInput(p.metadata.Root) {
		return nil
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if p.afterValidateInputStarted {
		return fmt.Errorf("AfterValidateInput execution already attempted; fresh capture required")
	}
	if err = p.validateAuxiliaryTopology(); err != nil {
		return err
	}
	if err = p.validateActionPolicyFacts(); err != nil {
		return err
	}
	nodes, err := p.snapshotPreparation()
	if err != nil {
		return err
	}
	evidence, err := p.preparationEvidence()
	if err != nil {
		return err
	}
	if p.metadata.OutputField < 0 || p.metadata.OutputField >= reflect.ValueOf(p.output).Elem().NumField() {
		return fmt.Errorf("AfterValidateInput requires a canonical output body")
	}
	outputBody := reflect.ValueOf(p.output).Elem().Field(p.metadata.OutputField)
	outputWasNil := outputBody.IsNil()
	outputRows := preparationRows(outputBody)
	inputPointers := map[uintptr]bool{}
	for _, node := range nodes {
		inputPointers[node.entity.Pointer()] = true
	}
	var independentOutput []reflect.Value
	for _, entity := range outputRows {
		if !inputPointers[entity.Pointer()] {
			independentOutput = append(independentOutput, entity)
		}
	}
	outputEvidence, err := immutableValues(independentOutput)
	if err != nil {
		return err
	}
	verify := func() error {
		after, evidenceErr := p.preparationEvidence()
		if evidenceErr != nil {
			return evidenceErr
		}
		if after != evidence {
			return fmt.Errorf("AfterValidateInput changed Current, Previous or Original evidence")
		}
		currentOutput := preparationRows(outputBody)
		if outputBody.IsNil() != outputWasNil || len(currentOutput) != len(outputRows) {
			return fmt.Errorf("AfterValidateInput changed output body occurrences")
		}
		for i, entity := range currentOutput {
			if entity.Pointer() != outputRows[i].Pointer() {
				return fmt.Errorf("AfterValidateInput changed output body occurrences")
			}
		}
		currentOutputEvidence, outputErr := immutableValues(independentOutput)
		if outputErr != nil {
			return outputErr
		}
		if currentOutputEvidence != outputEvidence {
			return fmt.Errorf("AfterValidateInput changed independent output body")
		}
		return p.refreshPreparation(nodes, false)
	}
	finish, err := engine.BeginWriteEligibility(ctx)
	if err != nil {
		return err
	}
	p.afterValidateInputStarted = true
	func() {
		defer func() {
			// Always verify the failed attempt too. A panic continues through the
			// existing owner boundary; its graph violation stays sticky on capture.
			violation := errors.Join(finish(), verify())
			if violation != nil {
				p.guardMu.Lock()
				p.afterValidateInputViolation = violation
				p.guardMu.Unlock()
				p.retainExecutionFailure(violation)
			}
			err = errors.Join(err, ctx.Err(), violation)
		}()
		results := p.hook.MethodByName("AfterValidateInput").Call([]reflect.Value{reflect.ValueOf(ctx), reflect.ValueOf(p.input), reflect.ValueOf(p.output)})
		err = methodError("AfterValidateInput", results)
	}()
	if err != nil {
		return err
	}
	if err = p.refreshPreparation(nodes, true); err != nil {
		return err
	}
	// Collection pruning changes graph producers and validation deferral. Recheck
	// native constraints without replaying Init or application payload validation.
	return p.validateFrames(ctx, validator, false)
}

// Illegal graph/evidence/capability changes retire the attempt rather than
// becoming an automatic replay source. A successful read/filter boundary keeps
// the existing fresh-capture recovery policy.
func (p *Program) afterValidateInputWasViolated() bool {
	if p == nil {
		return false
	}
	p.guardMu.Lock()
	defer p.guardMu.Unlock()
	return p.afterValidateInputViolation != nil
}
