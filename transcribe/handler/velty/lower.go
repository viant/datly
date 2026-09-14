// Package velty lowers target-neutral handler transcription plans into Velty.
package velty

import (
	"fmt"
	"strings"
	"unicode"

	plan "github.com/viant/datly/transcribe/handler/ast"
)

// Render lowers and renders one validated semantic plan.
func Render(value *plan.Plan) (string, error) {
	program, err := lower(value)
	if err != nil {
		return "", err
	}
	return render(program)
}

func lower(value *plan.Plan) (block, error) {
	if value == nil || value.Root == nil {
		return block{}, fmt.Errorf("Velty lowering requires a root write plan")
	}
	rootInput, err := pathSelector(value.Root.InputPath)
	if err != nil {
		return block{}, err
	}
	program := block{}
	if err = appendSequences(&program, value.Root); err != nil {
		return block{}, err
	}
	if err = appendCurrentIndexes(&program, value.Operation, value.Root); err != nil {
		return block{}, err
	}
	if err = (&traversalLowerer{operation: value.Operation}).traverse(&program, recordScope{record: value.Root, source: rootInput}); err != nil {
		return block{}, err
	}
	if value.Output != nil {
		output, pathErr := outputSelector(value.Output.Path)
		if pathErr != nil {
			return block{}, pathErr
		}
		program.append(assignment{target: output, value: rootInput})
	}
	return program, nil
}

func appendSequences(program *block, record *plan.RecordPlan) error {
	if record == nil {
		return fmt.Errorf("Velty sequence lowering received a nil record plan")
	}
	if record.Auxiliary && (record.Sequence != nil || record.Write.Existing != "" || record.Write.Missing != "" || len(record.Write.Allowed) > 0) {
		return fmt.Errorf("auxiliary record %q has mutation operations", record.Identity)
	}
	if record.Sequence != nil {
		destination, err := pathSelector(record.Sequence.Destination)
		if err != nil {
			return err
		}
		selectorPath := strings.Join(record.Sequence.Selector, "/")
		if selectorPath == "" {
			return fmt.Errorf("Velty sequence selector is empty for view %q", record.Identity)
		}
		program.append(callStatement{call: call{
			receiver: selector("sequencer"), method: "Allocate",
			args: []expression{stringLiteral(record.Table), destination, stringLiteral(selectorPath)},
		}, terminated: false})
	}
	for _, relation := range record.Relations {
		if relation == nil || relation.Child == nil {
			return fmt.Errorf("Velty sequence lowering received an incomplete relation plan")
		}
		if err := appendSequences(program, relation.Child); err != nil {
			return err
		}
	}
	return nil
}

func appendCurrentIndexes(program *block, operation plan.Operation, record *plan.RecordPlan) error {
	if operation == plan.OperationPatch && (!record.Auxiliary || record.Current != nil) {
		index, err := currentIndex(record)
		if err != nil {
			return err
		}
		if len(index.currentFields) == 1 {
			program.append(assignment{
				target: index.target,
				value:  call{receiver: index.current, method: "IndexBy", args: []expression{stringLiteral(index.currentFields[0])}},
			})
		} else {
			recordPath, pathErr := inputRecordPath(record.InputPath)
			if pathErr != nil {
				return pathErr
			}
			args := []expression{stringLiteral(index.name), index.current, selector("Input"), stringLiteral(recordPath)}
			for position := range index.currentFields {
				args = append(args, stringLiteral(index.currentFields[position]), stringLiteral(index.recordFields[position]))
			}
			program.append(callStatement{call: call{receiver: selector("index"), method: "Build", args: args}})
		}
	}
	for _, relation := range record.Relations {
		if relation == nil || relation.Child == nil {
			return fmt.Errorf("Velty index lowering received an incomplete relation plan")
		}
		if err := appendCurrentIndexes(program, operation, relation.Child); err != nil {
			return err
		}
	}
	return nil
}

func lowerWrite(operation plan.Operation, record *plan.RecordPlan, value, payload expression) (statement, error) {
	switch operation {
	case plan.OperationPost:
		return dmlCall(record.Write.Missing, record.Table, payload)
	case plan.OperationPut:
		return dmlCall(record.Write.Existing, record.Table, payload)
	case plan.OperationPatch:
		index, err := currentIndex(record)
		if err != nil {
			return nil, err
		}
		existing, err := dmlCall(record.Write.Existing, record.Table, payload)
		if err != nil {
			return nil, err
		}
		missing, err := dmlCall(record.Write.Missing, record.Table, payload)
		if err != nil {
			return nil, err
		}
		elseBlock := block{statements: []statement{missing}}
		condition := expression(nil)
		if len(index.recordFields) == 1 {
			key, keyErr := selectField(value, index.recordFields[0])
			if keyErr != nil {
				return nil, keyErr
			}
			condition = call{receiver: index.target, method: "HasKey", args: []expression{key}}
		} else {
			args := []expression{stringLiteral(index.name), value}
			condition = call{receiver: selector("index"), method: "Has", args: args}
		}
		return conditional{
			condition: condition, compareTrue: true,
			thenBlock: block{statements: []statement{existing}}, elseBlock: &elseBlock,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported Velty write operation %q", operation)
	}
}

type currentIndexPlan struct {
	name          string
	target        selector
	current       selector
	recordFields  []string
	currentFields []string
}

func currentIndex(record *plan.RecordPlan) (currentIndexPlan, error) {
	if record == nil || record.Current == nil || len(record.Keys) == 0 || len(record.Keys) != len(record.Current.Keys) {
		return currentIndexPlan{}, fmt.Errorf("Velty PATCH lowering for view %q requires equal non-empty record and current keys", recordIdentity(record))
	}
	current, err := pathSelector(record.Current.InputPath)
	if err != nil {
		return currentIndexPlan{}, err
	}
	result := currentIndexPlan{current: current}
	var name strings.Builder
	name.WriteString(last(record.Current.InputPath))
	name.WriteString("By")
	for position := range record.Keys {
		recordField := strings.TrimSpace(record.Keys[position].Field)
		currentField := strings.TrimSpace(record.Current.Keys[position].Field)
		if recordField == "" || currentField == "" {
			return currentIndexPlan{}, fmt.Errorf("Velty PATCH lowering for view %q requires named key fields", recordIdentity(record))
		}
		if position > 0 {
			name.WriteString("And")
		}
		name.WriteString(exported(recordField))
		result.recordFields = append(result.recordFields, recordField)
		result.currentFields = append(result.currentFields, currentField)
	}
	result.name = name.String()
	result.target = selector(result.name)
	return result, nil
}

func dmlCall(action plan.Action, table string, value expression) (statement, error) {
	method := ""
	switch action {
	case plan.ActionInsert:
		method = "Insert"
	case plan.ActionUpdate:
		method = "Update"
	default:
		return nil, fmt.Errorf("unsupported Velty DML action %q", action)
	}
	if strings.TrimSpace(table) == "" {
		return nil, fmt.Errorf("Velty DML table is required")
	}
	return callStatement{call: call{
		receiver: selector("dml"), method: method,
		args: []expression{stringLiteral(table), value},
	}, terminated: true}, nil
}

func pathSelector(path plan.FieldPath) (selector, error) {
	if len(path) == 0 {
		return "", fmt.Errorf("Velty selector path is empty")
	}
	for _, part := range path {
		if strings.TrimSpace(part) == "" {
			return "", fmt.Errorf("Velty selector path contains an empty field")
		}
	}
	return selector(strings.Join(path, ".")), nil
}

func outputSelector(path plan.FieldPath) (selector, error) {
	if len(path) == 0 {
		return "", fmt.Errorf("Velty output path is empty")
	}
	if path[0] != "Output" {
		return "", fmt.Errorf("Velty output path must be rooted at Output, got %q", path[0])
	}
	return pathSelector(path)
}

func inputRecordPath(path plan.FieldPath) (string, error) {
	if len(path) < 2 || path[0] != "Input" {
		return "", fmt.Errorf("Velty record path must be rooted below Input")
	}
	for _, part := range path[1:] {
		if strings.TrimSpace(part) == "" {
			return "", fmt.Errorf("Velty record path contains an empty field")
		}
	}
	return strings.Join(path[1:], "/"), nil
}

func selectPath(value selector, path plan.FieldPath) (selector, error) {
	result := value
	for _, field := range path {
		var err error
		result, err = selectField(result, field)
		if err != nil {
			return "", err
		}
	}
	return result, nil
}

func selectField(value expression, field string) (selector, error) {
	base, ok := value.(selector)
	if !ok || strings.TrimSpace(field) == "" {
		return "", fmt.Errorf("Velty record selector and field are required")
	}
	return selector(string(base) + "." + field), nil
}

func recordVariable(record *plan.RecordPlan, hint string) string {
	if hint == "" {
		hint = last(record.InputPath)
	}
	return "Rec" + exported(hint)
}

func recordIdentity(record *plan.RecordPlan) string {
	if record == nil {
		return ""
	}
	return record.Identity
}

func last(path plan.FieldPath) string {
	if len(path) == 0 {
		return ""
	}
	return path[len(path)-1]
}

func exported(value string) string {
	if value == "" {
		return "Record"
	}
	runes := []rune(value)
	runes[0] = unicode.ToUpper(runes[0])
	return string(runes)
}
