package velty

import (
	"fmt"

	"github.com/viant/velty/ast"
	"github.com/viant/velty/ast/expr"
	"github.com/viant/velty/ast/stmt"
	veltyparser "github.com/viant/velty/parser"
)

type programCapabilities struct {
	differ     bool
	dml        bool
	index      bool
	sequencer  bool
	validator  bool
	messageBus bool
	logger     bool
	writeHooks bool
}

type capabilityInspector struct {
	result programCapabilities
}

func inspectProgramCapabilities(template string) (programCapabilities, error) {
	root, err := veltyparser.Parse([]byte(template))
	if err != nil {
		return programCapabilities{}, fmt.Errorf("inspect Velty capabilities: %w", err)
	}
	inspector := &capabilityInspector{}
	if err = inspector.block(root); err != nil {
		return programCapabilities{}, err
	}
	return inspector.result, nil
}

func (i *capabilityInspector) block(block *stmt.Block) error {
	if block == nil {
		return nil
	}
	for _, item := range block.Statements() {
		if err := i.statement(item); err != nil {
			return err
		}
	}
	return nil
}

func (i *capabilityInspector) statement(item ast.Statement) error {
	switch actual := item.(type) {
	case *stmt.Append, *stmt.Break, nil:
		return nil
	case *expr.Select:
		return i.expression(actual)
	case *stmt.Statement:
		if err := i.expression(actual.X); err != nil {
			return err
		}
		return i.expression(actual.Y)
	case *stmt.Evaluate:
		return i.expression(actual.X)
	case *stmt.If:
		for current := actual; current != nil; current = current.Else {
			if err := i.expression(current.Condition); err != nil {
				return err
			}
			if err := i.block(&current.Body); err != nil {
				return err
			}
		}
		return nil
	case *stmt.ForEach:
		if err := i.expression(actual.Set); err != nil {
			return err
		}
		return i.block(&actual.Body)
	case *stmt.ForLoop:
		if err := i.statement(actual.Init); err != nil {
			return err
		}
		if err := i.expression(actual.Cond); err != nil {
			return err
		}
		if err := i.block(&actual.Body); err != nil {
			return err
		}
		return i.statement(actual.Post)
	default:
		return fmt.Errorf("inspect Velty capabilities: unsupported statement %T", item)
	}
}

func (i *capabilityInspector) expression(item ast.Expression) error {
	switch actual := item.(type) {
	case *expr.Select:
		i.selector(actual.ID)
		return i.selectorTail(actual.X)
	case *expr.Binary:
		if err := i.expression(actual.X); err != nil {
			return err
		}
		return i.expression(actual.Y)
	case *expr.Unary:
		return i.expression(actual.X)
	case *expr.Parentheses:
		return i.expression(actual.P)
	case *expr.Call:
		if err := i.expression(actual.X); err != nil {
			return err
		}
		for _, arg := range actual.Args {
			if err := i.expression(arg); err != nil {
				return err
			}
		}
		return nil
	case *expr.SliceIndex:
		if err := i.expression(actual.X); err != nil {
			return err
		}
		return i.expression(actual.Y)
	case *expr.Range:
		if err := i.expression(actual.X); err != nil {
			return err
		}
		return i.expression(actual.Y)
	case *expr.Literal, nil:
		return nil
	default:
		return fmt.Errorf("inspect Velty capabilities: unsupported expression %T", item)
	}
}

// selectorTail walks operations and indexes attached to a root selector without
// treating field names in the chain as invocation capabilities.
func (i *capabilityInspector) selectorTail(item ast.Expression) error {
	switch actual := item.(type) {
	case *expr.Select:
		return i.selectorTail(actual.X)
	case *expr.Call:
		for _, arg := range actual.Args {
			if err := i.expression(arg); err != nil {
				return err
			}
		}
		return i.selectorTail(actual.X)
	case *expr.SliceIndex:
		if err := i.expression(actual.X); err != nil {
			return err
		}
		return i.selectorTail(actual.Y)
	case nil:
		return nil
	default:
		return fmt.Errorf("inspect Velty capabilities: unsupported selector tail %T", item)
	}
}

func (i *capabilityInspector) selector(name string) {
	switch name {
	case "dml":
		i.result.dml = true
	case "index":
		i.result.index = true
	case "sequencer":
		i.result.sequencer = true
	case "differ":
		i.result.differ = true
	case "validator":
		i.result.validator = true
	case "messageBus":
		i.result.messageBus = true
	case "logger":
		i.result.logger = true
	case "writeHooks":
		i.result.writeHooks = true
	}
}
