package sql

import (
	"fmt"
)

type Binary struct {
	X        Node //left operand
	Operator string
	Y        Node // right operand

}

func (b *Binary) Validate(allowed map[string]int) error {

	err := validate(b, allowed)
	if err != nil {
		return err
	}

	return nil
}

func validate(b *Binary, allowed map[string]int) error {
	switch x := b.X.(type) {
	case *Binary:
		return x.Validate(allowed)
	case *Selector:
		if len(allowed) > 0 {
			kind, ok := allowed[x.Name]
			if !ok {
				return fmt.Errorf("column %v can not be used in criteria", x.Name)
			}

			switch y := b.Y.(type) {
			case *Literal:
				if y.Kind != kind {
					return fmt.Errorf("invalid data type for %v", x.Name)
				}
			case *Selector:
				_, ok := allowed[x.Name]
				if !ok {
					return fmt.Errorf("column %v can not be used in criteria", x.Name)
				}
			default:
				return fmt.Errorf("unsupported tokn %T with column %v", y, x.Name)
			}
		}

	case *Literal:
		if len(allowed) > 0 {
			switch y := b.Y.(type) {
			case *Literal:
				return fmt.Errorf("using literal %v literal abuses criteria", b.Operator)
			case *Selector:
				_, ok := allowed[y.Name]
				if !ok {
					return fmt.Errorf("column %v can not be used in criteria", y.Name)
				}
			default:
				return fmt.Errorf("unsupported tokn %T with literal %v", y, x.Value)
			}
		}
		return x.Validate(allowed)
	case *Parentheses:
		return x.Validate(allowed)
	case *Unary:
		return x.Validate(allowed)
	}
	return nil
}
