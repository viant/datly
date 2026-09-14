package velty

import (
	"fmt"
	"strconv"
	"strings"
)

type renderer struct {
	strings.Builder
}

func render(program block) (string, error) {
	output := &renderer{}
	if err := program.render(output, 0); err != nil {
		return "", err
	}
	return strings.TrimSpace(output.String()), nil
}

func (s selector) render(output *renderer) error {
	if strings.TrimSpace(string(s)) == "" {
		return fmt.Errorf("empty Velty selector")
	}
	output.WriteByte('$')
	output.WriteString(string(s))
	return nil
}

func (s stringLiteral) render(output *renderer) error {
	output.WriteString(strconv.Quote(string(s)))
	return nil
}

func (b booleanLiteral) render(output *renderer) error {
	output.WriteString(strconv.FormatBool(bool(b)))
	return nil
}

func (c call) render(output *renderer) error {
	if c.receiver == nil || strings.TrimSpace(c.method) == "" {
		return fmt.Errorf("Velty call receiver and method are required")
	}
	if err := c.receiver.render(output); err != nil {
		return err
	}
	output.WriteByte('.')
	output.WriteString(c.method)
	output.WriteByte('(')
	for index, argument := range c.args {
		if argument == nil {
			return fmt.Errorf("Velty call %s argument %d is nil", c.method, index)
		}
		if index > 0 {
			output.WriteString(", ")
		}
		if err := argument.render(output); err != nil {
			return err
		}
	}
	output.WriteByte(')')
	return nil
}

func (b binary) render(output *renderer) error {
	if b.left == nil || b.right == nil || (b.op != "==" && b.op != "!=") {
		return fmt.Errorf("invalid Velty binary expression")
	}
	if err := b.left.render(output); err != nil {
		return err
	}
	output.WriteByte(' ')
	output.WriteString(b.op)
	output.WriteByte(' ')
	return b.right.render(output)
}

func (s callStatement) render(output *renderer, indent int) error {
	output.indent(indent)
	if err := s.call.render(output); err != nil {
		return err
	}
	if s.terminated {
		output.WriteByte(';')
	}
	output.WriteByte('\n')
	return nil
}

func (s assignment) render(output *renderer, indent int) error {
	if s.target == nil || s.value == nil {
		return fmt.Errorf("Velty assignment target and value are required")
	}
	output.indent(indent)
	output.WriteString("#set(")
	if err := s.target.render(output); err != nil {
		return err
	}
	output.WriteString(" = ")
	if err := s.value.render(output); err != nil {
		return err
	}
	output.WriteString(")\n")
	return nil
}

func (s forEach) render(output *renderer, indent int) error {
	if strings.TrimSpace(s.item) == "" || s.set == nil {
		return fmt.Errorf("Velty foreach item and set are required")
	}
	output.indent(indent)
	output.WriteString("#foreach($")
	output.WriteString(s.item)
	output.WriteString(" in ")
	if err := s.set.render(output); err != nil {
		return err
	}
	output.WriteString(")\n")
	if err := s.body.render(output, indent+1); err != nil {
		return err
	}
	output.indent(indent)
	output.WriteString("#end\n")
	return nil
}

func (i conditional) render(output *renderer, indent int) error {
	if i.condition == nil {
		return fmt.Errorf("Velty condition is required")
	}
	output.indent(indent)
	output.WriteString("#if(")
	if err := i.condition.render(output); err != nil {
		return err
	}
	if i.compareTrue {
		output.WriteString(" == true")
	}
	output.WriteString(")\n")
	if err := i.thenBlock.render(output, indent+1); err != nil {
		return err
	}
	if i.elseBlock != nil {
		output.indent(indent)
		output.WriteString("#else\n")
		if err := i.elseBlock.render(output, indent+1); err != nil {
			return err
		}
	}
	output.indent(indent)
	output.WriteString("#end\n")
	return nil
}

func (b block) render(output *renderer, indent int) error {
	for _, item := range b.statements {
		if item == nil {
			return fmt.Errorf("nil Velty statement")
		}
		if err := item.render(output, indent); err != nil {
			return err
		}
	}
	return nil
}

func (r *renderer) indent(depth int) {
	for index := 0; index < depth; index++ {
		r.WriteString("  ")
	}
}
