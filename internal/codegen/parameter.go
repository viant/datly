package codegen

import (
	"fmt"
	"github.com/viant/datly/view"
	"strconv"
	"strings"
)

type Parameter struct {
	view.Parameter
	SQL string
}

func (p *Parameter) DsqlParameterDeclaration() string {
	builder := strings.Builder{}
	builder.WriteString("#set($_ = $")
	builder.WriteString(p.Name)
	builder.WriteString("<")
	switch p.In.Kind {
	case view.KindParam:
		builder.WriteString("?")
	default:
		if p.Schema.Cardinality == view.Many {
			builder.WriteString("[]")
		}

		builder.WriteString("*")
		builder.WriteString(p.Schema.DataType)
	}
	builder.WriteString(">(")
	builder.WriteString(string(p.In.Kind))
	builder.WriteByte('/')
	builder.WriteString(string(p.In.Name))
	builder.WriteByte(')')

	if p.SQL != "" {
		builder.WriteString(" /*\n")
		SQL := strings.TrimSpace(p.SQL)
		p.addedValidationModifierIfNeeded(&builder, SQL)
		builder.WriteString(SQL)
		builder.WriteString("\n*/\n")
	}
	builder.WriteByte(')')

	return builder.String()
}

func (p *Parameter) FieldDeclaration() string {
	builder := strings.Builder{}
	if p.SQL != "" {
		p.buildSQLDoc(&builder)
	}

	builder.WriteByte('\t')
	builder.WriteString(p.Name)
	builder.WriteString(" ")
	if p.Schema.Cardinality == view.Many {
		builder.WriteString("[]")
	}
	builder.WriteString("*")

	paramType := p.Schema.Type()
	if paramType != nil {
		builder.WriteString(paramType.String())
	} else {
		builder.WriteString(p.Schema.DataType)
	}

	tag := fmt.Sprintf(`datly:"kind=%v,in=%v"`, p.In.Kind, p.In.Name)
	builder.WriteString("`")
	builder.WriteString(tag)
	builder.WriteString("`")
	builder.WriteString(" ")
	return builder.String()
}

func (p *Parameter) buildSQLDoc(builder *strings.Builder) {
	SQL := strings.TrimSpace(p.SQL)
	if SQL == "" {
		return
	}

	builder.WriteString("\n/*\n ")
	p.addedValidationModifierIfNeeded(builder, SQL)
	builder.WriteString(SQL)
	builder.WriteString("\n*/\n")
}

func (p *Parameter) addedValidationModifierIfNeeded(builder *strings.Builder, SQL string) {
	hasValidationModifier := false
	switch SQL[0] {
	case '!', '?':
		hasValidationModifier = true
	}
	if !hasValidationModifier {
		if p.Required != nil && *p.Required {
			builder.WriteString("!")
			if p.ErrorStatusCode > 0 {
				builder.WriteString(strconv.Itoa(p.ErrorStatusCode))
			}
			builder.WriteString(" ")
		} else {
			builder.WriteString("? ")
		}
	}
}
