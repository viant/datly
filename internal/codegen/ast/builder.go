package ast

import "strings"

const (
	LangVelty = "velty"
	LangGO    = "go"
)

type (
	Builder struct {
		*strings.Builder
		Options
		Indent       string
		State        *Scope
		declarations map[string]string
	}
)

func (b *Builder) WriteIndentedString(s string) error {
	fragment := strings.ReplaceAll(s, "\n", "\n"+b.Indent)
	_, err := b.Builder.WriteString(fragment)
	return err
}

func (b *Builder) IncIndent(indent string) *Builder {
	newBuilder := *b
	newBuilder.Indent += indent
	newBuilder.State = newBuilder.State.NextScope()
	return &newBuilder
}

func (b *Builder) WriteString(s string) error {
	_, err := b.Builder.WriteString(s)
	return err
}

func NewBuilder(option Options, declaredVariables ...string) *Builder {
	return &Builder{
		Builder:      &strings.Builder{},
		Options:      option,
		declarations: map[string]string{},
		State:        NewScope(declaredVariables...),
	}
}
