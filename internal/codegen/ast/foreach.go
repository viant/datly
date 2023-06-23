package ast

import "fmt"

func (s *Foreach) Generate(builder *Builder) (err error) {
	switch builder.Lang {
	case LangVelty:
		if err = builder.WriteIndentedString("\n#foreach("); err != nil {
			return err
		}
		if err = s.Value.Generate(builder); err != nil {
			return err
		}
		if err = builder.WriteString(" in "); err != nil {
			return err
		}
		if err = s.Set.Generate(builder); err != nil {
			return err
		}
		if err = builder.WriteString(")"); err != nil {
			return err
		}

		bodyBuilder := builder.IncIndent("  ")
		if err = s.Body.Generate(bodyBuilder); err != nil {
			return err
		}
		if err = builder.WriteIndentedString("\n#end"); err != nil {
			return err
		}
		return nil

	case LangGO:
		if err = builder.WriteIndentedString("\nfor _, "); err != nil {
			return err
		}

		if err = s.Value.Generate(builder); err != nil {
			return err
		}

		if err = builder.WriteString(" := range "); err != nil {
			return err
		}

		if err = s.Set.Generate(builder); err != nil {
			return err
		}

		if err = builder.WriteString(" { "); err != nil {
			return err
		}

		bodyBuilder := builder.IncIndent("  ")
		if err = bodyBuilder.WriteIndentedString("\n"); err != nil {
			return err
		}

		if err = s.Body.Generate(bodyBuilder); err != nil {
			return err
		}

		if err = builder.WriteString("\n}"); err != nil {
			return err
		}

		return nil
	}

	return fmt.Errorf("unsupported option %T %v\n", s, builder.Lang)
}

func NewForEach(value, set *Ident, body Block) *Foreach {
	return &Foreach{
		Value: value,
		Set:   set,
		Body:  body,
	}
}
