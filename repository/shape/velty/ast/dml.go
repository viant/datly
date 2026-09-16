package ast

import (
	"fmt"
	"strings"
)

type Insert struct {
	Table   string
	Columns []string
	Fields  []string
}

func (s *Insert) Generate(builder *Builder) (err error) {
	switch builder.Lang {
	case LangVelty:
		builder.WriteString("INSERT INTO ")
		builder.WriteString(s.Table)
		builder.WriteString("(")
		builder.WriteString(strings.Join(s.Columns, ","))
		builder.WriteString(") Fields(")
		builder.WriteString(strings.Join(s.Fields, ","))
		builder.WriteString(");")
	case LangGO:
		return fmt.Errorf("DML not yet supported for golang")
	}
	return nil
}

type Update struct {
	Table     string
	Columns   []string
	Fields    []string
	PkColumns []string
	PkFields  []string
}

func (s *Update) Generate(builder *Builder) (err error) {
	switch builder.Lang {
	case LangVelty:
		if err = builder.WriteString("UPDATE "); err != nil {
			return err
		}
		if err = builder.WriteString(s.Table); err != nil {
			return err
		}
		if err = builder.WriteString("SET "); err != nil {
			return err
		}
		for i, column := range s.PkColumns {
			if i > 0 {
				if err = builder.WriteString(","); err != nil {
					return err
				}
			}
			if err = builder.WriteString(column); err != nil {
				return err
			}
			if err = builder.WriteString(" = "); err != nil {
				return err
			}
			if err = builder.WriteString(s.PkFields[i]); err != nil {
				return err
			}
		}
		for i, column := range s.Columns {
			if err = builder.WriteString("\t#if("); err == nil {
				if err = builder.WriteString(getHasField(s.Fields[i])); err == nil {
					if err = builder.WriteString(")"); err == nil {
						if err = builder.WriteString(","); err == nil {
							if err = builder.WriteString(column); err == nil {
								if err = builder.WriteString(" = "); err == nil {
									if err = builder.WriteString(s.Fields[i]); err == nil {
										err = builder.WriteString("\t#end")
									}
								}
							}
						}
					}
				}
			}
		}
		return err
	case LangGO:
		return fmt.Errorf("DML not yet supported for golang")
	}
	return nil
}

func getHasField(field string) string {
	if index := strings.LastIndex(field, "."); index != -1 {
		leaf := field[index+1:]
		field = field[:index]
		return field + "." + "Has." + leaf
	}
	return field
}
