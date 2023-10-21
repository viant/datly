package mysql

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"github.com/viant/datly/service/dbms/provider"
	"github.com/viant/sqlx/io"
	"github.com/viant/sqlx/option"
	"github.com/viant/xreflect"
	"reflect"
)

func NewSQLSource() *SQLSource {
	return &SQLSource{}
}

type SQLSource struct {
}

func (s *SQLSource) CreateTable(recordType reflect.Type, tableName string, tagName option.Tag, autogeneratePk bool) (*provider.Table, error) {
	for recordType.Kind() == reflect.Slice {
		recordType = recordType.Elem()
	}

	columns, err := io.StructColumns(recordType, string(tagName))
	if err != nil {
		return nil, err
	}

	structFields := make([]reflect.StructField, 0)
	pkColumns := make([]io.Column, 0)

	buffer := bytes.NewBuffer(nil)
	buffer.WriteString("CREATE TABLE IF NOT EXISTS ")
	buffer.WriteByte('`')
	buffer.WriteString(tableName)
	buffer.WriteByte('`')
	buffer.WriteString(" (\n")
	for i, column := range columns {
		if i != 0 {
			buffer.WriteString(", \n")
		}

		if err = s.appendColumn(buffer, column, &pkColumns, &structFields); err != nil {
			return nil, err
		}
	}

	if autogeneratePk {
		column := io.NewColumn("DATLY_RECORD_ID", "", xreflect.IntType, io.WithTag(&io.Tag{Autoincrement: true, PrimaryKey: true}))
		pkColumns = make([]io.Column, 0)
		buffer.WriteString(", \n")
		if err = s.appendColumn(buffer, column, &pkColumns, nil); err != nil {
			return nil, err
		}
	}

	for _, column := range pkColumns {
		buffer.WriteString(", \n")
		buffer.WriteString("PRIMARY KEY(`")
		buffer.WriteString(column.Name())
		buffer.WriteString("`)")
	}

	buffer.WriteString("\n)")
	return &provider.Table{
		SQL:        buffer.String(),
		RecordType: reflect.StructOf(structFields),
	}, nil
}

func (s *SQLSource) RecordType(recordType reflect.Type, tagName option.Tag) (reflect.Type, error) {
	for recordType.Kind() == reflect.Slice {
		recordType = recordType.Elem()
	}

	columns, err := io.StructColumns(recordType, string(tagName))
	if err != nil {
		return nil, err
	}

	structFields := make([]reflect.StructField, 0, len(columns))
	for _, column := range columns {
		_, columnType, err := s.normalizeType(column)
		if err != nil {
			return nil, err
		}

		structFields = append(structFields, reflect.StructField{Name: column.Name(), Type: columnType})
	}

	return reflect.StructOf(structFields), nil
}

func (s *SQLSource) appendColumn(buffer *bytes.Buffer, column io.Column, pkColumns *[]io.Column, fields *[]reflect.StructField) error {
	buffer.WriteByte('`')
	buffer.WriteString(column.Name())
	buffer.WriteByte('`')
	buffer.WriteString(" ")
	databaseType, scanType, err := s.normalizeType(column)
	if err != nil {
		return err
	}

	buffer.WriteString(databaseType)
	if column.ScanType().Kind() != reflect.Ptr {
		buffer.WriteString(" NOT NULL ")
	}

	if tag := column.Tag(); tag != nil {
		if tag.Autoincrement {
			buffer.WriteString(" AUTO_INCREMENT ")
		}

		if tag.PrimaryKey {
			*pkColumns = append(*pkColumns, column)
		}
	}

	if fields != nil {
		*fields = append(*fields, reflect.StructField{
			Name: column.Name(),
			Type: scanType,
		})
	}

	return nil
}

func (s *SQLSource) normalizeType(column io.Column) (string, reflect.Type, error) {
	scanType := column.ScanType()
	wasPtr := false
	for scanType.Kind() == reflect.Ptr {
		scanType = scanType.Elem()
		wasPtr = true
	}

	databaseType, rType, err := s.normalizeDereferenced(column, scanType)
	if wasPtr && rType != nil {
		rType = reflect.PtrTo(rType)
	}

	return databaseType, rType, err
}

func (s *SQLSource) normalizeDereferenced(column io.Column, scanType reflect.Type) (string, reflect.Type, error) {
	switch scanType.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Uintptr:
		return "INTEGER", scanType, nil
	case reflect.Float32, reflect.Float64:
		return "DECIMAL", scanType, nil
	case reflect.Bool:
		return "BOOLEAN", scanType, nil
	case reflect.String:
		if tag := column.Tag(); tag != nil {
			if tag.PrimaryKey {
				return "VARCHAR(40)", scanType, nil
			}
		}

		return "TEXT", scanType, nil
	case reflect.Struct:
		if scanType == xreflect.TimeType {
			return "DATETIME", xreflect.TimeType, nil
		}

		return "TEXT", scanType, nil

	default:
		if tag := column.Tag(); tag != nil && tag.Encoding != "" {
			return "TEXT", reflect.TypeOf(json.RawMessage{}), nil
		}

		return "", nil, fmt.Errorf("unsupported column type %v", column.ScanType().String())
	}
}
