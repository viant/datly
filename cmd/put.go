package cmd

import (
	"context"
	"fmt"
	"github.com/viant/datly/cmd/option"
	"github.com/viant/datly/view"
	"strings"
)

type updateStmtBuilder struct {
	parent *updateStmtBuilder
	*stmtBuilder
}

func newUpdateStmtBuilder(sb *strings.Builder, def *inputMetadata) *updateStmtBuilder {
	return &updateStmtBuilder{
		stmtBuilder: newStmtBuilder(sb, def, string(view.KindRequestBody)),
	}
}

func (s *Builder) preparePutRule(ctx context.Context, SQL []byte) (string, error) {
	routeConfig, config, metadata, err := s.buildInputMetadata(ctx, SQL)
	if err != nil {
		return "", err
	}

	template, err := s.buildUpdateSQL(routeConfig, config, metadata)
	if err != nil {
		return "", err
	}

	if _, err = s.uploadSQL(folderSQL, s.unique(config.fileName, s.fileNames, false), template, false); err != nil {
		return "", nil
	}

	return template, nil
}

func (s *Builder) buildUpdateSQL(routeConfig *option.RouteConfig, aViewConfig *viewConfig, metadata *inputMetadata) (string, error) {
	sb, err := s.prepareStringBuilder(metadata, aViewConfig, routeConfig)
	if err != nil {
		return "", err
	}

	return newUpdateStmtBuilder(sb, metadata).build("", true)
}

func (b *updateStmtBuilder) build(parentRecord string, withUnsafe bool) (string, error) {
	b.sb.WriteString("\nUPDATE ")
	b.sb.WriteString(b.typeDef.table)
	b.sb.WriteString("\nSET")
	if err := b.stmtBuilder.appendColumnNameValues(b.accessParam(parentRecord, b.paramName, false), func(field *view.Field) bool {
		fieldMeta, ok := b.typeDef.meta.metaByColName(field.Column)
		if !ok {
			return ok
		}

		return fieldMeta.primaryKey
	}); err != nil {
		return "", err
	}

	pkFields := b.typeDef.primaryKeyFields()
	if len(pkFields) == 0 {
		return "", fmt.Errorf("not found primary keys for table %v", b.typeDef.table)
	}

	conKeyword := "\nWHERE "
	for _, field := range pkFields {
		b.writeString(conKeyword)
		b.writeString(fmt.Sprintf("%v = $%v.%v", field.Column, b.accessParam(parentRecord, b.paramName, false), field.Name))
		conKeyword = " AND "
	}

	return b.sb.String(), nil
}
