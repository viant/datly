package cmd

import (
	"context"
	"fmt"
	"github.com/viant/datly/cmd/option"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/keywords"
	"strings"
)

type updateStmtBuilder struct {
	parent *updateStmtBuilder
	*stmtBuilder
}

func newUpdateStmtBuilder(sb *strings.Builder, def *inputMetadata) *updateStmtBuilder {
	return &updateStmtBuilder{
		stmtBuilder: newStmtBuilder(sb, def),
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

	if _, err = s.upload(s.preGenSQLURL(s.fileNames.unique(config.fileName)), template); err != nil {
		return "", nil
	}

	return template, nil
}

func (s *Builder) buildUpdateSQL(routeConfig *option.RouteConfig, aViewConfig *viewConfig, metadata *inputMetadata) (string, error) {
	sb, err := s.prepareStringBuilder(metadata, aViewConfig, routeConfig)
	if err != nil {
		return "", err
	}

	builder := newUpdateStmtBuilder(sb, metadata)
	if err = builder.appendHintsWithRelations(); err != nil {
		return "", err
	}

	if err = builder.iterateOverHints(metadata, func(currMetadata *inputMetadata) error {
		if !currMetadata.config.unexpandedTable.ViewConfig.FetchRecords {
			return nil
		}

		return builder.appendSQLHint(currMetadata)
	}); err != nil {
		return "", err
	}

	if err = builder.iterateOverHints(metadata, func(currMetadata *inputMetadata) error {
		if !currMetadata.config.unexpandedTable.ViewConfig.FetchRecords {
			return nil
		}

		builder.generateIndexIfNeeded(currMetadata)
		return nil
	}); err != nil {
		return "", err
	}

	return builder.build("", true)
}

func (usb *updateStmtBuilder) build(parentRecord string, withUnsafe bool) (string, error) {
	stack := NewStack()
	accessor, ok := usb.appendForEachIfNeeded(parentRecord, usb.paramName, &withUnsafe, stack)
	contentBuilder := usb
	if ok {
		contentBuilder = usb.withIndent()
	}

	if usb.appendOptionalIfNeeded(accessor, stack) {
		contentBuilder = contentBuilder.withIndent()
	}

	if err := contentBuilder.appendUpdate(accessor, contentBuilder.parent); err != nil {
		return "", err
	}

	for _, rel := range contentBuilder.typeDef.relations {
		_, err := contentBuilder.newRelation(rel).build(accessor.record, !contentBuilder.isMulti && withUnsafe)
		if err != nil {
			return "", err
		}
	}

	stack.Flush()
	return usb.sb.String(), nil
}

func (usb *updateStmtBuilder) appendUpdate(accessor *paramAccessor, parent *updateStmtBuilder) error {
	appended := false
	if parent != nil {
		appended = usb.appendFkCheck(accessor, parent.stmtBuilder)
	}

	if appended {
		defer func() {
			usb.writeString("\n#end")
		}()
	}

	if strings.ToLower(usb.typeDef.config.unexpandedTable.ExecKind) != option.ExecKindDML {
		usb.writeString(fmt.Sprintf("\n$%v.Update($%v, \"%v\");", keywords.KeySQL, accessor.record, usb.typeDef.table))
		return nil
	}

	if parent != nil {
		usb.appendSetFk(accessor, parent.stmtBuilder)
	}

	usb.writeString("\nUPDATE ")
	usb.writeString(usb.typeDef.table)
	usb.writeString("\nSET")
	if err := usb.stmtBuilder.appendColumnNameValues(accessor, true, nil); err != nil {
		return err
	}

	qualifiedFields := usb.qualifiedFields()
	if len(qualifiedFields) == 0 {
		return fmt.Errorf("not found pk/fk keys for table %v", usb.typeDef.table)
	}

	conKeyword := "\nWHERE "
	for _, field := range qualifiedFields {
		usb.writeString(conKeyword)
		usb.writeString(fmt.Sprintf("%v = $%v.%v", field.Column, accessor.record, field.Name))
		conKeyword = " AND "
	}

	usb.writeString(";")
	return nil
}

func (usb *updateStmtBuilder) newRelation(rel *inputMetadata) *updateStmtBuilder {
	relation := usb.stmtBuilder.newRelation(rel)
	return &updateStmtBuilder{
		parent:      usb,
		stmtBuilder: relation,
	}
}

func (usb *updateStmtBuilder) qualifiedFields() []*view.Field {
	var fields []*view.Field
	for i, field := range usb.typeDef.actualFields {
		meta, ok := usb.typeDef.meta.metaByColName(field.Column)
		if !ok {
			continue
		}

		if !meta.primaryKey && !usb.isParentFk(meta) {
			continue
		}

		fields = append(fields, usb.typeDef.actualFields[i])
	}

	return fields
}

func (usb *updateStmtBuilder) isParentFk(meta *fieldMeta) bool {
	if usb.parent == nil || meta.fkKey == nil {
		return false
	}

	return usb.parent.typeDef.table == meta.fkKey.ReferenceTable
}

func (usb *updateStmtBuilder) withIndent() *updateStmtBuilder {
	aCopy := *usb
	aCopy.stmtBuilder = aCopy.stmtBuilder.withIndent()
	return &aCopy
}
