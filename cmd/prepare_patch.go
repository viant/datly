package cmd

import (
	"context"
	"fmt"
	"github.com/viant/datly/cmd/option"
	"net/http"
	"strings"
)

type (
	patchStmtBuilder struct {
		*stmtBuilder
		insert *insertStmtBuilder
		update *updateStmtBuilder
	}

	indexChecker struct {
		indexName string
		field     string
		paramName string
	}
)

func newPatchStmtBuilder(sb *strings.Builder, metadata *inputMetadata) *patchStmtBuilder {
	builder := newStmtBuilder(sb, metadata, withSQL(true))
	return &patchStmtBuilder{
		stmtBuilder: builder,
		insert: &insertStmtBuilder{
			stmtBuilder: builder,
		},
		update: &updateStmtBuilder{
			stmtBuilder: builder,
		},
	}
}

func (s *Builder) preparePatchRule(ctx context.Context, builder *routeBuilder, sourceSQL []byte) (string, error) {
	routeOption, config, paramType, err := s.buildInputMetadata(ctx, builder, sourceSQL, http.MethodPatch)
	if err != nil {
		return "", err
	}

	_, sqlPart := s.extractRouteSettings(sourceSQL)
	sourceSQL = []byte(sqlPart)
	SQL, err := s.buildPatchSQL(builder, routeOption, config, paramType, sqlPart)
	if err != nil {
		return "", err
	}

	if _, err = s.upload(builder, builder.session.TemplateURL(s.fileNames.unique(config.fileName))+".sql", SQL); err != nil {
		return "", err
	}

	return SQL, err
}

func (s *Builder) buildPatchSQL(builder *routeBuilder, routeOption *option.RouteConfig, config *ViewConfig, metadata *inputMetadata, preSQL string) (string, error) {
	sb, err := s.prepareStringBuilder(builder, metadata, config, routeOption)
	if err != nil {
		return "", err
	}

	patchBuilder := newPatchStmtBuilder(sb, metadata)
	return patchBuilder.buildWithMeta(s.options.Prepare, preSQL)
}

func (b *patchStmtBuilder) buildWithMeta(opt Prepare, preSQL string) (string, error) {
	if err := b.stmtBuilder.appendHints(b.typeDef); err != nil {
		return "", err
	}

	if err := b.stmtBuilder.appendSQLWithRelations(opt.LoadPrevious, preSQL); err != nil {
		return "", err
	}

	b.insert.appendAllocation(b.typeDef, "", b.typeDef.paramName)
	indexes, err := b.generateIndexes(opt.LoadPrevious, true)
	if err != nil {
		return "", nil
	}

	return b.build("", true, indexes)
}

func (b *patchStmtBuilder) build(parentRecord string, withUnsafe bool, indexes []*indexChecker) (string, error) {
	stack := NewStack()
	accessor, multi := b.appendForEachIfNeeded(parentRecord, b.paramName, &withUnsafe, stack)
	contentBuilder := b
	if multi {
		contentBuilder = contentBuilder.withIndent()
	}

	if b.appendOptionalIfNeeded(accessor, stack) {
		contentBuilder = contentBuilder.withIndent()
	}

	if err := contentBuilder.appendPatchContent(indexes, accessor); err != nil {
		return "", err
	}

	for _, relation := range contentBuilder.typeDef.relations {
		if relation.config.isVirtual {
			continue
		}
		if _, err := contentBuilder.newRelation(relation).build(accessor.record, withUnsafe, indexes); err != nil {
			return "", nil
		}
	}

	stack.Flush()
	return b.sb.String(), nil
}

func (b *patchStmtBuilder) appendPatchContent(indexes []*indexChecker, accessor *paramAccessor) error {
	contentBuilder := b.withIndent()

	b.writeString("\n#if(")
	var i = 0
	for _, index := range indexes {
		if index.paramName != b.typeDef.paramName {
			continue
		}

		if i != 0 {
			contentBuilder.writeString(" && ")
		}

		i++
		contentBuilder.writeString(fmt.Sprintf("($%v.HasKey($%v) == true)", index.indexName, accessor.unsafeRecord+"."+index.field))
	}

	contentBuilder.writeString(")")

	if err := contentBuilder.update.appendUpdate(accessor, b.update.parent); err != nil {
		return err
	}

	b.writeString("\n#else")
	if err := contentBuilder.insert.appendInsert(accessor); err != nil {
		return err
	}

	b.writeString("\n#end")
	return nil
}

func (b *patchStmtBuilder) withIndent() *patchStmtBuilder {
	aCopy := *b
	aCopy.update = b.update.withIndent()
	aCopy.insert = b.insert.withIndent()
	aCopy.stmtBuilder = aCopy.stmtBuilder.withIndent()
	return &aCopy
}

func (b *patchStmtBuilder) newRelation(metadata *inputMetadata) *patchStmtBuilder {
	return &patchStmtBuilder{
		stmtBuilder: b.stmtBuilder.newRelation(metadata),
		insert:      b.insert.newRelation(metadata),
		update:      b.update.newRelation(metadata),
	}
}
