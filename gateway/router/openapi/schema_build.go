package openapi

import (
	"context"
	"fmt"
	"github.com/viant/datly/internal/setter"
	"github.com/viant/datly/view"
	"github.com/viant/datly/view/state"
	"github.com/viant/tagly/format/text"
	ftime "github.com/viant/tagly/format/time"
	"github.com/viant/xreflect"
	"os"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/viant/datly/gateway/router/openapi/openapi3"
)

func (c *SchemaContainer) addToSchema(ctx context.Context, component *ComponentSchema, dst *openapi3.Schema, schema *Schema) error {
	rType := dereferenceType(schema.rType)
	applySchemaExample(dst, schema)

	switch rType.Kind() {
	case reflect.Slice, reflect.Array:
		return c.addArraySchema(ctx, component, dst, schema, rType)
	case reflect.Struct:
		return c.addStructSchema(ctx, component, dst, schema, rType)
	default:
		return c.addDefaultSchema(ctx, component, dst, schema, rType)
	}
}

func (c *SchemaContainer) addArraySchema(ctx context.Context, component *ComponentSchema, dst *openapi3.Schema, schema *Schema, rType reflect.Type) error {
	itemSchema, err := c.createSchema(ctx, component, schema.SliceItem(rType))
	if err != nil {
		return err
	}
	dst.Type = arrayOutput
	dst.Items = itemSchema
	return nil
}

func (c *SchemaContainer) addStructSchema(ctx context.Context, component *ComponentSchema, dst *openapi3.Schema, schema *Schema, rType reflect.Type) error {
	if rType == xreflect.TimeType {
		addTimeSchema(dst, schema)
		return nil
	}

	dst.Type = objectOutput
	dst.Properties = openapi3.Schemas{}
	rootTable := rootTable(component)
	table := schema.tag.Table

	for i := 0; i < rType.NumField(); i++ {
		field := rType.Field(i)
		if shouldSkipStructField(field) {
			continue
		}

		aTag, err := ParseTag(field, field.Tag, schema.isInput, rootTable)
		if err != nil {
			return err
		}
		if normalizeFieldTag(aTag, field.Name, rootTable, table) {
			table = aTag.Table
		}
		if shouldSkipByTag(component, aTag) {
			continue
		}
		if aTag.Inlined {
			dst.AdditionalPropertiesAllowed = setter.BoolPtr(true)
			continue
		}

		fieldSchema, err := schema.Field(field, aTag)
		if err != nil {
			return err
		}
		if component.component.Output.IsExcluded(fieldSchema.path) {
			continue
		}

		updatedDocumentation(aTag, component.component.Docs(), fieldSchema)
		if field.Anonymous {
			if err := c.addToSchema(ctx, component, dst, fieldSchema); err != nil {
				return err
			}
			continue
		}

		childSchema, err := c.createSchema(ctx, component, fieldSchema)
		if err != nil {
			return err
		}
		dst.Properties[fieldSchema.fieldName] = childSchema
		if !aTag.IsNullable {
			dst.Required = append(dst.Required, fieldSchema.fieldName)
		}
	}

	return nil
}

func (c *SchemaContainer) addDefaultSchema(ctx context.Context, component *ComponentSchema, dst *openapi3.Schema, schema *Schema, rType reflect.Type) error {
	switch rType.Kind() {
	case reflect.Interface:
		return c.addInterfaceSchema(ctx, component, dst, schema, rType)
	case reflect.Map:
		return c.addMapSchema(ctx, component, dst, schema, rType)
	default:
		apiType, format, err := c.toOpenApiType(rType)
		if err != nil {
			return err
		}
		dst.Type = apiType
		dst.Format = format
		return nil
	}
}

func (c *SchemaContainer) addInterfaceSchema(ctx context.Context, component *ComponentSchema, dst *openapi3.Schema, schema *Schema, interfaceType reflect.Type) error {
	dst.Type = objectOutput
	variants, skipped, err := c.interfaceVariants(ctx, component, schema, interfaceType)
	if err != nil {
		return err
	}
	if len(skipped) > 0 {
		if shouldFailOnPolymorphismSkip() {
			return fmt.Errorf("failed to resolve polymorphic variants for %s: %s", interfaceType.String(), strings.Join(skipped, ","))
		}
		if dst.Extension == nil {
			dst.Extension = openapi3.Extension{}
		}
		dst.Extension["x-datly-polymorphism-skipped"] = skipped
		dst.Extension["x-datly-polymorphism-mode"] = "best-effort"
	}
	if len(variants) > 0 {
		dst.OneOf = variants
		if discriminator := oneOfDiscriminator(variants); discriminator != nil {
			dst.Discriminator = discriminator
			c.applyDiscriminatorToVariants(discriminator)
		}
	}
	return nil
}

func (c *SchemaContainer) interfaceVariants(ctx context.Context, component *ComponentSchema, schema *Schema, interfaceType reflect.Type) (openapi3.SchemaList, []string, error) {
	if component == nil || component.component == nil {
		return nil, nil, nil
	}
	registry := component.component.TypeRegistry()
	if registry == nil {
		return nil, nil, nil
	}

	packageNames := registry.PackageNames()
	sort.Strings(packageNames)

	seenByType := map[string]bool{}
	result := make(openapi3.SchemaList, 0)
	var skipped []string
	for _, packageName := range packageNames {
		pkg := registry.Package(packageName)
		if pkg == nil {
			continue
		}

		typeNames := pkg.TypeNames()
		sort.Strings(typeNames)
		for _, typeName := range typeNames {
			candidateType, err := pkg.Lookup(typeName)
			if err != nil || candidateType == nil {
				continue
			}
			candidateType = dereferenceType(candidateType)
			if !implementsInterface(candidateType, interfaceType) {
				continue
			}
			if candidateType.Kind() == reflect.Interface {
				continue
			}

			key := candidateType.String()
			if seenByType[key] {
				continue
			}
			seenByType[key] = true

			typeLabel := typeName
			if typeLabel == "" {
				typeLabel = candidateType.String()
			}

			variantSchema := &Schema{
				docs:        schema.docs,
				pkg:         schema.pkg,
				path:        key,
				fieldName:   typeLabel,
				name:        typeLabel,
				description: schema.description,
				example:     schema.example,
				rType:       candidateType,
				tag:         Tag{},
				ioConfig:    schema.ioConfig,
				isInput:     schema.isInput,
			}
			variantSchema.tag.TypeName = typeLabel

			builtSchema, err := c.createSchema(ctx, component, variantSchema)
			if err != nil {
				skipped = append(skipped, typeLabel)
				continue
			}
			if builtSchema.Ref == "" {
				skipped = append(skipped, typeLabel)
				continue
			}
			result = append(result, builtSchema)
		}
	}
	return result, skipped, nil
}

func (c *SchemaContainer) addMapSchema(ctx context.Context, component *ComponentSchema, dst *openapi3.Schema, schema *Schema, rType reflect.Type) error {
	valueSchema, err := c.mapValueSchema(ctx, component, schema, rType.Elem())
	if err != nil {
		return err
	}
	dst.Type = objectOutput
	dst.AdditionalProperties = valueSchema
	return nil
}

func (c *SchemaContainer) mapValueSchema(ctx context.Context, component *ComponentSchema, parent *Schema, valueType reflect.Type) (*openapi3.Schema, error) {
	valueType = dereferenceType(valueType)
	if apiType, format, ok := c.asOpenApiType(valueType); ok {
		return &openapi3.Schema{Type: apiType, Format: format}, nil
	}

	switch valueType.Kind() {
	case reflect.Slice, reflect.Array:
		itemsSchema, err := c.mapValueSchema(ctx, component, parent, valueType.Elem())
		if err != nil {
			return nil, err
		}
		return &openapi3.Schema{Type: arrayOutput, Items: itemsSchema}, nil
	default:
		valueFieldSchema := &Schema{
			docs:        parent.docs,
			pkg:         parent.pkg,
			path:        parent.path + ".value",
			fieldName:   parent.fieldName,
			name:        parent.name,
			description: parent.description,
			example:     parent.example,
			rType:       valueType,
			tag:         Tag{},
			ioConfig:    parent.ioConfig,
			isInput:     parent.isInput,
		}
		if valueType.Name() != "" {
			valueFieldSchema.tag.TypeName = valueType.Name()
		}
		return c.createSchema(ctx, component, valueFieldSchema)
	}
}

func (c *ComponentSchema) GetOrGenerateSchema(ctx context.Context, schema *Schema) (*openapi3.Schema, error) {
	return c.schemas.CreateSchema(ctx, c, schema)
}

func (c *SchemaContainer) CreateSchema(ctx context.Context, componentSchema *ComponentSchema, fieldSchema *Schema) (*openapi3.Schema, error) {
	c.mux.Lock()
	defer c.mux.Unlock()

	return c.createSchema(ctx, componentSchema, fieldSchema)
}

func (c *SchemaContainer) createSchema(ctx context.Context, componentSchema *ComponentSchema, fieldSchema *Schema) (*openapi3.Schema, error) {
	description, err := componentSchema.Description(ctx, fieldSchema.path, fieldSchema.description)
	if err != nil {
		return nil, err
	}
	example, err := componentSchema.Example(ctx, fieldSchema.path, fieldSchema.example)
	if err != nil {
		return nil, err
	}

	if fieldSchema.tag.TypeName != "" {
		if _, ok := c.generatedSchemas[fieldSchema.tag.TypeName]; ok {
			return c.SchemaRef(fieldSchema.tag.TypeName, description), nil
		}
	}

	if apiType, format, ok := c.asOpenApiType(fieldSchema.rType); ok {
		return &openapi3.Schema{
			Type:        apiType,
			Format:      format,
			Description: description,
			Example:     example,
		}, nil
	}

	schema, err := componentSchema.GenerateSchema(ctx, fieldSchema)
	if err != nil {
		return nil, err
	}

	if fieldSchema.tag.TypeName != "" {
		c.generatedSchemas[fieldSchema.tag.TypeName] = schema
		c.schemas = append(c.schemas, schema)
		schema = c.SchemaRef(fieldSchema.tag.TypeName, description)
	}

	return schema, nil
}

func (c *SchemaContainer) SchemaRef(schemaName string, description string) *openapi3.Schema {
	return &openapi3.Schema{
		Ref:         "#/components/schemas/" + schemaName,
		Description: description,
	}
}

func (c *SchemaContainer) toOpenApiType(rType reflect.Type) (string, string, error) {
	apiType, format, ok := c.asOpenApiType(rType)
	if !ok {
		return empty, empty, fmt.Errorf("unsupported openapi3 type %v", rType.String())
	}
	return apiType, format, nil
}

func (c *SchemaContainer) asOpenApiType(rType reflect.Type) (string, string, bool) {
	rType = dereferenceType(rType)
	switch rType.Kind() {
	case reflect.Int, reflect.Int64, reflect.Uint, reflect.Uint64:
		return integerOutput, int64Format, true
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return integerOutput, int32Format, true
	case reflect.Float64, reflect.Float32:
		return numberOutput, doubleFormat, true
	case reflect.Bool:
		return booleanOutput, empty, true
	case reflect.String:
		return stringOutput, empty, true
	}

	return empty, empty, false
}

func updatedDocumentation(aTag *Tag, docs *state.Docs, fieldSchema *Schema) {
	if docs == nil {
		return
	}
	if aTag.Column != "" && len(docs.Columns) > 0 {
		columns := docs.Columns
		if aTag.Description == "" {
			aTag.Description, _ = columns.ColumnDescription(aTag.Table, aTag.Column)
		}
		if aTag.Description == "" {
			aTag.Description, _ = columns.ColumnDescription("", aTag.Column)
		}
		if aTag.Example == "" {
			aTag.Example, _ = columns.ColumnExample(aTag.Table, aTag.Column)
		}
	}
	if aTag.Description == "" && len(docs.Paths) > 0 {
		if desc, ok := docs.Paths.ByName(fieldSchema.path); ok {
			aTag.Description = desc
		} else if desc, ok := docs.Paths.ByName(fieldSchema.name); ok {
			aTag.Description = desc
			fieldSchema.description = desc
		}
	}
	if aTag.Description != "" {
		fieldSchema.description = aTag.Description
	}
	if aTag.Example != "" {
		fieldSchema.example = aTag.Example
	}
}

func containsAny(format string, values ...string) bool {
	for _, value := range values {
		if strings.Contains(format, value) {
			return true
		}
	}
	return false
}

func hasInternalColumnTag(v *view.View, table, column string) bool {
	if v == nil || column == "" {
		return false
	}
	if matchesViewTable(v, table) {
		if cfg := v.ColumnsConfig[column]; cfg != nil && cfg.Tag != nil && strings.Contains(*cfg.Tag, `internal:"true"`) {
			return true
		}
	}
	for _, rel := range v.With {
		if rel == nil || rel.Of == nil {
			continue
		}
		if hasInternalColumnTag(&rel.Of.View, table, column) {
			return true
		}
	}
	return false
}

func matchesViewTable(v *view.View, table string) bool {
	if table == "" {
		return true
	}
	return strings.EqualFold(v.Table, table) || strings.EqualFold(v.Alias, table) || strings.EqualFold(v.Name, table)
}

func rootTable(component *ComponentSchema) string {
	if component.component.View.Mode == view.ModeQuery {
		return component.component.View.Table
	}
	return ""
}

func dereferenceType(rType reflect.Type) reflect.Type {
	for rType.Kind() == reflect.Ptr {
		rType = rType.Elem()
	}
	return rType
}

func applySchemaExample(dst *openapi3.Schema, schema *Schema) {
	if schema.tag.Example != "" {
		dst.Example = schema.tag.Example
	}
}

func addTimeSchema(dst *openapi3.Schema, schema *Schema) {
	dst.Type = stringOutput
	timeLayout := schema.tag._tag.TimeLayout
	if timeLayout == "" {
		timeLayout = time.RFC3339
	}
	if containsAny(timeLayout, "15", "04", "05") {
		dst.Format = "date-time"
	} else {
		dst.Format = "date"
	}
	if dst.Example == nil {
		dst.Example = time.Now().Format(timeLayout)
	}
	dst.Pattern = ftime.TimeLayoutToDateFormat(timeLayout)
}

func shouldSkipStructField(field reflect.StructField) bool {
	if field.PkgPath != "" {
		return true
	}
	rawTag := string(field.Tag)
	return strings.Contains(rawTag, `internal:"true"`) || strings.Contains(rawTag, `json:"-"`)
}

func normalizeFieldTag(aTag *Tag, fieldName, rootTable, currentTable string) (updatedTable bool) {
	if aTag.Table == "" {
		aTag.Table = currentTable
	}
	if aTag.Ignore {
		return false
	}
	if aTag.Column != "" && currentTable == "" {
		aTag.Table = rootTable
		return true
	}
	if currentTable != "" && aTag.Column == "" {
		aTag.Column = text.DetectCaseFormat(fieldName).To(text.CaseFormatUpperUnderscore).Format(fieldName)
	}
	return false
}

func shouldSkipByTag(component *ComponentSchema, aTag *Tag) bool {
	if aTag.Ignore {
		return true
	}
	return hasInternalColumnTag(component.component.View, aTag.Table, aTag.Column) ||
		hasInternalColumnTag(component.component.View, "", aTag.Column)
}

func implementsInterface(candidateType, interfaceType reflect.Type) bool {
	if candidateType.Implements(interfaceType) {
		return true
	}
	if candidateType.Kind() != reflect.Ptr && reflect.PtrTo(candidateType).Implements(interfaceType) {
		return true
	}
	return false
}

func oneOfDiscriminator(variants openapi3.SchemaList) *openapi3.Discriminator {
	mapping := map[string]string{}
	for _, variant := range variants {
		if variant == nil || variant.Ref == "" {
			continue
		}
		ref := variant.Ref
		name := ref[strings.LastIndex(ref, "/")+1:]
		if name == "" {
			continue
		}
		mapping[name] = ref
	}
	if len(mapping) == 0 {
		return nil
	}
	return &openapi3.Discriminator{
		PropertyName: "type",
		Mapping:      mapping,
	}
}

func (c *SchemaContainer) applyDiscriminatorToVariants(discriminator *openapi3.Discriminator) {
	if discriminator == nil || len(discriminator.Mapping) == 0 {
		return
	}
	for value, ref := range discriminator.Mapping {
		schemaName := refName(ref)
		if schemaName == "" {
			continue
		}
		variant := c.generatedSchemas[schemaName]
		if variant == nil || variant.Type != objectOutput {
			continue
		}
		if len(variant.Properties) == 0 {
			variant.Properties = openapi3.Schemas{}
		}
		if variant.Properties[discriminator.PropertyName] == nil {
			variant.Properties[discriminator.PropertyName] = &openapi3.Schema{
				Type: stringOutput,
				Enum: []interface{}{value},
			}
		}
		if !containsString(variant.Required, discriminator.PropertyName) {
			variant.Required = append(variant.Required, discriminator.PropertyName)
		}
	}
}

func refName(ref string) string {
	if ref == "" {
		return ""
	}
	index := strings.LastIndex(ref, "/")
	if index == -1 || index == len(ref)-1 {
		return ""
	}
	return ref[index+1:]
}

func containsString(values []string, target string) bool {
	for _, item := range values {
		if item == target {
			return true
		}
	}
	return false
}

func shouldFailOnPolymorphismSkip() bool {
	raw := strings.TrimSpace(strings.ToLower(os.Getenv("DATLY_OPENAPI_POLY_STRICT")))
	return raw == "1" || raw == "true" || raw == "yes"
}
