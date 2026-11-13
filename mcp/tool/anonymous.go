package tool

import (
	"encoding"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/datly/internal/inputvisibility"
	"github.com/viant/datly/runtime/registry"
	"github.com/viant/datly/spec"
	"github.com/viant/mcp-protocol/schema"
	"github.com/viant/mcp-protocol/schema/fieldname"
	"github.com/viant/mcp-protocol/schema/jsonschema"
	"github.com/viant/tagly/choice"
	tagformat "github.com/viant/tagly/format"
	xshape "github.com/viant/x/shape"
)

func (c *Compiler) compileAnonymousBody(inputField registry.InputField, caseFormat string) ([]Argument, error) {
	binding := inputField.Binding()
	if !strings.EqualFold(strings.TrimSpace(binding.Location.Kind), "body") {
		return nil, fmt.Errorf("anonymous field %q must use body binding, got %q", inputField.Path(), binding.Location.Kind)
	}
	if strings.TrimSpace(binding.Location.In) != "" {
		return nil, fmt.Errorf("anonymous body field %q cannot use named body source %q", inputField.Path(), binding.Location.In)
	}
	types := xshape.Runtime{}
	sourceType := types.Indirect(inputField.SourceType())
	customText := false
	if sourceType != nil {
		pointer := reflect.PointerTo(sourceType)
		// Match the pinned reflector's custom method sets without executing them.
		customText = sourceType.Implements(reflect.TypeFor[encoding.TextMarshaler]()) || pointer.Implements(reflect.TypeFor[encoding.TextMarshaler]()) || pointer.Implements(reflect.TypeFor[encoding.TextUnmarshaler]())
	}
	if inputField.WireType() != nil || customText {
		return nil, fmt.Errorf("anonymous body field %q custom JSON source requires a direct named body argument", inputField.Path())
	}
	customJSONBody := fieldname.CustomJSON(sourceType)
	if customJSONBody {
		caseFormat = ""
	}
	destinationType := types.Indirect(inputField.DestinationType())
	if sourceType == nil || sourceType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("anonymous body field %q source type must be a struct, got %v", inputField.Path(), inputField.SourceType())
	}
	if sourceType != destinationType {
		return nil, fmt.Errorf("anonymous body field %q requires matching source and destination structs, got %v and %v", inputField.Path(), inputField.SourceType(), inputField.DestinationType())
	}
	metadata := inputField.Representation()
	for metadata != nil && metadata.Original().Kind() == reflect.Pointer {
		metadata = metadata.Elem()
	}
	var fields []xshape.JSONField
	var err error
	if metadata != nil {
		fields, err = metadata.JSONFields()
	} else {
		fields, err = xshape.Linked(sourceType).JSONFields()
	}
	if err != nil {
		return nil, err
	}
	_, requiredNames := schema.StructToProperties(sourceType)
	if metadata != nil {
		published, e := (jsonschema.Reflector{}).Compile(jsonschema.Request{Type: sourceType, Representation: metadata})
		if e != nil {
			return nil, e
		}
		requiredNames, _ = published["required"].([]string)
	}
	required := make(map[string]bool, len(requiredNames))
	for _, name := range requiredNames {
		required[name] = true
	}
	nestedSchemas := inputField.WireSchemas()
	result := make([]Argument, 0, len(fields))
	for _, projected := range fields {
		if !projected.Field.Exported || inputvisibility.InternalRepresentation(sourceType, projected.Field.Index, metadata) {
			continue
		}
		field := projected.Field.StructField()
		publicName, aliases, hidden, err := publicFieldName(projected.Name, projected.Name, field)
		if err != nil {
			return nil, err
		}
		if hidden {
			continue
		}
		requiredName := publicName
		formatted, err := memberNameVisibility(caseFormat).Resolve(field, projected.Name)
		if err != nil {
			return nil, err
		}
		publicName = formatted.Public

		goName := ""
		// Legacy anonymous bodies accept the exact Go spelling before JSON names.
		// Explicit MCP naming/aliases retain their own existing conflict policy.
		if strings.TrimSpace(field.Tag.Get("mcp")) == "" && field.Name != publicName {
			goName = field.Name
			aliases = append(aliases, goName)
		}
		var childMetadata *xshape.Representation
		if metadata != nil {
			childMetadata = metadata.FieldShapeByIndex(projected.Field.Index)
		}
		selectedField := projected
		collection, err := choice.ParseCollectionEnum(field.Tag, field.Type)
		if err != nil {
			return nil, err
		}
		// Each flattened argument becomes its own schema root. Retain only its
		// authored subtree, remapping the original body path to that root.
		wireSchemas := map[string]*spec.WireSchema{}
		prefix := canonicalSchemaPath(sourceType, inputField.Path()+"."+field.Name, representationVisibility(metadata, caseFormat))
		for path, policy := range nestedSchemas {
			path = canonicalSchemaPath(sourceType, path, representationVisibility(metadata, caseFormat))
			if path == prefix || strings.HasPrefix(path, prefix+".") {
				wireSchemas[field.Name+strings.TrimPrefix(path, prefix)] = policy
			}
		}
		schemaRequired := required[requiredName]
		if policy := wireSchemas[field.Name]; policy != nil && policy.Required {
			schemaRequired = true
		}
		policy, err := tagformat.Parse(field.Tag)
		if err != nil {
			return nil, err
		}
		var nullable *bool
		if !customJSONBody && wireSchemas[field.Name] == nil && collection == nil {
			nullable = policy.Nullable
		}
		result = append(result, Argument{
			nullable:       nullable,
			enumeration:    collection,
			schemaField:    &selectedField,
			representation: childMetadata,
			documentation:  inputField.Documentation(), publicName: publicName, aliases: aliases, path: inputField.Path() + "." + field.Name,
			sourceKind: "body", sourceName: projected.Name,
			sourceType: field.Type, destinationType: field.Type,
			parentBodyType: inputField.SourceType(), parentBodySource: binding.Location.In, customJSONBody: customJSONBody,
			schemaRequired: schemaRequired, anonymousGoName: goName, wireSchemas: wireSchemas,
		})
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("anonymous body field %q has no exported JSON fields", inputField.Path())
	}
	return result, nil
}
