package registry

import (
	"context"
	"encoding/json"
	"github.com/viant/bindly"
	bindstate "github.com/viant/bindly/state"
	"github.com/viant/datly/spec"
	"github.com/viant/tagly/choice"
	"reflect"
	"testing"
)

func TestScalarAliasTextSource(t *testing.T) {
	type input struct {
		Mode string `choice:"0:ASAP" choice:"1:SPEND_EVENLY" enumAccept:"scalar" enumAlias:"0:0" enumAlias:"1:Even" enumStyle:"literal" enumCase:"fold"`
	}
	field := reflect.TypeFor[input]().Field(0)
	for _, kind := range []string{"query", "path", "header", "form", "cookie"} {
		t.Run(kind, func(t *testing.T) {
			b, err := EnumBinding(bindly.BindingSpec{Location: bindstate.Location{Kind: kind}}, field)
			if err != nil {
				t.Fatal(err)
			}
			if b.SourceType != reflect.TypeFor[string]() {
				t.Fatalf("text source incorrectly constrained to %v", b.SourceType)
			}
			got, err := b.Transformer.Transform(context.Background(), nil, "eVeN")
			if err != nil || got != "SPEND_EVENLY" {
				t.Fatalf("got=%v err=%v", got, err)
			}
		})
	}
}

func TestScalarAliasWireSchemaPersistenceAndClone(t *testing.T) {
	type input struct {
		Mode string `choice:"9007199254740993:HUGE" enumAccept:"scalar" enumStyle:"literal" enumCase:"fold"`
	}
	field := reflect.TypeFor[input]().Field(0)
	enumeration, err := choice.ParseEnum(field.Tag, field.Type)
	if err != nil {
		t.Fatal(err)
	}
	original := EnumInputSchema(enumeration, true)
	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var restored spec.WireSchema
	if err = json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	number, ok := restored.AnyOf[0].Enum[0].(json.Number)
	if !ok || number.String() != "9007199254740993" {
		t.Fatalf("integer identity lost: %v", restored.AnyOf[0].Enum)
	}
	patternIndex := -1
	for i, child := range restored.AnyOf {
		if child.MinLength != nil {
			patternIndex = i
			break
		}
	}
	if patternIndex < 0 {
		t.Fatal("missing string pattern length")
	}
	clone := restored.Clone()
	clone.AnyOf[0].Enum[0] = json.Number("1")
	*clone.AnyOf[patternIndex].MinLength = 0
	if restored.AnyOf[0].Enum[0] != number || *restored.AnyOf[patternIndex].MinLength != 4 {
		t.Fatal("clone mutated source")
	}
}
