package xlsbody

import (
	"context"
	"reflect"
	"testing"

	"github.com/viant/tagly/tags"
	"github.com/viant/x/shape"
)

type testContextKey struct{}
type decodeRecord struct{ calls int }
type representedWorkbook struct {
	Code    int               `json:"code" mcpEnum:"true" choice:"0:OFF" choice:"1:ON" enumAccept:"literal" enumNull:"reject" validate:"required"`
	Child   *representedChild `json:"child"`
	private *decodeRecord
}

func (w *representedWorkbook) UnmarshalXLS(ctx context.Context, raw []byte) error {
	record := ctx.Value(testContextKey{}).(*decodeRecord)
	record.calls++
	w.private = record
	w.Code = len(raw)
	return nil
}
func TestDecoderRepresentation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		edit  func(*shape.FieldMetadata)
		valid bool
	}{
		{"unchanged", nil, true},
		{"storage enums", func(f *shape.FieldMetadata) {
			if f.Name == "Code" {
				parsed, _ := tags.Parse(string(f.Tag))
				kept := tags.Tags{}
				for _, tag := range parsed {
					if !storageEnumTag(tag.Name) {
						kept = append(kept, tag)
					}
				}
				f.Tag = reflect.StructTag(kept.Literal())
			}
		}, true},
		{"hidden", func(f *shape.FieldMetadata) {
			if f.Name == "Code" {
				f.Tag = `json:"-"`
			}
		}, false},
		{"renamed", func(f *shape.FieldMetadata) {
			if f.Name == "Code" {
				f.Name = "Other"
			}
		}, false},
		{"required", func(f *shape.FieldMetadata) {
			if f.Name == "Code" {
				f.Tag = `json:"code" mcpEnum:"true" validate:"optional"`
			}
		}, false},
		{"narrow choice", func(f *shape.FieldMetadata) {
			if f.Name == "Code" {
				f.Tag = `json:"code" mcpEnum:"true" choice:"0:OFF" enumAccept:"literal" enumNull:"reject" validate:"required"`
			}
		}, false},
		{"changed null", func(f *shape.FieldMetadata) {
			if f.Name == "Code" {
				f.Tag = `json:"code" mcpEnum:"true" choice:"0:OFF" choice:"1:ON" enumAccept:"literal" enumNull:"allow" validate:"required"`
			}
		}, false},
		{"partial removal", func(f *shape.FieldMetadata) {
			if f.Name == "Code" {
				f.Tag = `json:"code" mcpEnum:"true" enumNull:"reject" validate:"required"`
			}
		}, false},
		{"unmanaged", func(f *shape.FieldMetadata) {
			if f.Name == "Child" {
				f.Tag = `json:"-"`
			}
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, typ := range []reflect.Type{reflect.TypeFor[representedWorkbook](), reflect.TypeFor[*representedWorkbook]()} {
				plan, err := shape.CompileRewrite(typ)
				if err != nil {
					t.Fatal(err)
				}
				metadata, err := plan.RewriteRepresentation(func(_ string, _ reflect.Type, f *shape.FieldMetadata) (*shape.FieldMetadata, error) {
					if tc.edit == nil {
						return nil, nil
					}
					tc.edit(f)
					return f, nil
				})
				if err != nil {
					t.Fatal(err)
				}
				record := &decodeRecord{}
				decoder := Decoder{Context: context.WithValue(t.Context(), testContextKey{}, record)}
				target := reflect.New(typ)
				err = decoder.UnmarshalWithRepresentation([]byte("book"), target.Interface(), metadata)
				if (err == nil) != tc.valid {
					t.Fatalf("valid=%v err=%v", tc.valid, err)
				}
				if !tc.valid {
					if record.calls != 0 {
						t.Fatal("decoder invoked on unsupported policy")
					}
					continue
				}
				value := target
				for value.Kind() == reflect.Pointer {
					value = value.Elem()
				}
				actual := value.Addr().Interface().(*representedWorkbook)
				if record.calls != 1 || actual.private != record || actual.Code != 4 {
					t.Fatal("canonical state/context lost")
				}
			}
		})
	}
}
func TestDecoderRepresentationCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := (Decoder{Context: ctx}).UnmarshalWithRepresentation(nil, &representedWorkbook{}, nil); err != context.Canceled {
		t.Fatalf("got %v", err)
	}
}

type representedChild struct {
	Code int `json:"code" mcpEnum:"true" choice:"0:OFF" enumAccept:"literal"`
}
type recursiveWorkbook struct{ Next *recursiveWorkbook }

func TestRecursiveUnchangedRepresentation(t *testing.T) {
	plan, err := shape.CompileRewrite(reflect.TypeFor[recursiveWorkbook]())
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := plan.RewriteRepresentation(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateRepresentation(metadata, make(map[*shape.Representation]bool)); err != nil {
		t.Fatal(err)
	}
}

func TestDecoderRejectsWrongCanonicalTarget(t *testing.T) {
	plan, err := shape.CompileRewrite(reflect.TypeFor[*representedWorkbook]())
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := plan.RewriteRepresentation(nil)
	if err != nil {
		t.Fatal(err)
	}
	record := &decodeRecord{}
	decoder := Decoder{Context: context.WithValue(t.Context(), testContextKey{}, record)}
	if err := decoder.UnmarshalWithRepresentation([]byte("book"), &representedWorkbook{}, metadata); err == nil {
		t.Fatal("wrong canonical target accepted")
	}
	if record.calls != 0 {
		t.Fatal("decoder invoked on mismatched target")
	}
}
