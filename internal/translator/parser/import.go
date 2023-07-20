package parser

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/url"
	"github.com/viant/datly/view"
	"github.com/viant/parsly"
	"os"
	"path"
	"reflect"
	"strings"
)

type TypeImport struct {
	URL            string
	Types          []string
	Alias          string
	Definition     []*view.TypeDefinition
	Methods        []reflect.Method
	ForceGoTypeUse bool
}

type TypeImports []*TypeImport

func (t *TypeImport) EnsureLocation(ctx context.Context, fs afs.Service, goModuleLocation string) {
	if !url.IsRelative(t.URL) {
		return
	}
	currentDir, _ := os.Getwd()
	if ok, _ := fs.Exists(ctx, path.Join(currentDir, t.URL)); ok {
		t.URL = url.Join(currentDir, t.URL)
	} else {
		t.URL = url.Join(goModuleLocation, t.URL)
	}
}

func (t *TypeImport) AppendTypeDefinition(definition *view.TypeDefinition) {
	t.Definition = append(t.Definition, definition)
}

func (t TypeImports) CustomTypeURL() string {
	for _, candidate := range t {
		if len(candidate.Methods) > 0 {
			return candidate.URL
		}
	}
	return ""
}

func (t TypeImports) Lookup(typeName string) *TypeImport {
	if strings.HasPrefix(typeName, "[]") {
		typeName = typeName[2:]
	}
	if strings.HasPrefix(typeName, "*") {
		typeName = typeName[1:]
	}
	for _, item := range t {
		for _, candidate := range item.Types {
			if candidate == typeName || typeName == item.Alias {
				return item
			}
		}
	}
	return nil
}

func (t *TypeImports) Append(spec *TypeImport) {
	*t = append(*t, spec)
}

// ParseImports parse go types import statement
func ParseImports(expr *string, handler func(spec *TypeImport) error) error {
	cursor := parsly.NewCursor("", []byte(*expr), 0)
	defer func() {
		*expr = strings.TrimSpace((*expr)[cursor.Pos:])
	}()

	matched := cursor.MatchAfterOptional(whitespaceMatcher, importKeywordMatcher)
	if matched.Code != importKeywordToken {
		return nil
	}

	matched = cursor.MatchAfterOptional(whitespaceMatcher, exprGroupMatcher, quotedMatcher)
	switch matched.Code {
	case quotedToken:
		text := matched.Text(cursor)
		importSpec, err := parseTypeSrc(text[1:len(text)-1], cursor)
		if err != nil {
			return err
		}
		return handler(importSpec)
	case exprGroupToken:
		exprContent := matched.Text(cursor)
		exprGroupCursor := parsly.NewCursor("", []byte(exprContent[1:len(exprContent)-1]), 0)

		for {

			matched = exprGroupCursor.MatchAfterOptional(whitespaceMatcher, quotedMatcher)
			switch matched.Code {
			case quotedToken:
				text := matched.Text(exprGroupCursor)
				importSpec, err := parseTypeSrc(text[1:len(text)-1], exprGroupCursor)
				if err != nil {
					return err
				}
				if err = handler(importSpec); err != nil {
					return err
				}
			case parsly.EOF:
				return nil
			default:
				return cursor.NewError(quotedMatcher)
			}
		}
	}
	return nil
}

func parseTypeSrc(imported string, cursor *parsly.Cursor) (*TypeImport, error) {
	var alias string
	matched := cursor.MatchAfterOptional(whitespaceMatcher, aliasKeywordMatcher)
	if matched.Code == aliasKeywordToken {
		matched = cursor.MatchAfterOptional(whitespaceMatcher, quotedMatcher)
		if matched.Code != quotedToken {
			return nil, cursor.NewError(quotedMatcher)
		}
		alias = strings.Trim(matched.Text(cursor), "\"")
	}

	index := strings.LastIndex(imported, ".")
	if index == -1 {
		return nil, fmt.Errorf(`unsupported import format: %v, supported: "[path].[type]"`, imported)
	}

	return &TypeImport{
		URL:   imported[:index],
		Types: []string{imported[index+1:]},
		Alias: alias,
	}, nil
}
