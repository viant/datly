package parser

import (
	"context"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/afs/url"
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
	Methods        []reflect.Method
	ForceGoTypeUse bool
}

type TypeImports []*TypeImport

func (t *TypeImport) EnsureLocation(ctx context.Context, fs afs.Service, rootPath, goModuleLocation string) {
	if !url.IsRelative(t.URL) {
		return
	}
	currentDir, _ := os.Getwd()
	if ok, _ := fs.Exists(ctx, path.Join(currentDir, t.URL)); ok {
		t.URL = url.Join(currentDir, t.URL)
	} else {

		parts := strings.Split(t.URL, "/")
		if strings.HasSuffix(goModuleLocation, parts[0]) { //backward compatiblity
			candidateURL := url.Join(goModuleLocation, parts[1:]...)
			if ok, _ := fs.Exists(ctx, candidateURL); ok {
				t.URL = candidateURL
				return
			}
		}
		candidateURL := url.Join(currentDir, "pkg", t.URL)
		if ok, _ := fs.Exists(ctx, candidateURL); ok { //backward compatiblity
			t.URL = candidateURL
			return
		}
		candidateURL = url.Join(rootPath, t.URL)
		if ok, _ := fs.Exists(ctx, candidateURL); ok { //backward compatiblity
			t.URL = candidateURL
			return
		}
		relPathParts := strings.Split(goModuleLocation, "/")
		relPath := strings.Join(relPathParts[:len(relPathParts)-2], "/")
		candidateURL = url.Join(relPath, t.URL)
		if ok, _ := fs.Exists(ctx, candidateURL); ok { //backward compatiblity
			t.URL = candidateURL
			return
		}

		if !strings.HasSuffix(goModuleLocation, t.URL) {
			t.URL = url.Join(goModuleLocation, t.URL)
		} else {
			t.URL = goModuleLocation
		}
	}
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
func ParseImports(ctx context.Context, expr *string, handler func(ctx context.Context, spec *TypeImport) error) error {
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
	case doubleQuotedToken:
		text := matched.Text(cursor)
		importSpec, err := parseTypeSrc(text[1:len(text)-1], cursor)
		if err != nil {
			return err
		}
		return handler(ctx, importSpec)
	case exprGroupToken:
		exprContent := matched.Text(cursor)
		exprGroupCursor := parsly.NewCursor("", []byte(exprContent[1:len(exprContent)-1]), 0)

		for {

			matched = exprGroupCursor.MatchAfterOptional(whitespaceMatcher, quotedMatcher)
			switch matched.Code {
			case doubleQuotedToken:
				text := matched.Text(exprGroupCursor)
				importSpec, err := parseTypeSrc(text[1:len(text)-1], exprGroupCursor)
				if err != nil {
					return err
				}
				if err = handler(ctx, importSpec); err != nil {
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
		if matched.Code != doubleQuotedToken {
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
