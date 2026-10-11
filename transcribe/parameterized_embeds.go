package transcribe

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/viant/bindly/resource"
	dsql "github.com/viant/datly/sql"
	"github.com/viant/velty"
	"io/fs"
	"strings"
	"time"
)

// Stage each parameterized occurrence behind an ordinary resource token.
// DQL parsing retains its resource boundary, including deferred parent macros.
func prepareParameterizedEmbeds(ctx context.Context, source *Source) (*Source, []velty.Patch, error) {
	if !strings.Contains(source.Text, "${embed(") {
		return source, nil, nil
	}
	if source.Resources == nil {
		return nil, nil, fmt.Errorf("parameterized SQL embed requires resources")
	}
	files := map[string][]byte{}
	var patches []velty.Patch
	var text strings.Builder
	cursor := 0
	for {
		index := strings.Index(source.Text[cursor:], "${embed(")
		if index < 0 {
			text.WriteString(source.Text[cursor:])
			break
		}
		start := cursor + index
		fragment := source.Text[start:]
		decoder := json.NewDecoder(strings.NewReader(fragment[len("${embed("):]))
		var args json.RawMessage
		if err := decoder.Decode(&args); err != nil {
			return nil, nil, fmt.Errorf("SQL embed arguments: %w", err)
		}
		offset := len("${embed(") + int(decoder.InputOffset())
		for offset < len(fragment) && strings.ContainsRune(" \t\r\n", rune(fragment[offset])) {
			offset++
		}
		if !strings.HasPrefix(fragment[offset:], "):") {
			return nil, nil, fmt.Errorf("SQL embed arguments require closing parenthesis and resource path")
		}
		end := strings.IndexByte(fragment[offset+2:], '}')
		if end < 0 {
			return nil, nil, fmt.Errorf("SQL embed requires closing brace")
		}
		end += start + offset + 3
		token := source.Text[start:end]
		body, err := dsql.ExpandEmbeddedResources(ctx, token, source.Resources)
		if err != nil {
			return nil, nil, err
		}
		// Include the occurrence identity so equal bodies never conflate source spans.
		name := fmt.Sprintf("__datly_parameterized__/%x.sql", sha256.Sum256([]byte(fmt.Sprintf("%d:%s", start, token))))
		if existing, openErr := source.Resources.Open(name); openErr == nil {
			existing.Close()
			return nil, nil, fmt.Errorf("parameterized SQL resource name already exists: %s", name)
		}
		files[name] = []byte(body)
		replacement := "${embed:" + name + "}"
		text.WriteString(source.Text[cursor:start])
		text.WriteString(replacement)
		patches = append(patches, velty.Patch{Span: velty.Span{Start: start, End: end - 1}, Replacement: []byte(replacement)})
		cursor = end
	}
	staged, err := source.Resources.WithDefault(&parameterizedResources{files: files, fallback: source.Resources})
	if err != nil {
		return nil, nil, err
	}
	detached := *source
	detached.Text = text.String()
	detached.Resources = staged
	return &detached, patches, nil
}

type parameterizedResources struct {
	files    map[string][]byte
	fallback *resource.Store
}

func (s *parameterizedResources) Open(name string) (fs.File, error) {
	if body, ok := s.files[name]; ok {
		return &parameterizedFile{Reader: bytes.NewReader(body), name: name, size: int64(len(body))}, nil
	}
	return s.fallback.Open(name)
}

type parameterizedFile struct {
	*bytes.Reader
	name string
	size int64
}

func (f *parameterizedFile) Close() error { return nil }
func (f *parameterizedFile) Stat() (fs.FileInfo, error) {
	return parameterizedInfo{name: f.name, size: f.size}, nil
}

type parameterizedInfo struct {
	name string
	size int64
}

func (f parameterizedInfo) Name() string       { return f.name }
func (f parameterizedInfo) Size() int64        { return f.size }
func (f parameterizedInfo) Mode() fs.FileMode  { return 0 }
func (f parameterizedInfo) ModTime() time.Time { return time.Time{} }
func (f parameterizedInfo) IsDir() bool        { return false }
func (f parameterizedInfo) Sys() any           { return nil }
