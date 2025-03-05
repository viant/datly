package handler

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/xdatly/handler"
	"github.com/viant/xunsafe"
	"io"
	"reflect"
	"strings"
)

const (
	LoadDelimitedDataHandler = "LoadDelimitedData"
)

type (
	LoadDelimitedDataProvider struct{}

	LoadDelimitedData struct {
		fs afs.Service
		*handler.Options
	}
)

// Exec executes handler
func (l *LoadDelimitedData) Exec(ctx context.Context, session handler.Session) (interface{}, error) {
	if len(l.Arguments) != 2 {
		return nil, fmt.Errorf("invalid Delimited Loader arguments: %v, expected URL and delimiter", l.Arguments)
	}

	URL, err := getValue(ctx, session, l.Arguments[0])
	if err != nil {
		return nil, fmt.Errorf("invalid Delimited Loader URL: %w", err)
	}
	delimiterArg, err := getValue(ctx, session, l.Arguments[1])
	if err != nil {
		return nil, fmt.Errorf("invalid Delimited Loader delimiter: %w", err)
	}
	delimiter, err := convertDelimiter(delimiterArg)
	if err != nil {
		return nil, fmt.Errorf("invalid Delimited Loader delimiter: %w", err)
	}

	if ok, _ := l.fs.Exists(ctx, URL); !ok {
		if ok, _ := l.fs.Exists(ctx, URL+".gz"); ok {
			URL += ".gz"
		}
	}

	isCompressed := strings.HasSuffix(URL, ".gz")
	data, err := l.fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return nil, fmt.Errorf("failed to load URL: %w", err)
	}
	if isCompressed {
		reader, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("failed to decompress URL: failed to create reader: %w (used URL: %s)", err, URL)
		}
		defer reader.Close()
		if data, err = io.ReadAll(reader); err != nil {
			return nil, fmt.Errorf("failed to decompress URL:%w (used URL: %s)", err, URL)
		}
	}
	xSlice := xunsafe.NewSlice(l.Options.OutputType)
	response := reflect.New(l.Options.OutputType).Interface()
	appender := xSlice.Appender(xunsafe.AsPointer(response))

	reader := csv.NewReader(bytes.NewReader(data))
	reader.Comma = delimiter
	reader.TrimLeadingSpace = true
	for {
		fields, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			// Some other error occurred (e.g. malformed CSV)
			return nil, fmt.Errorf("failed to read record: %w", err)
		}
		appender.Append(fields)
	}
	return response, nil
}

func getValue(ctx context.Context, session handler.Session, arg string) (string, error) {
	value, ok, err := session.Stater().Value(ctx, arg)
	if !ok || err != nil {
		return "", err
	}
	var result string
	switch value.(type) {
	case string:
		result = value.(string)
	case *string:
		result = *value.(*string)
	default:
		return "", fmt.Errorf("invalid type: expected %T, but had %T", result, value)
	}
	return result, nil
}

func (*LoadDelimitedDataProvider) New(ctx context.Context, opts ...handler.Option) (handler.Handler, error) {
	options := handler.NewOptions(opts)
	return &LoadDelimitedData{Options: options, fs: afs.New()}, nil
}

func convertDelimiter(s string) (rune, error) {
	switch s {
	case "\\t":
		return '\t', nil
	case "\\n":
		return '\n', nil
	case "\\r":
		return '\r', nil
	default:
		if len(s) == 1 {
			return rune(s[0]), nil
		}
		return 0, fmt.Errorf("invalid delimiter: %q", s)
	}
}
