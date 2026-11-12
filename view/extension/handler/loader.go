package handler

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/datly/utils/types"
	"github.com/viant/xdatly/handler"
	"github.com/viant/xunsafe"
	"io"
	"reflect"
	"strings"
)

const (
	LoadDataHandler = "LoadData"
)

type (
	LoadDataProvider struct{}

	LoadSource struct {
		URL string
	}

	LoadData struct {
		fs afs.Service
		*handler.Options
	}
)

// Exec executes handler
func (l *LoadData) Exec(ctx context.Context, session handler.Session) (interface{}, error) {
	if len(l.Arguments) == 0 {
		return nil, fmt.Errorf("invalid Loader arguments: %v, expected URL", l.Arguments)
	}
	URLValue, ok, err := session.Stater().Value(ctx, l.Arguments[0])
	if !ok || err != nil {
		return nil, fmt.Errorf("invalid Loader URL: %w", err)
	}

	var URL string
	switch v := URLValue.(type) {
	case string:
		URL = v
	case *string:
		URL = *v
	default:
		return nil, fmt.Errorf("invalid Loader URL: expected %T, but had %T", "", URLValue)
	}

	// Prefer .gz if the plain URL doesn't exist.
	if ok, _ := l.fs.Exists(ctx, URL); !ok {
		if ok, _ := l.fs.Exists(ctx, URL+".gz"); ok {
			URL += ".gz"
		}
	}

	// Download compressed or plain bytes (API returns []byte).
	data, err := l.fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return nil, fmt.Errorf("failed to load URL: %w", err)
	}

	// Build a streaming reader chain; avoid io.ReadAll on gzip.
	var r io.Reader = bytes.NewReader(data)
	if strings.HasSuffix(URL, ".gz") {
		gzr, err := gzip.NewReader(r)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress URL: failed to create reader: %w (used URL: %s)", err, URL)
		}
		defer gzr.Close()
		r = gzr
	}

	br := bufio.NewReaderSize(r, 1<<20) // read-ahead; does NOT cap JSON size
	dec := json.NewDecoder(br)
	dec.UseNumber()

	// Output slice + appender (kept from your original design)
	itemType := l.Options.OutputType.Elem()
	xSlice := xunsafe.NewSlice(l.Options.OutputType)
	response := reflect.New(l.Options.OutputType).Interface()
	appender := xSlice.Appender(xunsafe.AsPointer(response))

	// Reject top-level arrays to keep the code simple (no streaming array parsing).
	first, err := peekFirstNonSpace(br)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return response, nil // empty file -> empty slice
		}
		return nil, fmt.Errorf("read error: %w", err)
	}
	if first == '[' {
		return nil, fmt.Errorf("top-level JSON arrays are not supported; provide NDJSON (one object per line) or a single JSON object")
	}
	// Put the byte back so the decoder sees it.
	_ = br.UnreadByte()

	// Decode one value per call: supports single object or NDJSON.
	for {
		item := types.NewValue(itemType) // pointer to zero value of element type
		if err := dec.Decode(item); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("invalid item: %w", err)
		}
		appender.Append(item)
	}

	return response, nil
}

// Reads and returns the first non-space byte without consuming input for the decoder.
func peekFirstNonSpace(br *bufio.Reader) (byte, error) {
	for {
		b, err := br.ReadByte()
		if err != nil {
			return 0, err
		}
		if b == ' ' || b == '\n' || b == '\r' || b == '\t' {
			continue
		}
		return b, nil
	}
}

func (*LoadDataProvider) New(ctx context.Context, opts ...handler.Option) (handler.Handler, error) {
	options := handler.NewOptions(opts)
	return &LoadData{Options: options, fs: afs.New()}, nil
}
