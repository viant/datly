// Package loaddata provides an ordinary component handler for typed AFS data.
// Configuration and the storage capability use normal handler input DI.
package loaddata

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/viant/afs/storage"
	xhandler "github.com/viant/xdatly/handler"
)

type Config struct {
	URL         string `json:"url"`
	Format      string `json:"format"`
	Compression string `json:"compression,omitempty"`
	// MaxBytes bounds decoded bytes, including decompressed data. Zero is unlimited.
	MaxBytes int64 `json:"maxBytes,omitempty"`
}

type Input struct {
	Config  *Config        `parameter:"Config,kind=const,in=LoadData,required"`
	Storage storage.Opener `bind:"kind=afs,required"`
}

func (i *Input) Init(context.Context) error {
	if i.Config == nil || i.Config.URL == "" || i.Storage == nil {
		return fmt.Errorf("load data configuration, URL and storage are required")
	}
	switch i.Config.Format {
	case "json", "json-array", "ndjson":
	default:
		return fmt.Errorf("load data format must be json, json-array or ndjson")
	}
	if i.Config.Compression != "" && i.Config.Compression != "none" && i.Config.Compression != "gzip" {
		return fmt.Errorf("load data compression must be none or gzip")
	}
	if i.Config.MaxBytes < 0 {
		return fmt.Errorf("load data maxBytes must not be negative")
	}
	return nil
}

type Output[T any] struct{ Data []T }
type Handler[T any] struct{}

func New[T any]() xhandler.Contract[Input, Output[T]] { return &Handler[T]{} }

func (*Handler[T]) Exec(ctx context.Context, _ xhandler.Session, input *Input, output *Output[T]) error {
	reader, err := input.Storage.OpenURL(ctx, input.Config.URL)
	if err != nil {
		return err
	}
	defer reader.Close()
	var source io.Reader = reader
	if input.Config.Compression == "gzip" {
		compressed, err := gzip.NewReader(reader)
		if err != nil {
			return fmt.Errorf("load data gzip: %w", err)
		}
		defer compressed.Close()
		source = compressed
	}
	decoder := json.NewDecoder(&boundedReader{ctx: ctx, source: source, limit: input.Config.MaxBytes})
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	var rows []T
	switch input.Config.Format {
	case "json":
		var row T
		if err := decoder.Decode(&row); err != nil {
			return fmt.Errorf("load JSON: %w", err)
		}
		rows = append(rows, row)
	case "json-array":
		if err := decoder.Decode(&rows); err != nil {
			return fmt.Errorf("load JSON array: %w", err)
		}
	case "ndjson":
		for {
			var row T
			err := decoder.Decode(&row)
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("load NDJSON: %w", err)
			}
			rows = append(rows, row)
		}
	}
	if input.Config.Format != "ndjson" {
		var extra any
		if err := decoder.Decode(&extra); err != io.EOF {
			if err == nil {
				return fmt.Errorf("load data contains trailing JSON")
			}
			return fmt.Errorf("load data trailing content: %w", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	output.Data = rows
	return nil
}

type boundedReader struct {
	ctx         context.Context
	source      io.Reader
	limit, read int64
}

func (r *boundedReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if r.limit > 0 {
		remaining := r.limit - r.read
		if remaining < 0 {
			return 0, fmt.Errorf("load data exceeds %d bytes", r.limit)
		}
		if remaining < int64(len(p)) {
			p = p[:remaining+1]
		}
	}
	n, err := r.source.Read(p)
	r.read += int64(n)
	if r.limit > 0 && r.read > r.limit {
		return 0, fmt.Errorf("load data exceeds %d bytes", r.limit)
	}
	return n, err
}
