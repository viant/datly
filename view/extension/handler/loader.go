package handler

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/viant/afs"
	"github.com/viant/datly/utils/types"
	"github.com/viant/xdatly/handler"
	"github.com/viant/xunsafe"
	"reflect"
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
	switch URLValue.(type) {
	case string:
		URL = URLValue.(string)
	case *string:
		URL = *URLValue.(*string)
	default:
		return nil, fmt.Errorf("invalid Loader URL: expected %T, but had %T", URL, URLValue)
	}
	data, err := l.fs.DownloadWithURL(ctx, URL)
	if err != nil {
		return nil, fmt.Errorf("failed to load URL: %w", err)
	}
	itemType := l.Options.OutputType.Elem()
	xSlice := xunsafe.NewSlice(l.Options.OutputType)
	scanner := bufio.NewScanner(bytes.NewReader(data))
	response := reflect.New(l.Options.OutputType).Interface()
	appender := xSlice.Appender(xunsafe.AsPointer(response))
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		item := types.NewValue(itemType)
		err := json.Unmarshal(scanner.Bytes(), item)
		if err != nil {
			return nil, fmt.Errorf("invalid item: %w, %s", err, line)
		}
		appender.Append(item)
	}
	return response, nil
}

func (*LoadDataProvider) New(ctx context.Context, opts ...handler.Option) (handler.Handler, error) {
	options := handler.NewOptions(opts)
	return &LoadData{Options: options, fs: afs.New()}, nil
}
