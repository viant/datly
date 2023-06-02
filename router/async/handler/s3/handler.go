package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/viant/afs"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/router/async"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Handler struct {
	bucketURL string
}

func (h *Handler) Handle(ctx context.Context, record *async.RecordWithHttp, request *http.Request) error {
	marshal, err := json.Marshal(record)
	if err != nil {
		return err
	}

	newUUID, err := uuid.NewUUID()
	if err != nil {
		return err
	}

	URL := url.Join(h.bucketURL, strings.ReplaceAll(newUUID.String(), "-", "")+strconv.Itoa(int(time.Now().UnixNano()))) + ".async"
	err = afs.New().Upload(ctx, URL, file.DefaultFileOsMode, bytes.NewReader(marshal))
	fmt.Printf("%v, %v\n", err, URL)

	return err
}

func NewHandler(ctx context.Context, bucketURL string) (async.Handler, error) {
	if bucketURL == "" {
		return nil, fmt.Errorf("BucketURL can't be empty")
	}

	return &Handler{
		bucketURL: bucketURL,
	}, nil
}
