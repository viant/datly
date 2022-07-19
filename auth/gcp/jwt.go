package gcp

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/viant/scy/auth/gcp"
	"strings"
)

//JwtClaim represents IDJWT visitor
type JwtClaim struct{}

func (j *JwtClaim) Value(ctx context.Context, raw interface{}, options ...interface{}) (interface{}, error) {
	rawString, ok := raw.(string)
	if !ok {
		return nil, fmt.Errorf("expected to get string but got %T", raw)
	}
	if last := strings.LastIndexByte(rawString, ' '); last != -1 {
		rawString = rawString[last+1:]
	}
	data := rawString
	if decoded, err := base64.StdEncoding.DecodeString(rawString); err == nil {
		data = string(decoded)
	}
	info, err := gcp.JwtClaims(ctx, data)
	if err != nil {
		return nil, err
	}
	return info, nil
}
