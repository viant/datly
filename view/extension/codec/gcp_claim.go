package codec

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/viant/scy/auth/gcp"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/xdatly/codec"
	"reflect"
	"strings"
	"sync"
	"time"
)

const (
	CognitoKeyJwtClaim = "CognitoJwtClaim"
	KeyJwtClaim        = "JwtClaim"
)

// JwtClaim represents IDJWT visitor
type (
	JwtEntry struct {
		Token string
		Email string
		*jwt.Claims
	}

	JwtCache struct {
		entries map[string]*JwtEntry
		mux     sync.RWMutex
	}

	JwtClaim struct{}
)

func (j *JwtCache) Put(token string, claims *jwt.Claims) {
	if !claims.VerifyExpiresAt(time.Now(), true) {
		return
	}
	j.mux.Lock()
	defer j.mux.Unlock()
	if len(j.entries) > 100 {
		j.entries = map[string]*JwtEntry{}
	}
	j.entries[token] = &JwtEntry{Token: token, Claims: claims}
}

func (j *JwtCache) Lookup(token string) *jwt.Claims {
	j.mux.RLock()
	entry, ok := j.entries[token]
	j.mux.RUnlock()
	if !ok {
		return nil
	}
	if entry.Claims.VerifyExpiresAt(time.Now(), true) {
		return entry.Claims
	}
	j.mux.Lock()
	delete(j.entries, token)
	defer j.mux.Unlock()
	return nil
}

var jwtCache = &JwtCache{entries: map[string]*JwtEntry{}}

func (j *JwtClaim) ResultType(paramType reflect.Type) (reflect.Type, error) {
	return reflect.TypeOf(&jwt.Claims{}), nil
}

func (j *JwtClaim) Value(ctx context.Context, raw interface{}, options ...codec.Option) (interface{}, error) {
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
	if claim := jwtCache.Lookup(data); claim != nil {
		return claim, nil
	}
	info, err := gcp.JwtClaims(ctx, data)
	if err != nil {
		return nil, err
	}
	jwtCache.Put(data, info)
	return info, nil
}
