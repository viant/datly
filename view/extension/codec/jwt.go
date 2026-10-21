package codec

import (
	"context"
	"encoding/base64"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/viant/scy/auth/gcp"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/scy/auth/jwt/verifier"
	"github.com/viant/xdatly/codec"
)

const (
	KeyJwtClaim = "JwtClaim"
)

// JwtClaim represents IDJWT visitor
type (
	JwtEntry struct {
		Token string
		Email string
		*jwt.Claims
	}

	JwtCache struct {
		entries map[jwtCacheKey]*JwtEntry
		mux     sync.RWMutex
	}

	JwtClaim struct{}

	jwtCacheKey struct {
		token    string
		verifier *verifier.Service
	}

	jwtVerifierOption struct {
		service *verifier.Service
	}
)

// WithJWTVerifier makes the session-configured verifier available to JwtClaim.
// It appends to the codec's untyped extension options without replacing other
// options installed by the view/session pipeline.
func WithJWTVerifier(service *verifier.Service) codec.Option {
	return func(options *codec.Options) {
		if service != nil {
			options.Options = append(options.Options, &jwtVerifierOption{service: service})
		}
	}
}

// Put retains the legacy GCP-cache API. Configured verifier paths use the
// verifier-scoped private variant below.
func (j *JwtCache) Put(token string, claims *jwt.Claims) {
	j.put(token, nil, claims)
}

func (j *JwtCache) put(token string, service *verifier.Service, claims *jwt.Claims) {
	if claims == nil || !claims.VerifyExpiresAt(time.Now(), true) {
		return
	}
	j.mux.Lock()
	defer j.mux.Unlock()
	if len(j.entries) > 100 {
		j.entries = map[jwtCacheKey]*JwtEntry{}
	}
	j.entries[jwtCacheKey{token: token, verifier: service}] = &JwtEntry{Token: token, Claims: claims}
}

// Lookup retains the legacy GCP-cache API.
func (j *JwtCache) Lookup(token string) *jwt.Claims {
	return j.lookup(token, nil)
}

func (j *JwtCache) lookup(token string, service *verifier.Service) *jwt.Claims {
	key := jwtCacheKey{token: token, verifier: service}
	j.mux.RLock()
	entry, ok := j.entries[key]
	j.mux.RUnlock()
	if !ok {
		return nil
	}
	if entry.Claims.VerifyExpiresAt(time.Now(), true) {
		return entry.Claims
	}
	j.mux.Lock()
	delete(j.entries, key)
	defer j.mux.Unlock()
	return nil
}

var jwtCache = &JwtCache{entries: map[jwtCacheKey]*JwtEntry{}}

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
	service := jwtVerifier(options)
	if claim := jwtCache.lookup(data, service); claim != nil {
		return claim, nil
	}
	var info *jwt.Claims
	var err error
	if service != nil {
		info, err = service.VerifyClaims(ctx, data)
	} else {
		info, err = gcp.JwtClaims(ctx, data)
	}
	if err != nil {
		return nil, err
	}
	jwtCache.put(data, service, info)
	return info, nil
}

func jwtVerifier(options []codec.Option) *verifier.Service {
	codecOptions := codec.NewOptions(options)
	for index := len(codecOptions.Options) - 1; index >= 0; index-- {
		if candidate, ok := codecOptions.Options[index].(*jwtVerifierOption); ok {
			return candidate.service
		}
	}
	return nil
}
