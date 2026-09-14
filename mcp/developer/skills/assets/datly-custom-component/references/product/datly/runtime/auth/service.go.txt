// Package auth configures the original Datly JWT verification contract for
// explicitly declared input codecs. It never injects an ambient principal.
package auth

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/scy/auth/jwt"
	"github.com/viant/scy/auth/jwt/verifier"
	xcodec "github.com/viant/xdatly/codec"
)

const JwtClaim = "JwtClaim"

// Config retains the original Datly JWTValidator configuration, including
// CertURL and RSA public-key resources. Verification policy stays with Scy.
type Config struct {
	JWTValidator *verifier.Config
}

// Service is an application-configured codec factory. Pass it through the
// existing bootstrap CodecFactory input; only declared JwtClaim inputs use it.
type Service struct {
	verifier *verifier.Service
}

func New(ctx context.Context, config *Config) (*Service, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if config == nil || config.JWTValidator == nil {
		return nil, fmt.Errorf("JWTValidator configuration is required")
	}
	// The verifier builds its immutable key profiles during Init. Copy its
	// configuration value so later replacement of CertURL cannot retarget it.
	native := *config.JWTValidator
	service := verifier.New(&native)
	if err := service.Init(ctx); err != nil {
		return nil, fmt.Errorf("initialize JWTValidator: %w", err)
	}
	return &Service{verifier: service}, nil
}

func (s *Service) New(config *xcodec.Config, _ ...xcodec.Option) (xcodec.Instance, error) {
	if config == nil || !strings.EqualFold(strings.TrimSpace(config.Body), JwtClaim) {
		return nil, fmt.Errorf("expected %s codec configuration", JwtClaim)
	}
	if s == nil || s.verifier == nil {
		return nil, fmt.Errorf("JWTValidator is not initialized")
	}
	if config.SourceType != reflect.TypeFor[string]() || config.DestinationType != reflect.TypeFor[*jwt.Claims]() {
		return nil, fmt.Errorf("JwtClaim requires string input and *jwt.Claims destination")
	}
	if len(config.Args) != 0 {
		return nil, fmt.Errorf("JwtClaim does not accept transformation arguments")
	}
	return &claimsCodec{verifier: s.verifier}, nil
}

type claimsCodec struct {
	verifier *verifier.Service
}

func (c *claimsCodec) Value(ctx context.Context, raw any, _ ...xcodec.Option) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	value, ok := raw.(string)
	if !ok {
		return nil, fmt.Errorf("JwtClaim expected a string credential")
	}
	parts := strings.Fields(value)
	if len(parts) == 2 && strings.EqualFold(parts[0], "Bearer") {
		parts = parts[1:]
	}
	if len(parts) != 1 {
		return nil, fmt.Errorf("JwtClaim requires a token or Bearer credential")
	}
	claims, err := c.verifier.VerifyClaims(ctx, parts[0])
	if err != nil {
		return nil, fmt.Errorf("verify JWT: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if claims == nil {
		return nil, fmt.Errorf("JWTValidator returned no claims")
	}
	return claims, nil
}
