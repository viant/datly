// Package auth configures the original Datly JWT verification contract for
// explicitly declared input codecs. It never injects an ambient principal.
package auth

import (
	"context"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	jwtv5 "github.com/golang-jwt/jwt/v5"
	"github.com/viant/datly/internal/logging"
	"github.com/viant/scy"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/scy/auth/jwt/verifier"
	xcodec "github.com/viant/xdatly/codec"
	handlerexec "github.com/viant/xdatly/handler/exec"
)

const JwtClaim = "JwtClaim"

// Config retains the original Datly JWTValidator configuration, including
// CertURL and RSA public-key resources. Verification policy stays with Scy.
type Config struct {
	JWTValidator *verifier.Config
	ClaimPolicy  *ClaimPolicy
	// RetainFailedCredential enables deliberate legacy error-response parity.
	// The credential is private to VerificationFailure and never formatted.
	RetainFailedCredential bool
	// InternalWarmupJWT enables the embedded RSA credential for cache warmup only.
	InternalWarmupJWT bool
	// InternalWarmupClaims adds trusted application claims to the warmup-only JWT.
	InternalWarmupClaims map[string]any
}

// ClaimPolicy binds a successfully verified JWT to the intended issuer,
// audience and principal. It is optional so existing Datly deployments retain
// their current verifier contract until they opt in.
type ClaimPolicy struct {
	Issuer         string `json:"Issuer,omitempty" yaml:"Issuer,omitempty"`
	Audience       string `json:"Audience,omitempty" yaml:"Audience,omitempty"`
	RequireSubject bool   `json:"RequireSubject,omitempty" yaml:"RequireSubject,omitempty"`
}

// Service is an application-configured codec factory. Pass it through the
// existing bootstrap CodecFactory input; only declared JwtClaim inputs use it.
type Service struct {
	verifier               *verifier.Service
	warmupVerifier         *verifier.Service
	warmupSigner           *rsa.PrivateKey
	warmupClaimsJSON       []byte
	policy                 ClaimPolicy
	retainFailedCredential bool
}

func New(ctx context.Context, config *Config) (*Service, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if config == nil || config.JWTValidator == nil {
		return nil, fmt.Errorf("JWTValidator configuration is required")
	}
	var policy ClaimPolicy
	if config.ClaimPolicy != nil {
		policy = *config.ClaimPolicy
		policy.Issuer, policy.Audience = strings.TrimSpace(policy.Issuer), strings.TrimSpace(policy.Audience)
		if policy.Issuer == "" && policy.Audience == "" && !policy.RequireSubject {
			return nil, fmt.Errorf("JWT claim policy requires issuer, audience or subject binding")
		}
	}
	// The verifier builds its immutable key profiles during Init. Copy its
	// configuration value so later replacement of CertURL cannot retarget it.
	native := *config.JWTValidator
	service := verifier.New(&native)
	if err := service.Init(ctx); err != nil {
		return nil, fmt.Errorf("initialize JWTValidator: %w", err)
	}
	result := &Service{verifier: service, policy: policy, retainFailedCredential: config.RetainFailedCredential}
	if len(config.InternalWarmupClaims) > 0 && !config.InternalWarmupJWT {
		return nil, fmt.Errorf("internal warmup JWT claims require InternalWarmupJWT")
	}
	if config.InternalWarmupJWT {
		for name, value := range config.InternalWarmupClaims {
			if strings.TrimSpace(name) == "" {
				return nil, fmt.Errorf("internal warmup JWT claim name is required")
			}
			switch strings.ToLower(name) {
			case "exp", "iat", "nbf", "iss", "aud":
				return nil, fmt.Errorf("internal warmup JWT claim %q is reserved", name)
			case "sub":
				if subject, ok := value.(string); !ok || strings.TrimSpace(subject) == "" {
					return nil, fmt.Errorf("internal warmup JWT subject must be a nonempty string")
				}
			}
		}
		if len(config.InternalWarmupClaims) > 0 {
			claimsJSON, err := json.Marshal(config.InternalWarmupClaims)
			if err != nil {
				return nil, fmt.Errorf("encode internal warmup JWT claims: %w", err)
			}
			result.warmupClaimsJSON = claimsJSON
		}
		key, public, err := internalWarmupKey()
		if err != nil {
			return nil, err
		}
		internal := verifier.New(&verifier.Config{RSA: []*scy.Resource{{URL: "datly-internal-warmup", Data: public}}})
		if err := internal.Init(ctx); err != nil {
			return nil, fmt.Errorf("initialize internal warmup JWT verifier: %w", err)
		}
		result.warmupSigner, result.warmupVerifier = key, internal
	}
	return result, nil
}

// WarmupCredential creates a short-lived credential accepted only while the
// canonical component engine is executing a server-owned warmup phase.
func (s *Service) WarmupCredential(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if s == nil || s.warmupSigner == nil {
		return "", fmt.Errorf("internal warmup JWT is not enabled")
	}
	expires := time.Now().Add(time.Minute)
	if deadline, ok := ctx.Deadline(); ok && deadline.After(expires) {
		expires = deadline.Add(time.Minute)
	}
	claims := jwtv5.MapClaims{}
	if len(s.warmupClaimsJSON) > 0 {
		if err := json.Unmarshal(s.warmupClaimsJSON, &claims); err != nil {
			return "", fmt.Errorf("decode internal warmup JWT claims: %w", err)
		}
	}
	if _, ok := claims["sub"]; !ok {
		claims["sub"] = "datly-internal-warmup"
	}
	claims["exp"] = expires.Unix()
	if s.policy.Issuer != "" {
		claims["iss"] = s.policy.Issuer
	}
	if s.policy.Audience != "" {
		claims["aud"] = s.policy.Audience
	}
	token, err := jwtv5.NewWithClaims(jwtv5.SigningMethodRS256, claims).SignedString(s.warmupSigner)
	if err != nil {
		return "", err
	}
	return "Bearer " + token, nil
}

func (s *Service) New(config *xcodec.Config, _ ...xcodec.Option) (xcodec.Instance, error) {
	if config == nil {
		return nil, fmt.Errorf("expected %s codec configuration", JwtClaim)
	}
	if !strings.EqualFold(strings.TrimSpace(config.Body), JwtClaim) {
		return nil, fmt.Errorf("codec %q is not registered", config.Body)
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
	return &claimsCodec{verifier: s.verifier, warmupVerifier: s.warmupVerifier, policy: s.policy, retainFailedCredential: s.retainFailedCredential}, nil
}

type claimsCodec struct {
	verifier               *verifier.Service
	warmupVerifier         *verifier.Service
	policy                 ClaimPolicy
	retainFailedCredential bool
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
	var claims *jwt.Claims
	var err error
	if c.warmupVerifier != nil && handlerexec.IsCacheWarmup(ctx) {
		claims, err = c.warmupVerifier.VerifyClaims(ctx, parts[0])
	}
	if claims == nil {
		claims, err = c.verifier.VerifyClaims(ctx, parts[0])
	}
	if err != nil {
		if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("verify JWT: %w", err)
		}
		failure := &VerificationFailure{cause: err}
		if c.retainFailedCredential {
			failure.credential, failure.retained = value, true
		}
		return nil, failure
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if claims == nil {
		return nil, fmt.Errorf("JWTValidator returned no claims")
	}
	if c.policy.Issuer != "" && claims.Issuer != c.policy.Issuer {
		return nil, fmt.Errorf("JWT issuer does not match the configured policy")
	}
	if c.policy.Audience != "" && !claims.VerifyAudience(c.policy.Audience, true) {
		return nil, fmt.Errorf("JWT audience does not match the configured policy")
	}
	if c.policy.RequireSubject && strings.TrimSpace(claims.Subject) == "" {
		return nil, fmt.Errorf("JWT subject is required by the configured policy")
	}
	logging.ObserveIdentity(ctx, logging.Identity{UserID: claims.UserID, Username: claims.Username, Email: claims.Email, Scope: claims.Scope})
	return claims, nil
}
