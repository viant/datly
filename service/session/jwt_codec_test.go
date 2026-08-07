package session

import (
	"context"
	"testing"
	"time"

	jwtv5 "github.com/golang-jwt/jwt/v5"
	"github.com/viant/datly/service/auth"
	"github.com/viant/datly/service/auth/config"
	"github.com/viant/datly/service/auth/mock"
	dcodec "github.com/viant/datly/view/extension/codec"
	jwtclaims "github.com/viant/scy/auth/jwt"
	"github.com/viant/scy/auth/jwt/signer"
	"github.com/viant/scy/auth/jwt/verifier"
	_ "github.com/viant/scy/kms/blowfish"
)

func TestSessionCodecOptionsUseConfiguredVerifierRules(t *testing.T) {
	ctx := context.Background()
	hmacSigner := mock.HmacJwtSigner()
	signerService := signer.New(&signer.Config{Rules: []*signer.Rule{{
		Resource:  []string{"mcp"},
		Algorithm: "HS256",
		HMAC:      hmacSigner.HMAC,
	}}})
	if err := signerService.Init(ctx); err != nil {
		t.Fatal(err)
	}
	verifierConfig := &verifier.Config{Rules: []*verifier.Rule{{
		Resource:  []string{"mcp"},
		Algorithm: "HS256",
		HMAC:      mock.HmacJwtVerifier().HMAC,
	}}}
	authService := auth.New(&config.Config{JWTValidator: verifierConfig})
	if err := authService.Init(ctx); err != nil {
		t.Fatal(err)
	}
	token, err := signerService.Create(time.Hour, &jwtclaims.Claims{
		UserID: 73,
		RegisteredClaims: jwtv5.RegisteredClaims{
			Audience: jwtv5.ClaimStrings{"mcp"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	options := NewOptions(WithAuth(authService))
	actual, err := (&dcodec.JwtClaim{}).Value(ctx, token, options.codecOptionsWithAuth()...)
	if err != nil {
		t.Fatalf("JwtClaim.Value() failed to use session verifier: %v", err)
	}
	claims, ok := actual.(*jwtclaims.Claims)
	if !ok || claims.UserID != 73 {
		t.Fatalf("claims=%T %+v", actual, actual)
	}
}
