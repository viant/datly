package auth

import (
	"context"
	"crypto/rsa"
	_ "embed"
	"fmt"
	"sync"

	jwtv5 "github.com/golang-jwt/jwt/v5"
	"github.com/viant/scy"
	_ "github.com/viant/scy/kms/blowfish"
)

// These encrypted RSA resources come from original Datly's service/auth/mock/jwt.
// They are used only by the internal cache warmup credential path. The private
// key is shared by deployments of this build; the HTTP administrator gate and
// warmup-only codec scope remain the authorization boundaries.
//
//go:embed jwt/private.enc
var warmupPrivateEncrypted []byte

//go:embed jwt/public.enc
var warmupPublicEncrypted []byte

var embeddedWarmupKey struct {
	once    sync.Once
	private *rsa.PrivateKey
	public  []byte
	err     error
}

func internalWarmupKey() (*rsa.PrivateKey, []byte, error) {
	embeddedWarmupKey.once.Do(func() {
		loader := scy.New()
		private, err := loader.Load(context.Background(), &scy.Resource{URL: "datly-internal-warmup-private", Data: warmupPrivateEncrypted, Key: "blowfish://default"})
		if err != nil {
			embeddedWarmupKey.err = fmt.Errorf("load embedded warmup RSA private key: %w", err)
			return
		}
		key, err := jwtv5.ParseRSAPrivateKeyFromPEM([]byte(private.String()))
		if err != nil {
			embeddedWarmupKey.err = fmt.Errorf("parse embedded warmup RSA private key: %w", err)
			return
		}
		public, err := loader.Load(context.Background(), &scy.Resource{URL: "datly-internal-warmup-public", Data: warmupPublicEncrypted, Key: "blowfish://default"})
		if err != nil {
			embeddedWarmupKey.err = fmt.Errorf("load embedded warmup RSA public key: %w", err)
			return
		}
		embeddedWarmupKey.private = key
		embeddedWarmupKey.public = []byte(public.String())
	})
	return embeddedWarmupKey.private, embeddedWarmupKey.public, embeddedWarmupKey.err
}
