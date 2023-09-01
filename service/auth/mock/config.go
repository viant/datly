package mock

import (
	"embed"
	_ "github.com/viant/afs/embed"
	"github.com/viant/afs/storage"
	"github.com/viant/scy"
	"github.com/viant/scy/auth/jwt/signer"
	"github.com/viant/scy/auth/jwt/verifier"
)

//go:embed jwt/*
var embedFs embed.FS

// HmacJwtSigner returns new mock jwt signer config
func HmacJwtSigner() *signer.Config {
	return &signer.Config{
		HMAC: &scy.Resource{
			URL:     "embed:///jwt/hmac.enc",
			Key:     "blowfish://default",
			Options: []storage.Option{&embedFs},
		},
	}
}

// HmacJwtVerifier returns new mock jwt verifier config
func HmacJwtVerifier() *verifier.Config {
	return &verifier.Config{
		HMAC: &scy.Resource{
			URL:     "embed:///jwt/hmac.enc",
			Key:     "blowfish://default",
			Options: []storage.Option{&embedFs},
		},
	}
}

// RSAPublicJwtSigner returns new mock jwt rsa signer config
func RSAPublicJwtSigner() *signer.Config {
	return &signer.Config{
		RSA: &scy.Resource{
			URL:     "embed:///jwt/public.enc",
			Key:     "blowfish://default",
			Options: []storage.Option{&embedFs},
		},
	}
}

// RSAPrivateJwtVerifier returns new mock jwt rsa verifier config
func RSAPrivateJwtVerifier() *verifier.Config {
	return &verifier.Config{
		RSA: &scy.Resource{
			URL:     "embed:///jwt/private.enc",
			Key:     "blowfish://default",
			Options: []storage.Option{&embedFs},
		},
	}
}
