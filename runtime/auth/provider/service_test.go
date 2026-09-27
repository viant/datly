package provider

import (
	"context"
	"errors"
	"testing"

	scyauth "github.com/viant/scy/auth"
	"github.com/viant/scy/auth/jwt"
	xauth "github.com/viant/xdatly/auth"
)

type defaultAuthenticator struct{}

func (defaultAuthenticator) BasicAuth(context.Context, string, string) (*scyauth.Token, error) {
	return nil, nil
}
func (defaultAuthenticator) VerifyIdentity(context.Context, string) (*jwt.Claims, error) {
	return nil, nil
}
func (defaultAuthenticator) ReissueIdentityToken(context.Context, string, string) (*scyauth.Token, error) {
	return nil, nil
}
func (defaultAuthenticator) ResetCredentials(context.Context, string, string) error { return nil }

func TestServiceSelectsOnlyConfiguredAuthenticator(t *testing.T) {
	want := defaultAuthenticator{}
	service := &Service{authenticators: map[xauth.Vendor]xauth.Authenticator{xauth.VendorDefault: want}}
	for _, vendor := range []xauth.Vendor{"", xauth.VendorDefault} {
		actual, err := service.Authenticator(vendor)
		if err != nil || actual != want {
			t.Fatalf("Authenticator(%q) = (%T, %v)", vendor, actual, err)
		}
	}
	if _, err := service.Authenticator(xauth.VendorFirebase); !errors.Is(err, xauth.ErrUnsupportedVendor) {
		t.Fatalf("unsupported vendor error = %v", err)
	}
}

func TestNewRequiresCompleteStaticConfiguration(t *testing.T) {
	if _, err := New(context.Background(), Config{}); err == nil {
		t.Fatal("expected incomplete provider configuration to fail")
	}
}
