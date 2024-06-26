package firebase

import (
	"context"
	"fmt"
	"github.com/viant/scy"
	"github.com/viant/scy/auth"
	"github.com/viant/scy/auth/firebase"
	"github.com/viant/scy/auth/gcp"
	"github.com/viant/scy/auth/gcp/client"
	"github.com/viant/scy/auth/jwt"
	"github.com/viant/scy/cred"
	"google.golang.org/api/option"
	"reflect"
)

type (
	Service struct {
		firebase *firebase.Service
	}
)

func (s *Service) BasicAuth(ctx context.Context, user string, password string) (*auth.Token, error) {
	return s.firebase.InitiateBasicAuth(ctx, user, password)
}

func (s *Service) VerifyIdentity(ctx context.Context, idToken string) (*jwt.Claims, error) {
	return s.verifyToken(ctx, idToken)
}

func (s *Service) verifyToken(ctx context.Context, idToken string) (*jwt.Claims, error) {
	if claims, err := gcp.JwtClaims(ctx, idToken); err == nil {
		return claims, nil
	}
	return s.firebase.VerifyIdentity(ctx, idToken)
}

func New(ctx context.Context, config *Config) (*Service, error) {
	var options []option.ClientOption
	if resource := config.Resource; resource != nil && resource.URL != "" {
		scyService := scy.New()
		resource.SetTarget(reflect.TypeOf(&cred.Generic{}))
		secret, err := scyService.Load(ctx, resource)
		if err != nil {
			return nil, err
		}
		fmt.Printf("using firebase with cred")
		options = append(options, option.WithCredentialsJSON([]byte(secret.String())))
	} else {
		gcpService := gcp.New(client.NewScy())
		fmt.Printf("using firebase with source token")
		tokenSource := gcpService.TokenSource("https://www.googleapis.com/auth/cloud-platform")
		options = append(options, option.WithTokenSource(tokenSource))
	}

	firebaseService, err := firebase.New(ctx, config.Config, options...)
	if err != nil {
		return nil, err
	}
	return &Service{
		firebase: firebaseService,
	}, nil
}
