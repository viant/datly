package xdatly

import (
	"context"
	"encoding/base64"
	"fmt"
	"github.com/golang-jwt/jwt/v4"
	"github.com/viant/scy/auth/gcp"
	"strings"
)

//GCPJwtClaim represents IDJWT visitor
type (
	GCPJwtClaim struct{}
	JWTClaims   struct {
		Email         string      `json:"email,omitempty"`
		UserID        int         `json:"user_id,omitempty"`
		Username      string      `json:"username,omitempty"`
		FirstName     string      `json:"first_name,omitempty"`
		LastName      string      `json:"last_name,omitempty"`
		AccountName   string      `json:"account_name,omitempty"`
		AccountId     int         `json:"account_id,omitempty"`
		Scope         string      `json:"scope,omitempty"`
		Cognito       string      `json:"cognito,omitempty"`
		VerifiedEmail bool        `json:"verified_email,omitempty"`
		Data          interface{} `json:"dat,omitempty"`
		jwt.RegisteredClaims
	}
)

func (j *GCPJwtClaim) Value(ctx context.Context, raw interface{}, options ...interface{}) (interface{}, error) {
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
