package router

import (
	"context"
	"github.com/viant/datly/repository/path"
	"github.com/viant/datly/view/extension"
	"github.com/viant/scy/auth/jwt"
	"net/http"
	"strconv"
	"strings"
)

func Sanitize(request *http.Request, aPath *path.Path, headers http.Header, response http.ResponseWriter) {
	if authorization := headers.Get("Authorization"); authorization != "" {
		obfuscateAuthorization(request, response, authorization, headers, aPath)
	}

	if apiKey := aPath.APIKey; apiKey != nil {
		for key := range headers {
			if strings.EqualFold(apiKey.Header, key) {
				headers.Set(key, "*****")
			}
		}
	}
}

func obfuscateAuthorization(request *http.Request, response http.ResponseWriter, authorization string, headers http.Header, aPath *path.Path) {
	if response != nil {
		if jwtCodec, _ := extension.Config.LookupCodec(extension.CodecKeyJwtClaim); jwtCodec != nil {
			if claim, _ := jwtCodec.Instance.Value(context.TODO(), authorization); claim != nil {
				if jwtClaim, ok := claim.(*jwt.Claims); ok && jwtClaim != nil {
					headers.Set("User-ID", strconv.Itoa(jwtClaim.UserID))
					headers.Set("User-Email", jwtClaim.Email)
					if aPath.IsMetricsEnabled(request) {
						response.Header().Set("User-ID", strconv.Itoa(jwtClaim.UserID))
						response.Header().Set("User-Email", jwtClaim.Email)
					}
				}
			}
		}
	}
	headers.Set("Authorization", "***")
}
