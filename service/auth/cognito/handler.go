package cognito

import (
	"context"
	"fmt"
	"github.com/viant/afs/file"
	"github.com/viant/afs/url"
	"github.com/viant/datly/service/auth"
	"net/http"
	"strings"
	"time"
)

const (
	HeaderAuthorization = "Authorization"
	HeaderCookie        = "Set-Cookie"
	AuthTypeBasic       = "basic"
	AuthTypeBearer      = "bearer"
)

func (s *Service) Authorize(writer http.ResponseWriter, request *http.Request) bool {
	if s.authorizeRequest(writer, request) {
		return true
	}

	if s.Config.SignInURL != "" {
		if err := s.handleSignIn(writer, request); err == nil {
			return false
		}
	}
	writer.Header().Set("WWW-Authenticate", `Basic realm="restricted", charset="UTF-8"`)
	http.Error(writer, "Unauthorized", http.StatusUnauthorized)
	return false
}

func (s *Service) Auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.Authorize(w, r) {
			next(w, r)
		}
	}
}

func (s *Service) handleSignIn(w http.ResponseWriter, r *http.Request) error {
	URL := s.Config.SignInURL
	if strings.HasPrefix(url.Scheme(URL, file.Scheme), "http") {
		http.Redirect(w, r, URL, http.StatusMovedPermanently)
		return nil
	}
	data, err := s.fs.DownloadWithURL(context.Background(), URL, s.efs)
	if err != nil {
		return err
	}
	output := string(data)
	landingURL := landingPageURL(r)
	output = strings.ReplaceAll(string(data), "$redirect", landingURL)
	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(output))
	return nil
}

func hostname(r *http.Request) string {
	return strings.Split(r.Host, ":")[0]
}

func requestURI(r *http.Request) string {
	if r.RequestURI != "" {
		return r.RequestURI
	}
	return r.URL.String()
}

func landingPageURL(r *http.Request) string {
	if r.URL.Host == "" {
		if r.URL.Scheme == "" {
			r.URL.Scheme = "http"
		}
		landingPage := fmt.Sprintf("%s://%v%v", r.URL.Scheme, r.Host, r.RequestURI)
		return landingPage
	}
	landingURL := r.URL.String()
	return landingURL
}

func (s *Service) authorizeRequest(w http.ResponseWriter, r *http.Request) bool {
	ctx := context.Background()
	var authLiteral string
	if value, ok := r.Header[HeaderAuthorization]; ok {
		authLiteral = value[0]
	}
	if authLiteral == "" && s.Config.AuthCookie != "" {
		if cookie, err := r.Cookie(s.Config.AuthCookie); err == nil && cookie.Value != "" {
			authLiteral = cookie.Value
		}
	}

	authorization := auth.NewAuthorization(authLiteral)
	authType := strings.ToLower(authorization.Type)
	switch authType {
	case AuthTypeBasic:
		if username, password, ok := r.BasicAuth(); ok {
			if s.authenticateCredentials(w, r, username, password) {
				return true
			}
		}
	case AuthTypeBearer:
		if _, err := s.Service.VerifyIdentity(ctx, authorization.RawToken); err == nil {
			r.Header.Set(HeaderAuthorization, authLiteral)
			return true
		}
	default:
		if strings.ToLower(r.Method) == "post" && strings.Contains(requestURI(r), "signin") { //TODO put singing fragment to config
			r.ParseForm() //try to get credentials from a form
			username := r.FormValue("username")
			password := r.FormValue("password")
			if s.authenticateCredentials(w, r, username, password) {
				if redirect := r.FormValue("redirect"); redirect != "" {
					http.Redirect(w, r, redirect, http.StatusMovedPermanently)
				}
				return true
			}
		}
	}
	return false
}

func (s *Service) authenticateCredentials(w http.ResponseWriter, r *http.Request, username string, password string) bool {
	token, err := s.Service.InitiateBasicAuth(username, password)
	if err == nil && s.Config.AuthCookie != "" {
		domain := hostname(r)
		cookie := &http.Cookie{Name: s.Config.AuthCookie, Value: AuthTypeBearer + " " + token.IDToken, Domain: domain, Expires: time.Now().Add(55 * time.Minute)}
		w.Header().Set(HeaderCookie, cookie.String())
		r.Header.Set(HeaderAuthorization, cookie.Value)
		return true
	}
	return false
}
