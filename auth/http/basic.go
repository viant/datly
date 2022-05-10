package http

import (
	"net/http"
)

type Basic struct {
	Next    http.HandlerFunc
	Matcher func(username, password string) bool
}

func (b *Basic) Auth() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		if ok {
			if b.Matcher(username, password) {
				b.Next(w, r)
				return
			}
		}
		w.Header().Set("WWW-Authenticate", `Basic realm="restricted", charset="UTF-8"`)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
	}
}
