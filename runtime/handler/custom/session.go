package custom

import (
	xhandler "github.com/viant/xdatly/handler"
	xresponse "github.com/viant/xdatly/response"
)

type session struct {
	binder   xhandler.Binder
	response xresponse.Writer
}

func newSession(binder xhandler.Binder, response xresponse.Writer) *session {
	return &session{binder: binder, response: response}
}

func (s *session) Binder() xhandler.Binder {
	return s.binder
}

func (s *session) Response() xresponse.Writer {
	return s.response
}
