package session

import (
	"context"
	"fmt"
	reader "github.com/viant/datly/service/reader"
	"github.com/viant/datly/view"
	"os"
	"runtime/debug"
)

func (s *Session) ReadInto(ctx context.Context, dest interface{}, aView *view.View) error {
	if os.Getenv("DATLY_DEBUG_READINTO") == "1" {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("[READINTO DEBUG] panic view=%s dest=%T err=%v\n%s\n", aView.Name, dest, r, debug.Stack())
				panic(r)
			}
		}()
	}
	if err := s.SetViewState(ctx, aView); err != nil {
		return err
	}
	aReader := reader.New()
	return aReader.ReadInto(ctx, dest, aView, reader.WithResourceState(s.state))
}
