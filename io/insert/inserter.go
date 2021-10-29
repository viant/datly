package insert

import (
	"context"
	"fmt"
	"github.com/viant/datly/config"
	"github.com/viant/datly/io"
	iocontext "github.com/viant/datly/io/context"
	"github.com/viant/datly/io/db"
	"github.com/viant/datly/metadata"
	"github.com/viant/sqlx/io/insert"
)

func Insert(ctx context.Context, target interface{}, aView *metadata.View, options ...config.Option) error {
	var conn, err = iocontext.Connector(ctx, aView.Connector, options...)
	if err != nil {
		return err
	}
	if aView.Connector == "" {
		aView.SetConnector(conn.Name)
	}
	var container io.Container
	container, err = io.NewContainer(target, aView)
	if err != nil {
		return err
	}
	session := iocontext.LookupSession(ctx)
	if session == nil {
		return fmt.Errorf("context.session was nil")
	}
	return insertViewData(ctx, conn, aView, container)
}



func insertViewData(ctx context.Context, conn *db.Connector, view *metadata.View, container io.Container) (err error) {
	var started bool
	started, err = conn.Begin()
	if err != nil {
		return err
	}
	if started {
		defer func() {
			if err == nil {
				err = conn.Commit()
			} else {
				_ = conn.Rollback()
			}
		}()
	}



	//TODO references

	inserter, err := insert.New(ctx, conn.DB, view.Table, conn.Tx)
	if err != nil {
		return err
	}
	if _, _, err = inserter.Insert(container.Iter());err != nil {
		return err
	}
	return nil
}