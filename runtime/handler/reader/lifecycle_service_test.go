package reader

import (
	"context"
	"errors"
)

type fetchRow struct {
	ID      int    `sqlx:"id"`
	Name    string `sqlx:"name"`
	Fetched bool   `sqlx:"-"`
}

func (r *fetchRow) OnFetch(context.Context) error {
	r.Fetched = true
	return nil
}

type failingFetchRow struct {
	ID   int    `sqlx:"id"`
	Name string `sqlx:"name"`
}

func (*failingFetchRow) OnFetch(context.Context) error {
	return errors.New("fetch failed")
}
