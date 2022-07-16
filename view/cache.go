package view

import (
	"context"
	"fmt"
	"github.com/viant/afs/option"
	"github.com/viant/sqlx/io/read/cache"
	"time"
)

type Cache struct {
	Location     string
	TimeToLiveMs int
	PartSize     int
	cache        *cache.Service
}

func (c *Cache) init(ctx context.Context, viewName string) error {
	if c.Location == "" {
		return fmt.Errorf("view %v cache Location can't be empty", viewName)
	}

	if c.TimeToLiveMs == 0 {
		return fmt.Errorf("view %v cache TimeToLimeMs can't be empty", viewName)
	}

	newCache, err := cache.NewCache(c.Location, time.Duration(c.TimeToLiveMs)*time.Millisecond, viewName, option.NewStream(c.PartSize, 0))
	if err != nil {
		return err
	}

	c.cache = newCache
	return nil
}

func (c *Cache) Service() *cache.Service {
	return c.cache
}
