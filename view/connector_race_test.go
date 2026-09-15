package view

import (
	"context"
	"sync"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestConnectionDBConcurrentFirstUse(t *testing.T) {
	ResetDBPool()
	defer ResetDBPool()

	connector := NewConnector("race", "sqlite3", ":memory:")
	if err := connector.Init(context.Background(), nil); err != nil {
		t.Fatalf("Init() error: %v", err)
	}

	var waitGroup sync.WaitGroup
	waitGroup.Add(8)
	for i := 0; i < 8; i++ {
		go func() {
			defer waitGroup.Done()
			for j := 0; j < 100; j++ {
				db, err := connector.DB()
				if err != nil {
					t.Errorf("DB() error: %v", err)
					return
				}
				if db == nil {
					t.Error("DB() returned nil")
					return
				}
			}
		}()
	}
	waitGroup.Wait()
}
