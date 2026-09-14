package testharness

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
)

type Fixture struct {
	SchemaSQL string
	SeedSQL   string
	Request   string
	Expected  string
}

func LoadFixture(dir string) (*Fixture, error) {
	ret := &Fixture{}
	load := func(name string, target *string) error {
		path := filepath.Join(dir, name)
		data, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		*target = string(data)
		return nil
	}
	if err := load("schema.sql", &ret.SchemaSQL); err != nil {
		return nil, err
	}
	if err := load("seed.sql", &ret.SeedSQL); err != nil {
		return nil, err
	}
	if err := load("request.json", &ret.Request); err != nil {
		return nil, err
	}
	if err := load("expected.json", &ret.Expected); err != nil {
		return nil, err
	}
	return ret, nil
}

func (h *Harness) ApplyFixture(ctx context.Context, fixture *Fixture) error {
	if fixture == nil {
		return nil
	}
	if fixture.SchemaSQL != "" {
		if err := h.ExecStatements(ctx, fixture.SchemaSQL); err != nil {
			return err
		}
	}
	if fixture.SeedSQL != "" {
		if err := h.ExecStatements(ctx, fixture.SeedSQL); err != nil {
			return err
		}
	}
	return nil
}

func DecodeJSON[T any](raw string) (*T, error) {
	if raw == "" {
		return nil, nil
	}
	var ret T
	if err := json.Unmarshal([]byte(raw), &ret); err != nil {
		return nil, err
	}
	return &ret, nil
}
