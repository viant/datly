package config

import (
	"context"
	"fmt"
	"github.com/viant/datly/plugins"
	"github.com/viant/structql"
	"reflect"
)

const CodecStructql = "structql"

type (
	StructQLFactory string
	StructQLCodec   struct {
		query      string
		_query     *structql.Query
		ownerType  reflect.Type
		recordType reflect.Type
	}
)

func (s StructQLFactory) Valuer() plugins.Valuer {
	return s
}

func (s StructQLFactory) Value(ctx context.Context, raw interface{}, options ...interface{}) (interface{}, error) {
	return nil, plugins.UnexpectedUseError(s)
}

func (s StructQLFactory) Name() string {
	return CodecStructql
}

func (s StructQLFactory) New(codec *plugins.CodecConfig, paramType reflect.Type) (plugins.Valuer, error) {
	if codec.Query == "" {
		return nil, fmt.Errorf("codec query can't be empty")
	}

	structQLCodec, err := NewStructQLCodec(codec.Query, paramType)
	if err != nil {
		return nil, err
	}

	return structQLCodec.Valuer(), nil
}

func (s *StructQLCodec) Valuer() plugins.Valuer {
	return s
}

func (s *StructQLCodec) Name() string {
	return CodecStructql
}

func (s *StructQLCodec) ResultType(_ reflect.Type) (reflect.Type, error) {
	return s.recordType, nil
}

func (s *StructQLCodec) evaluateQuery() (*structql.Query, error) {
	if s._query != nil {
		return s._query, nil
	}

	query, err := structql.NewQuery(s.query, s.ownerType, nil)
	if err != nil {
		return nil, err
	}

	s._query = query
	return query, nil
}

func (s *StructQLCodec) Value(ctx context.Context, raw interface{}, options ...interface{}) (interface{}, error) {
	query, err := s.evaluateQuery()
	if err != nil {
		return nil, err
	}

	result, err := query.Select(raw)
	return result, err
}

func NewStructQLCodec(query string, paramType reflect.Type) (*StructQLCodec, error) {
	result := &StructQLCodec{
		query:     query,
		ownerType: paramType,
	}

	return result, result.init()
}

func (s *StructQLCodec) init() error {
	aQuery, err := s.evaluateQuery()
	if err != nil {
		return err
	}

	s.recordType = aQuery.Type()

	return nil
}
