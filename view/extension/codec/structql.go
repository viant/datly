package codec

import (
	"context"
	"fmt"
	"github.com/viant/structql"
	"github.com/viant/xdatly/codec"
	"github.com/viant/xunsafe"
	"reflect"
	"strings"
)

const Structql = "structql"

type (
	StructQLFactory string
	StructQLCodec   struct {
		query      string
		_query     *structql.Query
		_xtype     *xunsafe.Type
		ownerType  reflect.Type
		recordType reflect.Type
	}
)

func (s StructQLFactory) New(codec *codec.Config, _ ...codec.Option) (codec.Instance, error) {
	if codec.Body == "" {
		return nil, fmt.Errorf("codec query can't be empty")
	}

	SQL := strings.TrimSpace(codec.Body)
	switch SQL[0] {
	case '?':
		SQL = SQL[1:]
	case '!':
		SQL = SQL[1:]
	}

	structQLCodec, err := NewStructQLCodec(SQL, codec.InputType)
	if err != nil {
		return nil, err
	}

	return structQLCodec, nil
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
		return nil, fmt.Errorf("failed to evaludate structql %v codec: %w", s.query, err)
	}
	s._query = query
	return query, nil
}

func (s *StructQLCodec) Value(ctx context.Context, raw interface{}, options ...codec.Option) (interface{}, error) {
	query, err := s.evaluateQuery()
	if err != nil {
		return nil, err
	}

	result, err := s.selectQuery(query, raw)
	if err != nil {
		return nil, err
	}

	return result, err
}

func (s *StructQLCodec) selectQuery(query *structql.Query, raw interface{}) (interface{}, error) {
	if query.Limit == 1 {
		return query.First(raw)
	}
	result, err := query.Select(raw)
	if err == nil {
		result = s._xtype.Deref(result)
	}

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
	if s._query.Limit == 1 {
		s.recordType = s.recordType.Elem()
	}

	s._xtype = xunsafe.NewType(s.recordType)

	return nil
}
