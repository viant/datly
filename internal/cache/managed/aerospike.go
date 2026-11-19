package managed

import (
	"context"
	"fmt"

	as "github.com/aerospike/aerospike-client-go"
	"github.com/aerospike/aerospike-client-go/types"
	"github.com/google/uuid"
)

type recordClient interface {
	Get(*as.BasePolicy, *as.Key, ...string) (*as.Record, error)
	Put(*as.WritePolicy, *as.Key, as.BinMap) error
}

type AerospikeStore struct {
	client recordClient
	key    *as.Key
}

func NewAerospikeStore(client *as.Client, namespace, set, owner string) (*AerospikeStore, error) {
	key, err := as.NewKey(namespace, set, "datly-cache-generation/"+owner)
	if err != nil {
		return nil, err
	}
	return &AerospikeStore{client: client, key: key}, nil
}
func (s *AerospikeStore) Read(ctx context.Context) (Generation, error) {
	result := Generation{All: "0", Lazy: "0", Warmup: "0"}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	record, err := s.client.Get(nil, s.key)
	if e, ok := err.(types.AerospikeError); ok && e.ResultCode() == types.KEY_NOT_FOUND_ERROR {
		return result, nil
	}
	if err != nil {
		return result, err
	}
	if record == nil {
		return result, nil
	}
	for scope, target := range map[Scope]*string{All: &result.All, Lazy: &result.Lazy, Warmup: &result.Warmup} {
		value, exists := record.Bins[string(scope)]
		if !exists {
			continue
		}
		token, ok := value.(string)
		if !ok {
			return result, fmt.Errorf("invalid cache generation for %s", scope)
		}
		if _, err := uuid.Parse(token); err != nil {
			return result, err
		}
		*target = token
	}
	return result, nil
}
func (s *AerospikeStore) Rotate(ctx context.Context, scope Scope) (string, error) {
	if err := scope.Validate(); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	token := uuid.NewString()
	// Only this bin changes; concurrent scope invalidations cannot lose one another.
	policy := as.NewWritePolicy(0, as.TTLDontExpire)
	err := s.client.Put(policy, s.key, as.BinMap{string(scope): token})
	return token, err
}
