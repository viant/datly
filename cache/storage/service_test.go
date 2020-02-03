package storage

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/viant/afs"
	"testing"
	"time"
)

func TestService_Get(t *testing.T) {

	var useCases = []struct{
		description string
		baseURL     string
		key         string
		prepare bool
		data        []byte
		ttl         time.Duration
		sleepTime   time.Duration
		hasData bool
	}{
		{
			description:"get cached entry",
			prepare:true,
			baseURL:"mem://localhost/cache/case001/",
			key:"k1",
			data:[]byte("test is test 1"),
			hasData:true,
			ttl:100 * time.Millisecond,
		},

		{
			description:"expired cached entry",
			baseURL:"mem://localhost/cache/case002/",
			prepare:true,
			key:"k1",
			data:[]byte("test is test 2"),
			hasData:false,
			ttl:1 * time.Millisecond,
			sleepTime:10 * time.Millisecond,
		},

		{
			description:"missing cached entry",
			baseURL:"mem://localhost/cache/case003/",
			key:"k1",
			hasData:false,
		},


	}

	ctx := context.Background()


	for _, useCase := range useCases {
		srv := New(useCase.baseURL, afs.New())
		if useCase.prepare {
			err := srv.Put(ctx, useCase.key, useCase.data, useCase.ttl)
			assert.Nil(t, err, useCase.data)
		}


		if useCase.sleepTime > 0 {
			time.Sleep(useCase.sleepTime)
		}

		value, err := srv.Get(ctx, useCase.key)
		if ! assert.Nil(t, err, useCase.data) {
			return
		}
		if useCase.hasData {
			assert.EqualValues(t, useCase.data, value)
		} else {
			assert.Nil(t, value, useCase.description)
		}

	}
}