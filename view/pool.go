package view

import (
	"context"
	"database/sql"
	"fmt"
	as "github.com/aerospike/aerospike-client-go"
	"github.com/aerospike/aerospike-client-go/types"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	AerospikeConnectionTimeoutInS = 5
	PingTimeInS                   = 15
)

type (
	dbRegistry struct {
		index map[string]*db
		mutex sync.Mutex
	}

	db struct {
		mutex       sync.Mutex
		actual      *sql.DB
		ctx         context.Context
		cancelFunc  context.CancelFunc
		initialized bool
	}

	aerospikeClientRegistry struct {
		index map[string]*aerospikeClient
		mutex sync.Mutex
	}

	aerospikeClient struct {
		actual      *as.Client
		mutex       sync.RWMutex
		cancelFunc  func()
		initialized bool
	}
)

func ResetConnectionConfig() {
	AerospikeConnectionTimeoutInS = 5
	PingTimeInS = 15
}

func (c *aerospikeClient) connect() (*as.Client, error) {
	c.mutex.Lock()
	aClient := c.actual
	c.mutex.Unlock()

	if aClient == nil || !aClient.IsConnected() {
		return nil, fmt.Errorf("no connection to one of aerospike cache was available")
	}

	return aClient, nil
}

func (c *aerospikeClient) init(host string, port int) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.initialized {
		return nil
	}

	c.initialized = true
	var err error
	c.actual, err = c.newClient(host, port)
	go c.keepProbingIfNeeded(host, port)

	return err
}

func (c *aerospikeClient) newClient(host string, port int) (*as.Client, error) {
	client, err := as.NewClient(host, port)
	if client != nil {
		client.DefaultPolicy.TotalTimeout = 100 * time.Millisecond
		client.DefaultPolicy.MaxRetries = 2
	}

	return client, err
}

func (c *aerospikeClient) loginNewClientError(err error) bool {
	if err == nil {
		return false
	}

	aerospikeError, ok := c.asAerospikeErr(err)
	if !ok {
		return false
	}

	switch aerospikeError.ResultCode() {
	case types.NO_AVAILABLE_CONNECTIONS_TO_NODE, types.INVALID_NODE_ERROR, types.TIMEOUT:
		fmt.Printf("[WARN] no available connection to one of the aerospike clients: %v\n", err.Error())
		return true
	default:
		fmt.Printf("[WARN] Error occured while connecting to aerospike %v\n", err.Error())
		return false
	}
}

func (c *aerospikeClient) asAerospikeErr(err error) (types.AerospikeError, bool) {
	aerospikeError, ok := err.(types.AerospikeError)
	if ok {
		return aerospikeError, true
	}

	aerospikePtrErr, ok := err.(*types.AerospikeError)
	if ok && aerospikePtrErr != nil {
		return *aerospikePtrErr, true
	}

	return types.AerospikeError{}, false
}

func (c *aerospikeClient) keepProbingIfNeeded(host string, port int) {
	for c.actual == nil {
		time.Sleep(time.Duration(PingTimeInS) * time.Second)

		newClient, err := c.newClient(host, port)
		if newClient != nil {
			c.actual = newClient
		}

		if err != nil {
			c.loginNewClientError(err)
		}
	}
}

var aDbPool = newPool()
var aClientPool = newClientPool()

func newClientPool() *aerospikeClientRegistry {
	return &aerospikeClientRegistry{index: map[string]*aerospikeClient{}}
}

func (d *db) initWithLock(driver string, dsn string, config *DBConfig) error {
	d.mutex.Lock()
	err := d.initDatabase(driver, dsn, config)
	d.keepConnectionAlive(driver, dsn, config)
	d.mutex.Unlock()

	return err
}

func (d *db) initDatabase(driver string, dsn string, config *DBConfig) error {
	if d.initialized {
		return nil
	}

	d.initialized = true
	var err error
	d.actual, err = sql.Open(driver, dsn)
	if d.actual != nil {
		d.configureDB(config, d.actual)
	}

	return err
}

func (d *db) connect() (*sql.DB, error) {
	d.mutex.Lock()
	aDb := d.actual
	d.mutex.Unlock()

	if aDb == nil {
		return nil, fmt.Errorf("no connection with database is available")
	}

	return aDb, nil
}

func (d *db) configureDB(c *DBConfig, aDb *sql.DB) {
	if c.MaxIdleConns != 0 {
		aDb.SetMaxIdleConns(c.MaxIdleConns)
	}

	if c.ConnMaxIdleTimeMs != 0 {
		aDb.SetConnMaxIdleTime(c.ConnMaxIdleTime())
	}

	if c.MaxOpenConns != 0 {
		aDb.SetMaxOpenConns(c.MaxOpenConns)
	}

	if c.ConnMaxLifetimeMs != 0 {
		aDb.SetConnMaxLifetime(c.ConnMaxLifetime())
	}
}

func (d *db) keepConnectionAlive(driver string, dsn string, config *DBConfig) {
	if d.cancelFunc != nil {
		return
	}

	newCtx := context.Background()
	cancel, cancelFunc := context.WithCancel(newCtx)

	d.ctx = cancel
	d.cancelFunc = cancelFunc

	go func(driver, dsn string, config *DBConfig) {
		for {
			time.Sleep(time.Second * time.Duration(PingTimeInS))

			select {
			case <-cancel.Done():
				return
			default:
				d.mutex.Lock()
				aDb := d.actual
				d.mutex.Unlock()

				var err error
				if aDb != nil {
					err = aDb.PingContext(d.ctx)
				}

				if err != nil || aDb == nil {
					newDb, err := sql.Open(driver, dsn)
					d.mutex.Lock()
					d.actual = newDb
					if newDb != nil {
						d.configureDB(config, newDb)
					}
					d.mutex.Unlock()

					ctx, timeout := d.ctxWithTimeout(time.Duration(5) * time.Second)
					err = newDb.PingContext(ctx)
					if err != nil {
						fmt.Printf("[INFO] couldn't connect to one of %v database \n", driver)
					}

					timeout()
				}
			}
		}
	}(driver, dsn, config)
}

func (d *db) ctxWithTimeout(duration time.Duration) (context.Context, context.CancelFunc) {
	background := context.Background()
	ctxWithTimeout, cancelFn := context.WithTimeout(background, duration)
	return ctxWithTimeout, cancelFn
}

func (p *dbRegistry) DB(driver, dsn string, config *DBConfig) func() (*sql.DB, error) {
	builder := &strings.Builder{}

	if config == nil {
		config = &DBConfig{}
	}

	builder.WriteString(strconv.Itoa(config.ConnMaxLifetimeMs))
	builder.WriteByte('#')
	builder.WriteString(strconv.Itoa(config.MaxIdleConns))
	builder.WriteByte('#')
	builder.WriteString(strconv.Itoa(config.MaxOpenConns))
	builder.WriteByte('#')
	builder.WriteString(strconv.Itoa(config.ConnMaxIdleTimeMs))
	builder.WriteByte('#')
	builder.WriteString(driver)
	builder.WriteString("://")
	builder.WriteString(dsn)

	actualKey := builder.String()
	dbConn := p.getItem(actualKey, driver, dsn, config)

	return dbConn.connect
}

func (p *dbRegistry) getItem(key string, driver string, dsn string, config *DBConfig) *db {
	p.mutex.Lock()
	item, ok := p.index[key]
	if !ok {
		item = &db{}
		err := item.initWithLock(driver, dsn, config)
		if err != nil {
			fmt.Printf("error occured while initializing db %v\n", err.Error())
		}

		p.index[key] = item
	}

	p.mutex.Unlock()
	return item
}

func ResetDBPool() {
	for _, dbItem := range aDbPool.index {
		if dbItem.cancelFunc != nil {
			dbItem.cancelFunc()
		}
	}

	aDbPool = newPool()
}

func ResetAerospikePool() {
	for _, aClient := range aClientPool.index {
		if aClient.cancelFunc != nil {
			aClient.cancelFunc()
		}
	}
	aClientPool = newClientPool()
}

func newPool() *dbRegistry {
	return &dbRegistry{index: map[string]*db{}}
}

func (a *aerospikeClientRegistry) Client(host string, port int) func() (*as.Client, error) {
	aKey := host + ":" + strconv.Itoa(port)
	aClient := a.clientWithLock(aKey, host, port)

	return aClient.connect
}

func (a *aerospikeClientRegistry) clientWithLock(key string, host string, port int) *aerospikeClient {
	a.mutex.Lock()

	client, ok := a.index[key]
	if !ok {
		client = &aerospikeClient{}
		if err := client.init(host, port); err != nil {
			fmt.Printf("error occurred while connecting to aerospike client %v\n", err.Error())
		}
		a.index[key] = client
	}

	a.mutex.Unlock()
	return client
}
