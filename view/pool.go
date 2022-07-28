package view

import (
	"context"
	"database/sql"
	"fmt"
	as "github.com/aerospike/aerospike-client-go"
	"strconv"
	"strings"
	"sync"
	"time"
)

type (
	dbPool struct {
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

	aerospikePool struct {
		index map[string]*aerospikeClient
		mutex sync.Mutex
	}

	aerospikeClient struct {
		actual *as.Client
		mutex  sync.Mutex
	}
)

func (c *aerospikeClient) connect(host string, port int) (*as.Client, error) {
	c.mutex.Lock()
	client, err := c.connectIfNeeded(host, port)
	c.mutex.Unlock()
	return client, err
}

func (c *aerospikeClient) connectIfNeeded(host string, port int) (*as.Client, error) {
	client := c.actual
	if client != nil && client.IsConnected() {
		return client, nil
	}

	var err error
	c.actual, err = as.NewClient(host, port)
	return c.actual, err
}

var aDbPool = newPool()
var aClientPool = newClientPool()

func newClientPool() *aerospikePool {
	return &aerospikePool{index: map[string]*aerospikeClient{}}
}

func (d *db) initWithLock(ctx context.Context, driver string, dsn string, config *DBConfig) error {
	d.mutex.Lock()
	err := d.initDatabase(ctx, driver, dsn, config)
	d.keepConnectionAlive(driver, dsn, config)
	d.mutex.Unlock()

	return err
}

func (d *db) initDatabase(ctx context.Context, driver string, dsn string, config *DBConfig) error {
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
			time.Sleep(time.Second * time.Duration(15))

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

					err = newDb.PingContext(d.ctxWithTimeout(time.Duration(5) * time.Second))
					if err != nil {
						fmt.Printf("[INFO] couldn't connect to one of %v database \n", driver)
					}
				}
			}
		}
	}(driver, dsn, config)
}

func (d *db) ctxWithTimeout(duration time.Duration) context.Context {
	background := context.Background()
	ctxWithTimeout, _ := context.WithTimeout(background, duration)
	return ctxWithTimeout
}

func (p *dbPool) DB(ctx context.Context, driver, dsn string, config *DBConfig) func() (*sql.DB, error) {
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
	dbConn := p.getItem(ctx, actualKey, driver, dsn, config)

	return dbConn.connect
}

func (p *dbPool) getItem(ctx context.Context, key string, driver string, dsn string, config *DBConfig) *db {
	p.mutex.Lock()
	item, ok := p.index[key]
	if !ok {
		item = &db{}
		err := item.initWithLock(ctx, driver, dsn, config)
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
		dbItem.cancelFunc()
	}

	aDbPool = newPool()
}

func ResetAerospikePool() {
	aClientPool = newClientPool()
}

func newPool() *dbPool {
	return &dbPool{index: map[string]*db{}}
}

func (a *aerospikePool) Client(host string, port int) (*as.Client, error) {
	aKey := host + ":" + strconv.Itoa(port)
	aClient := a.clientWithLock(aKey)

	return aClient.connect(host, port)

}

func (a *aerospikePool) clientWithLock(key string) *aerospikeClient {
	a.mutex.Lock()

	client, ok := a.index[key]
	if !ok {
		client = &aerospikeClient{}
		a.index[key] = client
	}

	a.mutex.Unlock()
	return client
}
