package view

import (
	"database/sql"
	as "github.com/aerospike/aerospike-client-go"
	"strconv"
	"strings"
	"sync"
)

type (
	dbPool struct {
		index map[string]*db
		mutex sync.Mutex
	}

	db struct {
		mutex  sync.Mutex
		actual *sql.DB
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

func (d *db) connectWithLock(driver string, dsn string, config *DBConfig) (*sql.DB, error) {
	d.mutex.Lock()
	aDb, err := d.connect(driver, dsn, config)

	if err == nil && d.actual != aDb {

		d.actual = aDb
	}

	d.mutex.Unlock()
	return aDb, err
}

func (d *db) connect(driver string, dsn string, c *DBConfig) (*sql.DB, error) {
	if d.actual != nil {
		if err := d.actual.Ping(); err != nil {
			d.actual = nil
			return d.connect(driver, dsn, c)
		}

		return d.actual, nil
	}

	aDb, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, err
	}

	aDb.SetMaxIdleConns(c.MaxIdleConns)
	aDb.SetConnMaxIdleTime(c.ConnMaxIdleTime())
	aDb.SetMaxOpenConns(c.MaxOpenConns)
	aDb.SetConnMaxLifetime(c.ConnMaxLifetime())
	return aDb, err
}

func (p *dbPool) DB(driver, dsn string, config *DBConfig) (*sql.DB, error) {
	builder := &strings.Builder{}
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
	dbConn := p.getItem(actualKey)

	return dbConn.connectWithLock(driver, dsn, config)
}

func (p *dbPool) getItem(key string) *db {
	p.mutex.Lock()
	item, ok := p.index[key]
	if !ok {
		item = &db{}
		p.index[key] = item
	}

	p.mutex.Unlock()
	return item
}

func ResetDBPool() {
	aDbPool = newPool()
}

func ResetAerospikePool() {
	aClientPool = newClientPool()
}

func newPool() *dbPool {
	return &dbPool{index: map[string]*db{}}
}
