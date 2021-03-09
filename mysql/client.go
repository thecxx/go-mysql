package mysql

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

var (
	ErrorClientInvalidReplica = errors.New("invalid replica")
)

type Client struct {
	primary  *Database
	replicas []*Database
	cursor   int32
	mutex    sync.RWMutex
}

// NewClient returns a new client.
func NewClient(primary *Config) (*Client, error) {
	d, err := NewDatabaseWithConfig(primary)
	if err != nil {
		return nil, err
	}
	return &Client{
		primary:  d,
		replicas: nil,
		cursor:   0,
	}, nil
}

// SetReplica sets a new replica database.
func (c *Client) SetReplica(replica *Config) error {
	if replica == nil {
		return ErrorClientInvalidReplica
	}
	d, err := NewDatabaseWithConfig(replica)
	if err != nil {
		return err
	}
	// Set new reader
	c.mutex.Lock()
	c.replicas = append(c.replicas, d)
	c.mutex.Unlock()

	return nil
}

// Query executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
func (c *Client) Query(query string, args ...interface{}) (Result, error) {
	return c.getr().Query(query, args...)
}

// QueryContext executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
func (c *Client) QueryContext(ctx context.Context, query string, args ...interface{}) (Result, error) {
	return c.getr().QueryContext(ctx, query, args...)
}

// Exec executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
func (c *Client) Exec(query string, args ...interface{}) (Result, error) {
	return c.getp().Exec(query, args...)
}

// ExecContext executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
func (c *Client) ExecContext(ctx context.Context, query string, args ...interface{}) (Result, error) {
	return c.getp().ExecContext(ctx, query, args...)
}

// BeginTransaction starts a transaction. The default isolation level is dependent on
// the driver.
func (c *Client) BeginTransaction() (*Transaction, error) {
	return c.getp().BeginTransaction()
}

// BeginTransactionContext starts a transaction.
//
// The provided context is used until the transaction is committed or rolled back.
// If the context is canceled, the sql package will roll back
// the transaction. Tx.Commit will return an error if the context provided to
// BeginTx is canceled.
//
// The provided TxOptions is optional and may be nil if defaults should be used.
// If a non-default isolation level is used that the driver doesn't support,
// an error will be returned.
func (c *Client) BeginTransactionContext(ctx context.Context) (*Transaction, error) {
	return c.getp().BeginTransactionContext(ctx)
}

// GetPrimary returns the primary database.
func (c *Client) GetPrimary() *Database {
	return c.getp()
}

// GetReplica returns a replica database.
func (c *Client) GetReplica() *Database {
	return c.getr()
}

// Close stop the client.
func (c *Client) Close() {
	if c.primary != nil {
		c.primary.Close()
	}
	if len(c.replicas) > 0 {
		for _, r := range c.replicas {
			r.Close()
		}
	}
}

// Get a database for write.
func (c *Client) getp() *Database {
	return c.primary
}

// Get a client for read.
func (c *Client) getr() *Database {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	n := len(c.replicas)
	switch {
	// 1. If no reader
	case n <= 0:
		return c.getp()
	// 2. Only one
	case n == 1:
		return c.replicas[0]
	}
	// 3. Schedule
	return c.replicas[atomic.AddInt32(&c.cursor, 1)%int32(n)]
}
