package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	driver "github.com/go-sql-driver/mysql"
)

const (
	DefaultMaxOpenConns = 50
	DefaultMaxIdleConns = 10
	DefaultMaxLifetime  = 30 * time.Second
	DefaultDialTimeout  = 2 * time.Second
	DefaultReadTimeout  = 0 * time.Second
	DefaultWriteTimeout = 0 * time.Second
)

var (
	defaultCtx                = context.Background()
	ErrorNotImplemented       = errors.New("not implemented")
	ErrorResultNoColumnsFound = errors.New("no columns found")
)

func buildResult(d *Database, result sql.Result) Result {
	return Result{d.cf.UniqId(), nil, result}
}

func buildResultRows(d *Database, rows *sql.Rows) Result {
	return Result{d.cf.UniqId(), rows, nil}
}

type Config struct {
	*driver.Config
	// Extension
	MaxOpenConns int
	MaxIdleConns int
	MaxLifetime  time.Duration
	PingTest     bool
}

// New a default config.
func NewDefaultConfig(addr, dbname, user, passwd string, ping bool) *Config {
	c := &Config{
		Config: driver.NewConfig(),
	}
	c.Net = "tcp"
	c.Timeout = DefaultDialTimeout
	c.ReadTimeout = DefaultReadTimeout
	c.WriteTimeout = DefaultWriteTimeout
	c.MaxOpenConns = DefaultMaxOpenConns
	c.MaxIdleConns = DefaultMaxIdleConns
	c.MaxLifetime = DefaultMaxLifetime
	// Authentication
	c.Addr = addr
	c.DBName = dbname
	c.User = user
	c.Passwd = passwd
	// Ping test
	c.PingTest = false

	return c
}

// Generate an unique ID.
func (c *Config) UniqId() string {
	return fmt.Sprintf("%s://%s/%s", c.Net, c.Addr, c.DBName)
}

type Database struct {
	db *sql.DB
	cf *Config
}

// NewDatabase returns a new database connection.
func NewDatabase(addr, dbname, user, passwd string, opts ...DatabaseOption) (*Database, error) {
	conf := NewDefaultConfig(addr, dbname, user, passwd, false)
	// Apply options
	for _, fun := range opts {
		fun(conf)
	}
	return NewDatabaseWithConfig(conf)
}

// NewDatabaseWithConfig returns a new database connection.
func NewDatabaseWithConfig(conf *Config) (*Database, error) {
	// Open a specific database
	db, err := sql.Open("mysql", conf.FormatDSN())
	if err != nil {
		return nil, err
	}
	// Setting
	db.SetConnMaxLifetime(conf.MaxLifetime)
	db.SetMaxOpenConns(conf.MaxOpenConns)
	db.SetMaxIdleConns(conf.MaxIdleConns)
	// Test ping
	if conf.PingTest {
		ctx, _ := context.WithTimeout(defaultCtx, conf.Timeout)
		// Ping with a timeout context
		if err = db.PingContext(ctx); err != nil {
			db.Close()
			return nil, err
		}
	}
	return &Database{db, conf}, nil
}

// Query executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
func (d *Database) Query(query string, args ...interface{}) (Result, error) {
	return d.QueryContext(defaultCtx, query, args...)
}

// QueryContext executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
func (d *Database) QueryContext(ctx context.Context, query string, args ...interface{}) (Result, error) {
	rows, err := d.db.QueryContext(ctx, query, args...)
	return buildResultRows(d, rows), err
}

// Exec executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
func (d *Database) Exec(query string, args ...interface{}) (Result, error) {
	return d.ExecContext(defaultCtx, query, args...)
}

// ExecContext executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
func (d *Database) ExecContext(ctx context.Context, query string, args ...interface{}) (Result, error) {
	result, err := d.db.ExecContext(ctx, query, args...)
	return buildResult(d, result), err
}

// Prepare creates a prepared statement for later queries or executions.
// Multiple queries or executions may be run concurrently from the
// returned statement.
// The caller must call the statement's Close method
// when the statement is no longer needed.
func (d *Database) Prepare(query string) (*Statement, error) {
	return d.PrepareContext(defaultCtx, query)
}

// PrepareContext creates a prepared statement for later queries or executions.
// Multiple queries or executions may be run concurrently from the
// returned statement.
// The caller must call the statement's Close method
// when the statement is no longer needed.
//
// The provided context is used for the preparation of the statement, not for the
// execution of the statement.
func (d *Database) PrepareContext(ctx context.Context, query string) (*Statement, error) {
	stmt, err := d.db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return &Statement{d, stmt}, nil
}

// BeginTransaction starts a transaction. The default isolation level is dependent on
// the driver.
func (d *Database) BeginTransaction() (*Transaction, error) {
	return d.BeginTransactionContext(defaultCtx)
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
func (d *Database) BeginTransactionContext(ctx context.Context) (*Transaction, error) {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &Transaction{d, tx}, nil
}

// Ping verifies a connection to the database is still alive,
// establishing a connection if necessary.
func (d *Database) Ping(ctx context.Context) error {
	return d.db.PingContext(ctx)
}

// Get the number of connections currently in use.
func (d *Database) ActiveConns() int {
	return d.db.Stats().InUse
}

// Get the number of idle connections.
func (d *Database) IdleConns() int {
	return d.db.Stats().Idle
}

// Close closes the database and prevents new queries from starting.
// Close then waits for all queries that have started processing on the server
// to finish.
//
// It is rare to Close a DB, as the DB handle is meant to be
// long-lived and shared between many goroutines.
func (d *Database) Close() error {
	return d.db.Close()
}

type Transaction struct {
	db *Database
	tx *sql.Tx
}

// Query executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
func (t *Transaction) Query(query string, args ...interface{}) (Result, error) {
	return t.QueryContext(defaultCtx, query, args...)
}

// QueryContext executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
func (t *Transaction) QueryContext(ctx context.Context, query string, args ...interface{}) (Result, error) {
	rows, err := t.tx.QueryContext(ctx, query, args...)
	return buildResultRows(t.db, rows), err
}

// Exec executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
func (t *Transaction) Exec(query string, args ...interface{}) (Result, error) {
	return t.ExecContext(defaultCtx, query, args...)
}

// ExecContext executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
func (t *Transaction) ExecContext(ctx context.Context, query string, args ...interface{}) (Result, error) {
	result, err := t.tx.ExecContext(ctx, query, args...)
	return buildResult(t.db, result), err
}

// Prepare creates a prepared statement for later queries or executions.
// Multiple queries or executions may be run concurrently from the
// returned statement.
// The caller must call the statement's Close method
// when the statement is no longer needed.
func (t *Transaction) Prepare(query string) (*Statement, error) {
	return t.PrepareContext(defaultCtx, query)
}

// PrepareContext creates a prepared statement for later queries or executions.
// Multiple queries or executions may be run concurrently from the
// returned statement.
// The caller must call the statement's Close method
// when the statement is no longer needed.
//
// The provided context is used for the preparation of the statement, not for the
// execution of the statement.
func (t *Transaction) PrepareContext(ctx context.Context, query string) (*Statement, error) {
	stmt, err := t.tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return &Statement{t.db, stmt}, nil
}

// Rollback aborts the transaction.
func (t *Transaction) Rollback() error {
	return t.tx.Rollback()
}

// Commit commits the transaction.
func (t *Transaction) Commit() error {
	return t.tx.Commit()
}

type Statement struct {
	db   *Database
	stmt *sql.Stmt
}

// Query executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
func (s *Statement) Query(args ...interface{}) (Result, error) {
	return s.QueryContext(defaultCtx, args...)
}

// QueryContext executes a query that returns rows, typically a SELECT.
// The args are for any placeholder parameters in the query.
func (s *Statement) QueryContext(ctx context.Context, args ...interface{}) (Result, error) {
	rows, err := s.stmt.QueryContext(ctx, args...)
	return buildResultRows(s.db, rows), err
}

// Exec executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
func (s *Statement) Exec(args ...interface{}) (Result, error) {
	return s.ExecContext(defaultCtx, args...)
}

// ExecContext executes a query without returning any rows.
// The args are for any placeholder parameters in the query.
func (s *Statement) ExecContext(ctx context.Context, args ...interface{}) (Result, error) {
	result, err := s.stmt.ExecContext(ctx, args...)
	return buildResult(s.db, result), err
}

// Close closes the statement.
func (s *Statement) Close() error {
	return s.stmt.Close()
}

type Result struct {
	hit    string
	rows   *sql.Rows
	result sql.Result
}

// Hit returns the data source.
func (r Result) Hit() string {
	return r.hit
}

// Get first row from the set.
func (r Result) Row() (row map[string]string, err error) {
	// If no rows
	if r.rows == nil {
		return nil, nil
	}
	columns, err := r.rows.Columns()
	if err != nil {
		return nil, err
	}
	if len(columns) == 0 {
		return nil, ErrorResultNoColumnsFound
	}
	// Init args
	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	if r.rows.Next() {
		// Scan
		err = r.rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}
		row = make(map[string]string)
		for i, value := range values {
			if value == nil {
				row[columns[i]] = ""
			} else {
				row[columns[i]] = string(value)
			}
		}
		r.rows.Close()
	}
	return row, nil
}

// Get all rows from the set.
func (r Result) Rows() (rows []map[string]string, err error) {
	// If no rows
	if r.rows == nil {
		return nil, nil
	}
	columns, err := r.rows.Columns()
	if err != nil {
		return nil, err
	}
	if len(columns) == 0 {
		return nil, ErrorResultNoColumnsFound
	}
	// Init args
	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(columns))
	for i := range values {
		scanArgs[i] = &values[i]
	}
	for r.rows.Next() {
		// Scan
		err = r.rows.Scan(scanArgs...)
		if err != nil {
			return nil, err
		}
		row := make(map[string]string)
		for i, value := range values {
			if value == nil {
				row[columns[i]] = ""
			} else {
				row[columns[i]] = string(value)
			}
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// Unmarshal all rows to a declared variable.
func (r Result) Unmarshal(rows interface{}) error {
	return ErrorNotImplemented
}

// RowsAffected returns the number of rows affected by an
// update, insert, or delete. Not every database or database
// driver may support this.
func (r Result) RowsAffected() (int64, error) {
	return r.result.RowsAffected()
}

// LastInsertId returns the integer generated by the database
// in response to a command. Typically this will be from an
// "auto increment" column when inserting a new row. Not all
// databases support this feature, and the syntax of such
// statements varies.
func (r Result) LastInsertId() (int64, error) {
	return r.result.LastInsertId()
}
