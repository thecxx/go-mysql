package mysql

import (
	"time"
)

type DatabaseOption func(conf *Config)

// MaxConnLifetime
func WithMaxConnLifetime(lifetime time.Duration) DatabaseOption {
	return func(conf *Config) {
		conf.MaxLifetime = lifetime
	}
}

// MaxOpenConns
func WithMaxOpenConns(limit int) DatabaseOption {
	return func(conf *Config) {
		conf.MaxOpenConns = limit
	}
}

// MaxIdleConns
func WithMaxIdleConns(limit int) DatabaseOption {
	return func(conf *Config) {
		conf.MaxIdleConns = limit
	}
}

// DialTimeout
func WithDialTimeout(timeout time.Duration) DatabaseOption {
	return func(conf *Config) {
		conf.Timeout = timeout
	}
}

// ReadTimeout
func WithReadTimeout(timeout time.Duration) DatabaseOption {
	return func(conf *Config) {
		conf.ReadTimeout = timeout
	}
}

// WriteTimeout
func WithWriteTimeout(timeout time.Duration) DatabaseOption {
	return func(conf *Config) {
		conf.WriteTimeout = timeout
	}
}

// PingTest
func WithPingTest(b bool) DatabaseOption {
	return func(conf *Config) {
		conf.PingTest = b
	}
}
