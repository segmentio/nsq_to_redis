package mocks

import (
	"sync/atomic"

	"github.com/garyburd/redigo/redis"
)

func NewNoOpRedisConn() redis.Conn {
	return &NoOpRedisConn{}
}

type NoOpRedisConn struct {
	Flushes uint64
}

func (c *NoOpRedisConn) Close() error {
	return nil
}

func (c *NoOpRedisConn) Err() error {
	return nil
}

func (c *NoOpRedisConn) Do(commandName string, args ...interface{}) (reply interface{}, err error) {
	return nil, nil
}

func (c *NoOpRedisConn) Send(commandName string, args ...interface{}) error {
	return nil
}

func (c *NoOpRedisConn) Flush() error {
	atomic.AddUint64(&c.Flushes, 1)
	return nil
}

func (c *NoOpRedisConn) Receive() (reply interface{}, err error) {
	return nil, nil
}
