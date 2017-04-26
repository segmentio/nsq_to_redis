package mocks

import "github.com/garyburd/redigo/redis"

func NewNoOpRedisConn() redis.Conn {
	return &noOpRedisConn{}
}

type noOpRedisConn struct{}

func (c *noOpRedisConn) Close() error {
	return nil
}

func (c *noOpRedisConn) Err() error {
	return nil
}

func (c *noOpRedisConn) Do(commandName string, args ...interface{}) (reply interface{}, err error) {
	return nil, nil
}

func (c *noOpRedisConn) Send(commandName string, args ...interface{}) error {
	return nil
}

func (c *noOpRedisConn) Flush() error {
	return nil

}

func (c *noOpRedisConn) Receive() (reply interface{}, err error) {
	return nil, nil
}
