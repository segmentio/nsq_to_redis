package broadcast

import "github.com/garyburd/redigo/redis"

type Conn interface {
	Send(cmd string, args ...interface{}) error
	Flush() error
}

// Conn is a single threaded
// buffer for redis commands.
type conn struct {
	conn    redis.Conn
	pending int
}

// NewConn returns a new Conn.
func NewConn(c redis.Conn) Conn {
	return &conn{conn: c}
}

// Send sends the given command.
func (c *conn) Send(cmd string, args ...interface{}) error {
	err := c.conn.Send(cmd, args...)
	c.pending++
	return err
}

// Flush will flush the redis buffers
// and receive all responses from redis.
func (c *conn) Flush() error {
	err := c.conn.Flush()
	if err != nil {
		return err
	}

	for i := 0; i < c.pending; i++ {
		_, err := c.conn.Receive()
		if err != nil {
			return err
		}
	}

	return nil
}
