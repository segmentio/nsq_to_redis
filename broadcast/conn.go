package broadcast

import "github.com/garyburd/redigo/redis"

// Conn is a single threaded
// buffer for redis commands.
type Conn struct {
	conn    redis.Conn
	pending int
}

// newConn returns a new Conn.
func newConn(c redis.Conn) *Conn {
	return &Conn{conn: c}
}

// Send sends the given command.
func (c *Conn) Send(cmd string, args ...interface{}) error {
	err := c.conn.Send(cmd, args...)
	c.pending++
	return err
}

// Flush will flush the redis buffers
// and receive all responses from redis.
func (c *Conn) Flush() error {
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
