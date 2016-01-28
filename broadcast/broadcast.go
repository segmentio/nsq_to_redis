package broadcast

import (
	"encoding/json"
	"time"

	"github.com/bitly/go-nsq"
	"github.com/garyburd/redigo/redis"
	"github.com/segmentio/go-log"
	"github.com/statsd/client"
)

// Handler is a message handler.
type Handler interface {
	Handle(*Conn, *Message) error
}

// Message is a parsed message.
type Message struct {
	ID   nsq.MessageID
	JSON map[string]interface{}
	Body []byte
}

// Options for broadcast.
type Options struct {
	Redis   *redis.Pool
	Metrics *statsd.Client
	Log     *log.Logger
}

// Broadcast consumer distributes messages to N handlers.
type Broadcast struct {
	handlers []Handler
	*Options
}

// New broadcast consumer.
func New(o *Options) *Broadcast {
	return &Broadcast{Options: o}
}

// Add handler.
func (b *Broadcast) Add(h Handler) {
	b.handlers = append(b.handlers, h)
}

// HandleMessage parses distributes messages to each delegate.
func (b *Broadcast) HandleMessage(msg *nsq.Message) error {
	start := time.Now()

	// parse
	m := new(Message)
	m.ID = msg.ID
	m.Body = msg.Body
	err := json.Unmarshal(m.Body, &m.JSON)
	if err != nil {
		b.Log.Error("error parsing json: %s", err)
		return nil
	}

	db := b.Redis.Get()
	defer db.Close()
	conn := newConn(db)

	for _, h := range b.handlers {
		err := h.Handle(conn, m)
		if err != nil {
			return err
		}
	}

	err = conn.Flush()
	if err != nil {
		b.Metrics.Incr("errors.flush")
		b.Log.Error("flush: %s", err)
		return err
	}

	b.Metrics.Duration("timers.broadcast", time.Since(start))
	return nil
}
