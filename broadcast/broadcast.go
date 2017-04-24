package broadcast

import (
	"encoding/json"
	"time"

	"github.com/bitly/go-nsq"
	"github.com/garyburd/redigo/redis"
	"github.com/segmentio/go-log"
	"github.com/segmentio/go-stats"
	"github.com/segmentio/nsq_to_redis/ratelimit"
	"github.com/statsd/client"
	"github.com/tidwall/gjson"
)

// Handler is a message handler.
type Handler interface {
	Handle(*Conn, *Message) error
}

// Message is a parsed message.
type Message struct {
	ID   nsq.MessageID
	Body json.RawMessage
}

// Options for broadcast.
type Options struct {
	Redis        *redis.Pool
	Metrics      *statsd.Client
	Ratelimiter  *ratelimit.Ratelimiter
	RatelimitKey string
	Log          *log.Logger
}

// Broadcast consumer distributes messages to N handlers.
type Broadcast struct {
	handlers []Handler
	stats    *stats.Stats
	*Options
}

// New broadcast consumer.
func New(o *Options) *Broadcast {
	stats := stats.New()
	go stats.TickEvery(10 * time.Second)
	return &Broadcast{
		stats:   stats,
		Options: o,
	}
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
	var body json.RawMessage
	err := json.Unmarshal(msg.Body, &body)
	if err != nil {
		b.Log.Error("error parsing json: %s", err)
		return nil
	}
	m.Body = body

	// ratelimit
	if b.rateExceeded(m) {
		b.stats.Incr("ratelimit.discard")
		b.Metrics.Incr("counts.ratelimit.discard")
		b.Log.Debug("ratelimit exceeded, discarding message")
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

// rateExceeded returns true if the given message
// rate was exceeded. The method returns false
// if ratelimit was not configured or exceeded.
func (b *Broadcast) rateExceeded(msg *Message) bool {
	if b.Ratelimiter != nil {
		k := gjson.Get(string(msg.Body), b.RatelimitKey).String()
		return b.Ratelimiter.Exceeded(k)
	}

	return false
}
