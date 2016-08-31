package broadcast

import (
	"encoding/json"
	"math/rand"
	"time"

	"github.com/bitly/go-nsq"
	"github.com/garyburd/redigo/redis"
	"github.com/segmentio/go-log"
	"github.com/segmentio/go-stats"
	"github.com/segmentio/nsq_to_redis/ratelimit"
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
	Redis        *redis.Pool
	Metrics      *statsd.Client
	Ratelimiter  *ratelimit.Ratelimiter
	RatelimitKey string
	SampleRate   float64
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
	if b.Options.SampleRate >= rand.Float64() {
		return nil // didn't pass sampling
	}

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
		if v, ok := msg.JSON[b.RatelimitKey].(string); ok {
			return b.Ratelimiter.Exceeded(v)
		}
	}

	return false
}
