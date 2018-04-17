package broadcast

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/bitly/go-nsq"
	"github.com/garyburd/redigo/redis"
	"github.com/segmentio/go-log"
	"github.com/segmentio/go-stats"
	"github.com/segmentio/nsq_to_redis/ratelimit"
	statsd "github.com/statsd/client"
	"github.com/tidwall/gjson"
)

type RedisPool interface {
	Get() redis.Conn
}

// Handler is a message handler.
type Handler interface {
	Handle(Conn, *Message) error
}

// Message is a parsed message.
type Message struct {
	ID   nsq.MessageID
	JSON json.RawMessage
}

// Options for broadcast.
type Options struct {
	Redis         RedisPool
	Metrics       *statsd.Client
	Ratelimiter   *ratelimit.Ratelimiter
	RatelimitKey  string
	Log           *log.Logger
	FlushInterval time.Duration
}

// Broadcast consumer distributes messages to N handlers.
type Broadcast struct {
	Done chan struct{}
	*Options

	handlers []Handler
	stats    *stats.Stats

	// Single connection, channel and mutex are used when applying a flush interval
	conn  Conn
	mutex sync.Mutex
}

// New broadcast consumer.
func New(o *Options) *Broadcast {
	stats := stats.New()
	go stats.TickEvery(10 * time.Second)

	broadcast := Broadcast{
		stats:   stats,
		Options: o,
		Done:    make(chan struct{}, 1),
	}

	if o.FlushInterval < 0 {
		panic("FlushInterval must not be a negative duration")
	} else if o.FlushInterval > 0 {
		conn := NewConn(o.Redis.Get())
		broadcast.conn = conn
		broadcast.flushOnInterval(conn)
	}

	return &broadcast
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
	err := json.Unmarshal(msg.Body, &m.JSON)
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

	// TODO: Remove segment metadata here.

	conn, done := b.getConn()
	defer done()

	for _, h := range b.handlers {
		err := h.Handle(conn, m)
		if err != nil {
			return err
		}
	}

	if b.FlushInterval == 0 {
		if err := b.flush(conn); err != nil {
			return err
		}
	}

	b.Metrics.Duration("timers.broadcast", time.Since(start))
	return nil
}

// Flushes all messages, then sends on the Done channel.
func (b *Broadcast) Stop() {
	if b.FlushInterval > 0 {
		conn, done := b.getConn()
		defer done()
		b.flush(conn)
	}

	b.Done <- struct{}{}
}

func (b *Broadcast) getConn() (conn Conn, done func()) {
	if b.FlushInterval == 0 {
		db := b.Redis.Get()
		conn = NewConn(db)
		done = func() {
			db.Close()
		}
	} else {
		b.mutex.Lock()
		conn = b.conn
		done = func() {
			b.mutex.Unlock()
		}
	}

	return conn, done
}

func (b *Broadcast) flushOnInterval(conn Conn) {
	go func() {
		for range time.Tick(b.FlushInterval) {
			b.mutex.Lock()
			b.flush(conn)
			b.mutex.Unlock()
		}
	}()
}

func (b *Broadcast) flush(conn Conn) error {
	err := conn.Flush()
	if err != nil {
		b.Metrics.Incr("errors.flush")
		b.Log.Error("flush: %s", err)
		return err
	}

	return nil
}

// rateExceeded returns true if the given message
// rate was exceeded. The method returns false
// if ratelimit was not configured or exceeded.
func (b *Broadcast) rateExceeded(msg *Message) bool {
	if b.Ratelimiter != nil {
		k := gjson.Get(string(msg.JSON), b.RatelimitKey).String()
		return b.Ratelimiter.Exceeded(k)
	}

	return false
}

// NewMessage returns a Message or an error if unable to do so.
// Used primarily by tests.
func NewMessage(id, contents string) (*Message, error) {
	nsqId := [nsq.MsgIDLength]byte{}
	copy(nsqId[:], id[:nsq.MsgIDLength])

	m := new(Message)
	m.ID = nsqId
	err := json.Unmarshal([]byte(contents), &m.JSON)
	if err != nil {
		return nil, err
	}

	return m, nil
}
