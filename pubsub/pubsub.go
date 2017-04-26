package pubsub

import (
	"time"

	interpolate "github.com/segmentio/go-interpolate"
	"github.com/segmentio/go-log"
	"github.com/segmentio/go-stats"
	"github.com/segmentio/nsq_to_redis/broadcast"
	"github.com/statsd/client"
)

// Options for PubSub.
type Options struct {
	Format  string         // Redis publish channel format
	Log     *log.Logger    // Logger
	Metrics *statsd.Client // Metrics
}

// PubSub publishes messages to a formatted channel.
type PubSub struct {
	template *interpolate.Template
	stats    *stats.Stats
	*Options
}

// New pubsub with options.
func New(options *Options) (*PubSub, error) {
	p := &PubSub{
		Options: options,
		stats:   stats.New(),
	}

	tmpl, err := interpolate.New(p.Format)
	if err != nil {
		return nil, err
	}

	p.template = tmpl
	go p.stats.TickEvery(10 * time.Second)

	return p, nil
}

// HandleMessage expects parsed json messages from NSQ,
// applies them against the publish channel template to
// produce the channel name, and then publishes to Redis.
func (p *PubSub) Handle(c broadcast.Conn, msg *broadcast.Message) error {
	start := time.Now()

	channel, err := p.template.Eval(msg.JSON)
	if err != nil {
		p.Log.Error("evaluating template: %s", err)
		return nil
	}

	p.Log.Info("publish %s to %s", msg.ID, channel)
	p.Log.Debug("contents %s %s", msg.ID, msg.Body)

	err = c.Send("PUBLISH", channel, []byte(msg.Body))
	if err != nil {
		p.Log.Error("publish: %s", err)
		return err
	}

	p.Metrics.Duration("timers.published", time.Since(start))
	p.Metrics.Incr("counts.published")
	p.stats.Incr("published")
	return nil
}
