package pubsub

import (
	"encoding/json"
	"time"

	"github.com/bitly/go-nsq"
	"github.com/garyburd/redigo/redis"
	"github.com/segmentio/go-interpolate"
	"github.com/segmentio/go-log"
	"github.com/segmentio/go-stats"
	"github.com/statsd/client"
)

// Options for PubSub.
type Options struct {
	Format  string         // Redis publish channel format
	Redis   *redis.Pool    // Redis client
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

// HandleMessage parses json messages received from NSQ,
// applies them against the publish channel template to
// produce the channel name, and then publishes to Redis.
func (p *PubSub) HandleMessage(msg *nsq.Message) error {
	var v interface{}
	start := time.Now()

	err := json.Unmarshal(msg.Body, &v)
	if err != nil {
		p.Log.Error("parsing json: %s", err)
		return nil
	}

	channel, err := p.template.Eval(v)
	if err != nil {
		p.Log.Error("evaluating template: %s", err)
		return nil
	}

	p.Log.Info("publish %s to %s", msg.ID, channel)
	p.Log.Debug("contents %s %s", msg.ID, msg.Body)

	client := p.Redis.Get()
	defer client.Close()

	_, err = client.Do("PUBLISH", channel, msg.Body)

	if err != nil {
		p.Log.Error("publish: %s", err)
		return err
	}

	p.Metrics.Duration("timers.published", time.Since(start))
	p.Metrics.Incr("counts.published")
	p.stats.Incr("published")
	return nil
}
