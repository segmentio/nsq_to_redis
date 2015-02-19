package pubsub

import "github.com/segmentio/go-interpolate"
import "github.com/segmentio/go-stats"
import "github.com/segmentio/go-log"
import "github.com/bitly/go-nsq"
import "gopkg.in/redis.v2"
import "encoding/json"
import "time"

// Options.
type Options struct {
	Format string        // Redis publish channel format
	Redis  *redis.Client // Redis client
	Log    *log.Logger   // Logger
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

	err = p.Redis.Publish(channel, string(msg.Body)).Err()
	if err != nil {
		p.Log.Error("publishing: %s", err)
		return err
	}

	p.stats.Incr("published")
	return nil
}
