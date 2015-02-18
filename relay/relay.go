package relay

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

// Relay.
type Relay struct {
	template *interpolate.Template
	stats    *stats.Stats
	*Options
}

// New relay with options.
func New(options *Options) (*Relay, error) {
	r := &Relay{
		Options: options,
		stats:   stats.New(),
	}

	tmpl, err := interpolate.New(r.Format)
	if err != nil {
		return nil, err
	}

	r.template = tmpl
	go r.stats.TickEvery(10 * time.Second)

	return r, nil
}

// HandleMessage parses json messages received from NSQ,
// applies them against the publish channel template to
// produce the channel name, and then publishes to Redis.
func (r *Relay) HandleMessage(msg *nsq.Message) error {
	var v interface{}

	err := json.Unmarshal(msg.Body, &v)
	if err != nil {
		r.Log.Error("parsing json: %s", err)
		return nil
	}

	channel, err := r.template.Eval(v)
	if err != nil {
		r.Log.Error("evaluating template: %s", err)
		return nil
	}

	r.Log.Info("publish %s to %s", msg.ID, channel)
	r.Log.Debug("contents %s %s", msg.ID, msg.Body)

	err = r.Redis.Publish(channel, string(msg.Body)).Err()
	if err != nil {
		r.Log.Error("publishing: %s", err)
		return err
	}

	r.stats.Incr("published")
	return nil
}
