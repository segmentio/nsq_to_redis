package list

import "github.com/segmentio/go-interpolate"
import "github.com/garyburd/redigo/redis"
import "github.com/segmentio/go-stats"
import "github.com/segmentio/go-log"
import "github.com/bitly/go-nsq"
import "encoding/json"
import "time"

// Options.
type Options struct {
	Format string      // Redis list key format
	Redis  *redis.Pool // Redis client
	Log    *log.Logger // Logger
	Size   int64       // List size
}

// List writes messages to capped lists.
type List struct {
	template *interpolate.Template
	stats    *stats.Stats
	*Options
}

// New list with options.
func New(options *Options) (*List, error) {
	r := &List{
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
// applies them against the key template to produce a
// key name, and writes to the list.
func (l *List) HandleMessage(msg *nsq.Message) error {
	var v interface{}

	err := json.Unmarshal(msg.Body, &v)
	if err != nil {
		l.Log.Error("parsing json: %s", err)
		return nil
	}

	key, err := l.template.Eval(v)
	if err != nil {
		l.Log.Error("evaluating template: %s", err)
		return nil
	}

	l.Log.Info("pushing %s to %s", msg.ID, key)
	l.Log.Debug("contents %s %s", msg.ID, msg.Body)

	client := l.Redis.Get()
	defer client.Close()

	client.Send("LPUSH", key, msg.Body)
	client.Send("LTRIM", key, 0, l.Size-1)

	err = client.Flush()
	if err != nil {
		l.Log.Error("flush: %s", err)
		return err
	}

	_, err = client.Receive()
	if err != nil {
		l.Log.Error("lpush: %s", err)
		return err
	}

	_, err = client.Receive()
	if err != nil {
		l.Log.Error("ltrim: %s", err)
		return err
	}

	l.stats.Incr("pushed")
	return nil
}
