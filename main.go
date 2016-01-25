package main

import (
	"io/ioutil"
	"strconv"
	"time"

	"github.com/bitly/go-nsq"
	"github.com/garyburd/redigo/redis"
	"github.com/segmentio/go-log"
	"github.com/segmentio/nsq_to_redis/broadcast"
	"github.com/segmentio/nsq_to_redis/list"
	"github.com/segmentio/nsq_to_redis/pubsub"
	"github.com/statsd/client"
	"github.com/tj/docopt"
	"github.com/tj/go-gracefully"
)

var version = "1.3.0"

const usage = `
  Usage:
    nsq_to_redis
      --topic name [--channel name]
      [--max-attempts n] [--max-in-flight n]
      [--statsd addr]
      [--statsd-prefix prefix]
      [--lookupd-http-address addr...]
      [--nsqd-tcp-address addr...]
      [--redis-address addr]
      [--max-idle n]
      [--idle-timeout t]
      [--list name] [--list-size n]
      [--publish name]
      [--level name]

    nsq_to_redis -h | --help
    nsq_to_redis --version

  Options:
    --lookupd-http-address addr  nsqlookupd addresses [default: :4161]
    --nsqd-tcp-address addr      nsqd tcp addresses
    --redis-address addr         redis address [default: :6379]
    --max-attempts n             nsq max message attempts [default: 5]
    --max-in-flight n            nsq messages in-flight [default: 250]
    --max-idle n                 redis max idle connections [default: 15]
    --idle-timeout t             idle connection timeout [default: 1m]
    --list-size n                redis list size [default: 100]
    --list name                  redis list template
    --publish name               redis channel template
    --topic name                 nsq consumer topic name
    --channel name               nsq consumer channel name [default: nsq_to_redis]
    --level name                 log level [default: info]
    --statsd addr                tcp address [default: ]
    --statsd-prefix prefix       prefix for statsd [default: nsq_to_redis.]
    -h, --help                   output help information
    -v, --version                output version

`

func main() {
	args, err := docopt.Parse(usage, nil, true, version, false)
	if err != nil {
		log.Fatalf("error parsing arguments: %s", err)
	}

	lookupds := args["--lookupd-http-address"].([]string)
	channel := args["--channel"].(string)
	topic := args["--topic"].(string)

	var metrics *statsd.Client
	if addr := args["--statsd"].(string); addr != "" {
		metrics, err = statsd.Dial(addr)
	} else {
		metrics = statsd.NewClient(ioutil.Discard)
	}
	metrics.Prefix(args["--statsd-prefix"].(string))

	idleTimeout, err := time.ParseDuration(args["--idle-timeout"].(string))
	if err != nil {
		log.Fatalf("error parsing idle timeout: %s", err)
	}

	maxIdle, err := strconv.Atoi(args["--max-idle"].(string))
	if err != nil {
		log.Fatalf("error parsing max-idle: %s", err)
	}
	if maxIdle > 100 {
		log.Fatalf("max-idle must be below 100")
	}

	pool := &redis.Pool{
		IdleTimeout:  idleTimeout,
		MaxIdle:      maxIdle,
		MaxActive:    100,
		Dial:         dial(args["--redis-address"].(string)),
		TestOnBorrow: ping,
	}

	broadcast := broadcast.New(&broadcast.Options{
		Redis:   pool,
		Metrics: metrics,
		Log:     log.Log,
	})
	config := config(args)

	consumer, err := nsq.NewConsumer(topic, channel, config)
	if err != nil {
		log.Fatalf("error starting consumer: %s", err)
	}

	log.SetLevelString(args["--level"].(string))

	// Pub/Sub support.
	if format, ok := args["--publish"].(string); ok {
		log.Info("publishing to %q", format)
		pubsub, err := pubsub.New(&pubsub.Options{
			Format:  format,
			Log:     log.Log,
			Metrics: metrics,
		})

		if err != nil {
			log.Fatalf("error starting pubsub: %s", err)
		}

		broadcast.Add(pubsub)
	}

	// Capped list support.
	if format, ok := args["--list"].(string); ok {
		size, err := strconv.Atoi(args["--list-size"].(string))
		if err != nil {
			log.Fatalf("error parsing --list-size: %s", err)
		}

		log.Info("listing to %q (size=%d)", format, size)
		list, err := list.New(&list.Options{
			Format:  format,
			Log:     log.Log,
			Metrics: metrics,
			Size:    20,
		})

		if err != nil {
			log.Fatalf("error starting list: %s", err)
		}

		broadcast.Add(list)
	}

	consumer.AddConcurrentHandlers(broadcast, maxIdle)
	nsqds := args["--nsqd-tcp-address"].([]string)

	if len(nsqds) > 0 {
		err = consumer.ConnectToNSQDs(nsqds)
	} else {
		err = consumer.ConnectToNSQLookupds(lookupds)
	}
	if err != nil {
		log.Fatalf("error connecting to nsqds: %s", err)
	}

	gracefully.Shutdown()

	log.Info("stopping")
	consumer.Stop()
	<-consumer.StopChan
	log.Info("bye :)")
}

// Parse NSQ configuration from args.
func config(args map[string]interface{}) *nsq.Config {
	config := nsq.NewConfig()

	n, err := strconv.Atoi(args["--max-attempts"].(string))
	if err != nil {
		log.Fatalf("error parsing --max-attempts: %s", err)
	}
	config.MaxAttempts = uint16(n)

	n, err = strconv.Atoi(args["--max-in-flight"].(string))
	if err != nil {
		log.Fatalf("error parsing --max-in-flight: %s", err)
	}
	config.MaxInFlight = n

	return config
}

// Dialer.
func dial(addr string) func() (redis.Conn, error) {
	return func() (redis.Conn, error) {
		return redis.Dial("tcp", addr)
	}
}

// Idle connection test.
func ping(client redis.Conn, t time.Time) error {
	_, err := client.Do("PING")
	return err
}
