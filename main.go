package main

import "github.com/segmentio/nsq_to_redis/broadcast"
import "github.com/segmentio/nsq_to_redis/pubsub"
import "github.com/segmentio/nsq_to_redis/list"
import "github.com/segmentio/go-log"
import "github.com/tj/go-gracefully"
import "github.com/bitly/go-nsq"
import "github.com/tj/docopt"
import "gopkg.in/redis.v2"
import "strconv"
import "time"

var Version = "0.2.0"

const Usage = `
  Usage:
    nsq_to_redis
      --topic name [--channel name]
      [--max-attempts n] [--max-in-flight n]
      [--lookupd-http-address addr...]
      [--redis-address addr]
      [--idle-timeout t]
      [--list name] [--list-size n]
      [--publish name]
      [--level name]

    nsq_to_redis -h | --help
    nsq_to_redis --version

  Options:
    --lookupd-http-address addr  nsqlookupd addresses [default: :4161]
    --redis-address addr         redis address [default: :6379]
    --max-attempts n             nsq max message attempts [default: 5]
    --max-in-flight n            nsq messages in-flight [default: 250]
    --idle-timeout t             idle connection timeout [default: 1m]
    --list-size n                redis list size [default: 100]
    --list name                  redis list template
    --publish name               redis channel template
    --topic name                 nsq consumer topic name
    --channel name               nsq consumer channel name [default: nsq_to_redis]
    --level name                 log level [default: warning]
    -h, --help                   output help information
    -v, --version                output version

`

func main() {
	args, err := docopt.Parse(Usage, nil, true, Version, false)
	if err != nil {
		log.Fatalf("error parsing arguments: %s", err)
	}

	lookupds := args["--lookupd-http-address"].([]string)
	channel := args["--channel"].(string)
	topic := args["--topic"].(string)

	broadcast := broadcast.New()
	config := config(args)

	idleTimeout, err := time.ParseDuration(args["--idle-timeout"].(string))
	if err != nil {
		log.Fatalf("error parsing idle timeout: %s", err)
	}

	redis := redis.NewClient(&redis.Options{
		Network:     "tcp",
		Addr:        args["--redis-address"].(string),
		DialTimeout: 10 * time.Second,
		IdleTimeout: idleTimeout,
	})

	consumer, err := nsq.NewConsumer(topic, channel, config)
	if err != nil {
		log.Fatalf("error starting consumer: %s", err)
	}

	log.SetLevelString(args["--level"].(string))

	// Pub/Sub support.
	if format, ok := args["--publish"].(string); ok {
		log.Info("publishing to %q", format)
		pubsub, err := pubsub.New(&pubsub.Options{
			Format: format,
			Redis:  redis,
			Log:    log.Log,
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
			Format: format,
			Redis:  redis,
			Log:    log.Log,
			Size:   20,
		})

		if err != nil {
			log.Fatalf("error starting list: %s", err)
		}

		broadcast.Add(list)
	}

	consumer.AddConcurrentHandlers(broadcast, 50)
	err = consumer.ConnectToNSQLookupds(lookupds)
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
