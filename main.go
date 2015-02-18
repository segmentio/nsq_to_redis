package main

import "github.com/segmentio/nsq_to_redis/relay"
import "github.com/segmentio/go-log"
import "github.com/tj/go-gracefully"
import "github.com/bitly/go-nsq"
import "github.com/tj/docopt"
import "gopkg.in/redis.v2"
import "strconv"
import "time"

var Version = "0.0.2"

const Usage = `
  Usage:
    nsq_to_redis
      --topic name --publish name [--channel name]
      [--max-attempts n] [--max-in-flight n]
      [--lookupd-http-address addr...]
      [--redis-address addr]
      [--level name]

    nsq_to_redis -h | --help
    nsq_to_redis --version

  Options:
    --lookupd-http-address addr  nsqlookupd addresses [default: :4161]
    --redis-address addr         redis address [default: :6379]
    --max-attempts n             nsq max message attempts [default: 5]
    --max-in-flight n            nsq messages in-flight [default: 250]
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

	topic := args["--topic"].(string)
	channel := args["--channel"].(string)
	publish := args["--publish"].(string)
	lookupds := args["--lookupd-http-address"].([]string)

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

	redis := redis.NewClient(&redis.Options{
		Network:     "tcp",
		Addr:        args["--redis-address"].(string),
		DialTimeout: 10 * time.Second,
	})

	consumer, err := nsq.NewConsumer(topic, channel, config)
	if err != nil {
		log.Fatalf("error starting consumer: %s", err)
	}

	log.SetLevelString(args["--level"].(string))

	relay, err := relay.New(&relay.Options{
		Format: publish,
		Redis:  redis,
		Log:    log.Log,
	})

	if err != nil {
		log.Fatalf("error starting relay: %s", err)
	}

	consumer.AddConcurrentHandlers(relay, 50)
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
