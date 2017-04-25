package pubsub

import (
	"encoding/json"
	"io/ioutil"
	"testing"

	nsq "github.com/bitly/go-nsq"
	"github.com/bmizerany/assert"
	"github.com/garyburd/redigo/redis"
	"github.com/segmentio/go-log"
	"github.com/segmentio/nsq_to_redis/broadcast"
	statsd "github.com/statsd/client"
)

func TestPubSub(t *testing.T) {
	pubSub, err := New(&Options{
		Format:  "stream:project:{projectId}:ingress",
		Log:     log.Log.New("pubsub_test"),
		Metrics: statsd.NewClient(ioutil.Discard),
	})
	if err != nil {
		t.Error(err)
	}

	cPublish, err := redis.Dial("tcp", ":6379")
	if err != nil {
		t.Error(err)
	}
	defer cPublish.Close()

	b := broadcast.NewConn(cPublish)
	broadcastMessage, err := newMessage("nsq__message__id", `{"projectId":"gy2d"}`)
	if err != nil {
		t.Error(err)
	}

	cSubscribe, err := redis.Dial("tcp", ":6379")
	if err != nil {
		t.Error(err)
	}
	defer cSubscribe.Close()
	psc := redis.PubSubConn{Conn: cSubscribe}
	psc.Subscribe("stream:project:gy2d:ingress")
	defer psc.Close()

	msgC := make(chan redis.Message, 10)
	go func() {
		for {
			switch v := psc.Receive().(type) {
			case redis.Message:
				msgC <- v
			}
		}
	}()

	pubSub.Handle(b, broadcastMessage)
	b.Flush()

	msg := <-msgC
	assert.Equal(t, "stream:project:gy2d:ingress", msg.Channel)
	// todo(prateek): compare the JSON not the bytes.
	assert.Equal(t, `[123 34 112 114 111 106 101 99 116 73 100 34 58 34 103 121 50 100 34 125]`, string(msg.Data))
}

func newMessage(id, contents string) (*broadcast.Message, error) {
	var body json.RawMessage
	err := json.Unmarshal([]byte(contents), &body)
	if err != nil {
		return nil, err
	}

	nsqId := [nsq.MsgIDLength]byte{}
	copy(nsqId[:], id[:nsq.MsgIDLength])

	return &broadcast.Message{
		ID:   nsqId,
		Body: body,
	}, nil
}
