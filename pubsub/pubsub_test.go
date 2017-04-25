package pubsub

import (
	"io/ioutil"
	"testing"

	"github.com/bmizerany/assert"
	"github.com/garyburd/redigo/redis"
	"github.com/segmentio/go-log"
	"github.com/segmentio/nsq_to_redis/broadcast"
	"github.com/segmentio/nsq_to_redis/broadcast/mocks"
	statsd "github.com/statsd/client"
)

func TestPubSub(t *testing.T) {
	pubSub, err := New(&Options{
		Format:  "stream:project:{projectId}:ingress",
		Log:     log.Log.New("pubsub_test"),
		Metrics: statsd.NewClient(ioutil.Discard),
	})
	assert.Equal(t, nil, err)

	cPublish, err := redis.Dial("tcp", ":6379")
	assert.Equal(t, nil, err)
	defer cPublish.Close()

	conn := broadcast.NewConn(cPublish)
	broadcastMessage, err := broadcast.NewMessage("nsq__message__id", `{"projectId":"gy2d"}`)
	if err != nil {
		t.Error(err)
	}

	cSubscribe, err := redis.Dial("tcp", ":6379")
	assert.Equal(t, nil, err)
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

	err = pubSub.Handle(conn, broadcastMessage)
	assert.Equal(t, nil, err)
	err = conn.Flush()
	assert.Equal(t, nil, err)

	msg := <-msgC
	assert.Equal(t, "stream:project:gy2d:ingress", msg.Channel)
	assert.Equal(t, `{"projectId":"gy2d"}`, string(msg.Data))
}

func BenchmarkPubSub(b *testing.B) {
	l := log.Log.New("pubsub_benchmark")
	l.SetLevel(log.ERROR)

	pubSub, err := New(&Options{
		Format:  "stream:project:{projectId}:ingress",
		Log:     l,
		Metrics: statsd.NewClient(ioutil.Discard),
	})
	if err != nil {
		b.Error(err)
	}

	conn := &mocks.Conn{}
	conn.On("Send", "PUBLISH", []interface{}{
		"stream:project:gy2d:ingress",
		[]byte(`{"projectId":"gy2d"}`),
	}).Return(nil)
	msg, err := broadcast.NewMessage("nsq_message_id_1", `{"projectId":"gy2d"}`)

	for i := 0; i < b.N; i++ {
		pubSub.Handle(conn, msg)
	}
}
