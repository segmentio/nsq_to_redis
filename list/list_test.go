package list

import (
	"io/ioutil"
	"testing"

	"github.com/bmizerany/assert"
	"github.com/garyburd/redigo/redis"
	goredis "github.com/go-redis/redis"
	"github.com/segmentio/go-log"
	"github.com/segmentio/nsq_to_redis/broadcast"
	"github.com/segmentio/nsq_to_redis/broadcast/mocks"
	statsd "github.com/segmentio/statsdclient"
)

func BenchmarkList(b *testing.B) {
	l := log.Log.New("list_benchmark")
	l.SetLevel(log.ERROR)

	list, err := New(&Options{
		Format:  "stream_benchmark:persist:{projectId}:ingress",
		Log:     l,
		Metrics: statsd.NewClient(ioutil.Discard),
		Size:    50,
	})
	if err != nil {
		b.Error(err)
	}

	conn := &mocks.Conn{}
	conn.On("Send", "LPUSH", []interface{}{
		"stream_benchmark:persist:gy2d:ingress",
		[]byte(`{"projectId":"gy2d"}`),
	}).Return(nil)
	conn.On("Send", "LTRIM", []interface{}{
		"stream_benchmark:persist:gy2d:ingress",
		0,
		int64(49),
	}).Return(nil)

	msg, err := broadcast.NewMessage("nsq_message_id_1", `{"projectId":"gy2d"}`)

	for i := 0; i < b.N; i++ {
		list.Handle(conn, msg)
	}
}

func TestList(t *testing.T) {
	list, err := New(&Options{
		Format:  "stream:persist:{projectId}:ingress",
		Log:     log.Log.New("list_test"),
		Metrics: statsd.NewClient(ioutil.Discard),
		Size:    50,
	})
	assert.Equal(t, nil, err)

	cPublish, err := redis.Dial("tcp", ":6379")
	assert.Equal(t, nil, err)
	defer cPublish.Close()

	conn := broadcast.NewConn(cPublish)
	broadcastMessage, err := broadcast.NewMessage("nsq_message_id_1", `{"projectId":"gy2d"}`)

	err = list.Handle(conn, broadcastMessage)
	assert.Equal(t, nil, err)
	err = conn.Flush()
	assert.Equal(t, nil, err)

	client := goredis.NewClient(&goredis.Options{
		Addr: "localhost:6379",
	})
	defer client.LTrim("stream:persist:gy2d:ingress", 0, 0)

	vals, err := client.LRange("stream:persist:gy2d:ingress", 0, -1).Result()
	assert.Equal(t, nil, err)
	assert.Equal(t, vals, []string{`{"projectId":"gy2d"}`})
}
