package broadcast

import (
	"io/ioutil"
	"sync/atomic"
	"testing"
	"time"

	nsq "github.com/bitly/go-nsq"
	"github.com/bmizerany/assert"
	"github.com/garyburd/redigo/redis"
	"github.com/segmentio/go-log"
	"github.com/segmentio/nsq_to_redis/broadcast/mocks"
	"github.com/segmentio/nsq_to_redis/ratelimit"
	statsd "github.com/segmentio/statsdclient"
	"github.com/stretchr/testify/mock"
)

func Benchmark1handler(b *testing.B)  { benchmarkBroadcast(1, b) }
func Benchmark2Handlers(b *testing.B) { benchmarkBroadcast(2, b) }

func benchmarkBroadcast(n int, b *testing.B) {
	pool := &mockRedisPool{}
	pool.On("Get").Return(mocks.NewNoOpRedisConn())

	broadcast := New(&Options{
		Redis:        pool,
		Metrics:      statsd.NewClient(ioutil.Discard),
		Log:          log.Log,
		Ratelimiter:  ratelimit.New(10, 500),
		RatelimitKey: "projectId",
	})

	expectedMessage, err := NewMessage("nsq__message__id", `{"projectId":"gy2d"}`)
	if err != nil {
		b.Error(err)
	}

	for i := 0; i < n; i++ {
		h := &mockHandler{}
		h.On("Handle", mock.Anything, expectedMessage).Return(nil)
		broadcast.Add(h)
	}

	nsqMsg := nsq.NewMessage(newNSQMessageId("nsq__message__id"), []byte(`{"projectId":"gy2d"}`))

	for n := 0; n < b.N; n++ {
		broadcast.HandleMessage(nsqMsg)
	}
}

func TestBroadcast(t *testing.T) {
	pool := &redis.Pool{
		IdleTimeout: 1 * time.Minute,
		MaxIdle:     15,
		MaxActive:   100,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", ":6379")
		},
	}

	b := New(&Options{
		Redis:        pool,
		Metrics:      statsd.NewClient(ioutil.Discard),
		Log:          log.Log,
		Ratelimiter:  ratelimit.New(10, 500),
		RatelimitKey: "projectId",
	})

	expectedMessage, err := NewMessage("nsq__message__id", `{"projectId":"gy2d"}`)
	assert.Equal(t, nil, err)

	h1 := &mockHandler{}
	h1.On("Handle", mock.Anything, expectedMessage).Times(1).Return(nil)
	h2 := &mockHandler{}
	h2.On("Handle", mock.Anything, expectedMessage).Times(1).Return(nil)

	b.Add(h1)
	b.Add(h2)

	nsqMsg := nsq.NewMessage(newNSQMessageId("nsq__message__id"), []byte(`{"projectId":"gy2d"}`))
	b.HandleMessage(nsqMsg)

	h1.AssertExpectations(t)
	h2.AssertExpectations(t)
}

func TestBroadcastInvalidFlushInterval(t *testing.T) {
	assert.Panic(t, "FlushInterval must not be a negative duration", func() {
		New(&Options{FlushInterval: -1 * time.Hour})
	})
}

func TestBroadcastWithoutFlushInterval(t *testing.T) {
	pool := getMockPool()
	broadcast := New(&Options{
		Redis:   pool,
		Metrics: statsd.NewClient(ioutil.Discard),
		Log:     log.Log,
	})

	sendMessages(broadcast)

	mockConn := pool.Get().(*mocks.NoOpRedisConn)
	assert.Equal(t, 2, int(atomic.LoadUint64(&mockConn.Flushes)))
}

func TestBroadcastWithFlushInterval(t *testing.T) {
	pool := getMockPool()
	broadcast := New(&Options{
		Redis:         pool,
		Metrics:       statsd.NewClient(ioutil.Discard),
		Log:           log.Log,
		FlushInterval: 2 * time.Millisecond,
	})

	sendMessages(broadcast)

	mockConn := pool.Get().(*mocks.NoOpRedisConn)
	assert.Equal(t, 0, int(atomic.LoadUint64(&mockConn.Flushes)))
	<-time.After(3 * time.Millisecond)
	assert.Equal(t, 1, int(atomic.LoadUint64(&mockConn.Flushes)))
}

func TestBroadcastWithFlushIntervalStop(t *testing.T) {
	pool := getMockPool()
	broadcast := New(&Options{
		Redis:         pool,
		Metrics:       statsd.NewClient(ioutil.Discard),
		Log:           log.Log,
		FlushInterval: 10 * time.Second,
	})

	sendMessages(broadcast)

	mockConn := pool.Get().(*mocks.NoOpRedisConn)
	assert.Equal(t, 0, int(atomic.LoadUint64(&mockConn.Flushes)))
	broadcast.Stop()
	<-broadcast.Done
	assert.Equal(t, 1, int(atomic.LoadUint64(&mockConn.Flushes)))
}

func getMockPool() RedisPool {
	pool := &mockRedisPool{}
	pool.On("Get").Return(mocks.NewNoOpRedisConn())
	return pool
}

func sendMessages(broadcast *Broadcast) {
	nsqMsg := nsq.NewMessage(newNSQMessageId("nsq__message__id"), []byte(`{"projectId":"gy2d"}`))
	broadcast.HandleMessage(nsqMsg)
	broadcast.HandleMessage(nsqMsg)
}

func newNSQMessageId(id string) nsq.MessageID {
	nsqId := [nsq.MsgIDLength]byte{}
	copy(nsqId[:], id[:nsq.MsgIDLength])
	return nsqId
}

type mockHandler struct {
	mock.Mock
}

// Handle provides a mock function with given fields: _a0, _a1
func (_m *mockHandler) Handle(_a0 Conn, _a1 *Message) error {
	ret := _m.Called(_a0, _a1)

	var r0 error
	if rf, ok := ret.Get(0).(func(Conn, *Message) error); ok {
		r0 = rf(_a0, _a1)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

type mockRedisPool struct {
	mock.Mock
}

// Get provides a mock function with given fields:
func (_m *mockRedisPool) Get() redis.Conn {
	ret := _m.Called()

	var r0 redis.Conn
	if rf, ok := ret.Get(0).(func() redis.Conn); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(redis.Conn)
	}

	return r0
}
