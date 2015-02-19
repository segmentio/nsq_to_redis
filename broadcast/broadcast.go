package broadcast

import "github.com/bitly/go-nsq"

// Broadcast consumer distributes messages to N handlers.
type Broadcast struct {
	handlers []nsq.Handler
}

// New broadcast consumer.
func New() *Broadcast {
	return new(Broadcast)
}

// Add handler.
func (b *Broadcast) Add(h nsq.Handler) {
	b.handlers = append(b.handlers, h)
}

// HandleMessage parses distributes messages to each delegate.
func (b *Broadcast) HandleMessage(msg *nsq.Message) error {
	for _, h := range b.handlers {
		err := h.HandleMessage(msg)
		if err != nil {
			return err
		}
	}
	return nil
}
