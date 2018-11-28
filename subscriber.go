package psmq

import (
	"log"

	"github.com/streadway/amqp"
)

// Handler 接到訊息之後的動作
type Handler func(data []byte)

// Subscriber 接收端
type Subscriber struct {
	psmq    *PSMQ
	msgs    <-chan amqp.Delivery
	handler Handler
}

// NewSubscriber new a subscriber
func (ps *PSMQ) NewSubscriber(exchange string, queueTTLSec int32, h Handler) (*Subscriber, error) {
	failedPrefix := "New subscriber failed"
	err := ps.declareExchange(exchange)
	if err != nil {
		return nil, failedError(failedPrefix, err)
	}

	queue, err := ps.declareQueue(queueTTLSec)
	if err != nil {
		return nil, failedError(failedPrefix, err)
	}

	// ----- Binding Queue -----
	err = ps.bindQueue(queue, exchange)
	if err != nil {
		return nil, failedError(failedPrefix, err)
	}

	// ----- Consumer -----
	msgs, err := ps.channel.Consume(queue, "", true, false, false, false, nil)
	if err != nil {
		return nil, failedError(failedPrefix, err)
	}
	return &Subscriber{ps, msgs, h}, nil
}

// Run a subscriber
func (s *Subscriber) Run() {
	forever := make(chan bool)

	go func() {
		for d := range s.msgs {
			s.handler(d.Body)
		}
	}()
	log.Printf("[psmq] Waiting for message. Press \"CTRL+C\" to exit.")
	<-forever
}
