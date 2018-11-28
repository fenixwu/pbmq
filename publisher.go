package psmq

import (
	"github.com/streadway/amqp"
)

type contentType string

// Publisher 發送端
type Publisher struct {
	psmq                  *PSMQ
	contentType, exchange string
}

// NewPublisher a publisher
func NewPublisher(pb *PSMQ, contentType, exchange string) (*Publisher, error) {
	err := pb.declareExchange(exchange)
	if err != nil {
		return nil, failedError("New publisher failed", err)
	}
	return &Publisher{pb, contentType, exchange}, nil
}

// Publish 發訊息
func (p *Publisher) Publish(message []byte) error {
	err := p.psmq.channel.Publish(p.exchange, "", false, false,
		amqp.Publishing{
			DeliveryMode: 2, // persistence
			ContentType:  p.contentType,
			Body:         message,
		})

	if err != nil {
		return failedError("Publish failed", err)
	}
	return nil
}
