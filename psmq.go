package psmq

import (
	"errors"

	"github.com/streadway/amqp"
)

const defaultQueueTTLSec int32 = 30

// PSMQ is a message queue just for Publish/Subscribe.
type PSMQ struct {
	connection *amqp.Connection
	channel    *amqp.Channel
}

// New a rabbit
func New(url string) (*PSMQ, error) {
	connection, err := amqp.Dial(url)
	if err != nil {
		return nil, failedError("Connecte failed", err)
	}

	channel, err := connection.Channel()
	if err != nil {
		return nil, failedError("Open channel failed", err)
	}

	return &PSMQ{connection, channel}, nil
}

// Close 關閉
func (pb *PSMQ) Close() {
	pb.connection.Close()
	pb.channel.Close()
}

// 宣告隊列
func (pb *PSMQ) declareQueue(queueTTLSec int32) (queue string, err error) {
	var ttl = defaultQueueTTLSec
	if queueTTLSec > 0 {
		ttl = queueTTLSec
	}

	q, err := pb.channel.QueueDeclare("", true, false, false, false, amqp.Table{
		"x-expires": ttl * 1000,
	})

	if err != nil {
		err = failedError("Declare queue faile", err)
		return
	}

	queue = q.Name
	return
}

// 宣告交換器
func (pb *PSMQ) declareExchange(name string) error {
	err := pb.channel.ExchangeDeclare(name, amqp.ExchangeFanout, true, false, false, false, nil)
	if err != nil {
		return failedError("Declare exchange failed", err)
	}
	return nil
}

// 綁定隊列收發規則
func (pb *PSMQ) bindQueue(queue, exchange string) error {
	err := pb.channel.QueueBind(queue, "", exchange, false, nil)
	if err != nil {
		return failedError("Bind queue failed", err)
	}
	return nil
}

func failedError(prefix string, err error) error {
	return errors.New(prefix + ": " + err.Error())
}
