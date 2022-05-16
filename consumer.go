package rmqworker

import (
	"context"
	"errors"

	"github.com/streadway/amqp"
)

func (c *consumer) declareQueue(ch *amqp.Channel, task DeclareQueueTask) error {
	args := amqp.Table{}
	if task.MessagesLifetime > 0 {
		args["x-message-ttl"] = task.MessagesLifetime
	}
	if task.MaxLength > 0 {
		args["x-max-length"] = task.MaxLength
	}
	if task.DisableOverflow {
		args["x-overflow"] = "reject-publish"
	}

	_, err := ch.QueueDeclare(
		task.Name,       // name
		task.Durable,    // durable
		task.AutoDelete, // delete when unused
		false,           // exclusive
		false,           // no-wait
		args,            // arguments
	)
	if err != nil {
		return errors.New("failed to declare queue: " + err.Error())
	}
	return nil
}

func (c *consumer) declareExchange(ch *amqp.Channel) error {
	err := ch.ExchangeDeclare(
		c.Binding.ExchangeName, // name
		"direct",               // type
		true,                   // durable
		false,                  // auto-deleted
		false,                  // internal
		false,                  // no-wait
		nil,                    // arguments
	)
	if err != nil {
		return errors.New("failed to declare exchange: " + err.Error())
	}
	return nil
}

func (c *consumer) bindQueue(ch *amqp.Channel) error {
	err := ch.QueueBind(
		c.QueueData.Name,       // queue name
		c.Binding.RoutingKey,   // routing key
		c.Binding.ExchangeName, // exchange
		false,                  // no-wait
		nil,                    // arguments
	)
	if err != nil {
		return errors.New("failed to bind queue `" + c.QueueData.Name + "` to `" + c.Binding.ExchangeName + "`: " + err.Error())
	}
	return nil
}

// Declare implement darkmq.Consumer.(Declare) interface method
func (c *consumer) Declare(ctx context.Context, ch *amqp.Channel) error {
	err := c.declareQueue(ch, c.QueueData)
	if err != nil {
		return err
	}

	if c.Binding.ExchangeName != "" {
		err = c.declareExchange(ch)
		if err != nil {
			return err
		}

		err = c.bindQueue(ch)
		if err != nil {
			return err
		}
	}

	return nil
}

// Consume implement darkmq.Consumer.(Consume) interface method
func (c *consumer) Consume(ctx context.Context, ch *amqp.Channel) error {
	err := ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		return errors.New("failed to set QOS: " + err.Error())
	}

	// set consumer tag
	if c.Tag == "" {
		c.Tag = getUUID()
	}

	msgs, err := ch.Consume(
		c.QueueData.Name, // queue
		c.Tag,            // consumer name
		false,            // auto-ack
		false,            // exclusive
		false,            // no-local
		false,            // no-wait
		nil,              // args
	)
	if err != nil {
		return errors.New("failed to consume from `" + c.QueueData.Name + "`: ")
	}

	for {
		select {
		case msg, chClosed := <-msgs:
			if !chClosed {
				return amqp.ErrClosed
			}

			// get message
			delivery := NewRMQDeliveryHandler(msg)

			// accept message
			err := delivery.Accept()
			if err != nil {
				c.errorCallback(err)
				continue
			}

			// handle message
			c.msgHandler(delivery)

		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// GetTag - get consumer tag
func (c *consumer) GetTag() string {
	return c.Tag
}
