package rmqworker

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/matrixbotio/constants-lib"
	darkmq "github.com/sagleft/darkrmq"
	"github.com/streadway/amqp"
)

type consumer struct {
	Tag       string
	ManualAck bool

	QueueData DeclareQueueTask
	Binding   exchandeBindData

	msgHandler    func(delivery RMQDeliveryHandler)
	errorCallback func(err *constants.APIError)
}

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
	if task.DeadLetterExchange != "" {
		args["x-dead-letter-exchange"] = task.DeadLetterExchange
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
		errInfo := "failed to declare queue: " + err.Error()
		c.errorCallback(constants.Error(
			"SERVICE_REQ_FAILED",
			errInfo,
		))
		return errors.New(errInfo)
	}
	return nil
}

func (c *consumer) declareExchange(ch *amqp.Channel) error {
	err := ch.ExchangeDeclare(
		c.Binding.ExchangeName, // name
		ternary(c.Binding.ExchangeType == "", ExchangeTypeDirect, c.Binding.ExchangeType), // type
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		errInfo := "failed to declare exchange: " + err.Error()
		c.errorCallback(constants.Error(
			"SERVICE_REQ_FAILED",
			errInfo,
		))
		return errors.New(errInfo)
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
		errInfo := "failed to bind queue `" + c.QueueData.Name + "` to `" + c.Binding.ExchangeName + "`: " + err.Error()
		c.errorCallback(constants.Error(
			"SERVICE_REQ_FAILED",
			errInfo,
		))
		return errors.New(errInfo)
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
// NOTE: it's blocking method
func (c *consumer) Consume(task darkmq.ConsumeTask) error {
	err := task.Ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		errInfo := "failed to set QOS: " + err.Error()
		c.errorCallback(constants.Error(
			"SERVICE_REQ_FAILED",
			errInfo,
		))
		return errors.New(errInfo)
	}

	// set consumer tag
	if task.UniqueTag == "" {
		task.UniqueTag = uuid.NewString()
	}

	msgs, err := task.Ch.Consume(
		c.QueueData.Name, // queue
		task.UniqueTag,   // consumer name
		false,            // auto-ack
		false,            // exclusive
		false,            // no-local
		false,            // no-wait
		nil,              // args
	)
	if err != nil {
		errInfo := "failed to consume from `" + c.QueueData.Name + "`: " + err.Error()
		c.errorCallback(constants.Error(
			"SERVICE_REQ_FAILED",
			errInfo,
		))
		return errors.New(errInfo)
	}

	// notify consumer ready
	task.ReadyCh <- struct{}{}

	for {
		select {
		case delivery, chOpened := <-msgs:
			if err := c.handleDelivery(delivery, chOpened); err != nil {
				c.ErrorCallback(err)
			}
		case <-task.Ctx.Done():
			return task.Ctx.Err()
		}
	}
}

// GetTag - get consumer tag
func (c *consumer) GetTag() string {
	return c.Tag
}

func (c *consumer) ErrorCallback(err error) {
	c.errorCallback(constants.Error(
		"SERVICE_REQ_FAILED",
		err.Error(),
	))
}

func (c *consumer) handleDelivery(delivery amqp.Delivery, chOpened bool) error {
	if !chOpened {
		return fmt.Errorf("handle delivery: %w", amqp.ErrClosed)
	}

	deliveryHandler := NewRMQDeliveryHandler(delivery)

	if !c.ManualAck {
		if err := deliveryHandler.Accept(); err != nil {
			return fmt.Errorf("handle delivery accept message: %s", err.Message)
		}
	}

	c.msgHandler(deliveryHandler)

	return nil
}
