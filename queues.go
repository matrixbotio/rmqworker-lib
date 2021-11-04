package rmqworker

import (
	"github.com/matrixbotio/constants-lib"
	"github.com/streadway/amqp"
)

// rmqQueueDeclare - declare RMQ queue
func (r *RMQHandler) rmqQueueDeclare(RMQChannel *amqp.Channel, queueName string, durable bool, autodelete bool) APIError {
	_, err := RMQChannel.QueueDeclare(
		queueName,  // name
		durable,    // durable
		autodelete, // delete when unused
		false,      // exclusive
		false,      // no-wait
		nil,        // arguments
	)
	if err != nil {
		return constants.Error(
			"SERVICE_REQ_FAILED",
			"failed to declare '"+queueName+"' queue: "+err.Error(),
		)
	}
	return nil
}

// rmqQueueBind - bind queue to exchange
func (r *RMQHandler) rmqQueueBind(RMQChannel *amqp.Channel, fromExchangeName, toQueueName, routingKey string) APIError {
	err := RMQChannel.QueueBind(
		toQueueName,      // queue name
		routingKey,       // routing key
		fromExchangeName, // exchange
		false,
		nil,
	)
	if err != nil {
		return constants.Error(
			"SERVICE_REQ_FAILED",
			"failed to bind '"+toQueueName+"' queue to '"+
				fromExchangeName+"' exchange: "+err.Error(),
		)
	}
	return nil
}

// RMQQueueDeclareAndBind - declare queue & bind to exchange
func (r *RMQHandler) RMQQueueDeclareAndBind(task RMQQueueDeclareTask) APIError {
	// declare
	err := r.rmqQueueDeclare(
		r.Connections.Publish.Channel, // channel
		task.QueueName,                // queue name
		task.Durable,                  // is queue durable
		task.AutoDelete,               // auto-delete queue on consumer quit
	)
	if err != nil {
		return err
	}

	// bind
	return r.rmqQueueBind(
		r.Connections.Publish.Channel, // channel
		task.FromExchangeName,         // exchange name
		task.QueueName,                // queue name
		task.RoutingKey,               // routing key
	)
}

// DeclareQueues - declare RMQ exchanges list
func (r *RMQHandler) DeclareQueues(queues []string) APIError {
	for _, queueName := range queues {
		err := r.rmqQueueDeclare(
			r.Connections.Publish.Channel, // RMQ channel
			queueName,                     // queue name
			true,                          // durable
			false,                         // auto-delete
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// DeleteQueues - delete RMQ queues.
// map[manager name] -> array of queue names
func (r *RMQHandler) DeleteQueues(queueNames map[string][]string) APIError {
	for managerName, queueNames := range queueNames {
		for _, queueName := range queueNames {
			_, err := r.Connections.Publish.Channel.QueueDelete(
				queueName, // queue name
				false,     // if unused
				false,     // if empty
				true,      // no-wait
			)
			if err != nil {
				return constants.Error(
					"SERVICE_REQ_FAILED",
					"failed to delete "+managerName+" queue: "+err.Error(),
				)
			}
		}
	}
	return nil
}
