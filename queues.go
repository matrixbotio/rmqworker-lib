package rmqworker

import (
	"github.com/matrixbotio/constants-lib"
	"github.com/streadway/amqp"
)

// rmqQueueDeclare - declare RMQ queue
func (r *RMQHandler) rmqQueueDeclare(connectionPair *connectionPair, queueName string, durable bool, autodelete bool) APIError {
	connectionPair.rwMutex.RLock()
	defer connectionPair.rwMutex.RUnlock()

	args := amqp.Table{}
	if r.limitMessagesLifetime {
		args["x-message-ttl"] = r.messagesLifetime
	}

	_, err := connectionPair.Channel.QueueDeclare(
		queueName,  // name
		durable,    // durable
		autodelete, // delete when unused
		false,      // exclusive
		false,      // no-wait
		args,       // arguments
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
func (r *RMQHandler) rmqQueueBind(connectionPair *connectionPair, fromExchangeName, toQueueName, routingKey string) APIError {
	connectionPair.rwMutex.RLock()
	defer connectionPair.rwMutex.RUnlock()
	err := connectionPair.Channel.QueueBind(
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
		&r.Connections.Publish, // channel
		task.QueueName,         // queue name
		task.Durable,           // is queue durable
		task.AutoDelete,        // auto-delete queue on consumer quit
	)
	if err != nil {
		return err
	}

	// bind
	return r.rmqQueueBind(
		&r.Connections.Publish, // channel
		task.FromExchangeName,  // exchange name
		task.QueueName,         // queue name
		task.RoutingKey,        // routing key
	)
}

// DeclareQueues - declare RMQ exchanges list
func (r *RMQHandler) DeclareQueues(queues []string) APIError {
	for _, queueName := range queues {
		err := r.rmqQueueDeclare(
			&r.Connections.Publish, // RMQ channel
			queueName,              // queue name
			true,                   // durable
			false,                  // auto-delete
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
	r.Connections.Publish.rwMutex.RLock()
	defer r.Connections.Publish.rwMutex.RUnlock()
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
