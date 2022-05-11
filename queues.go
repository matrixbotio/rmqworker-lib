package rmqworker

import (
	"github.com/matrixbotio/constants-lib"
	"github.com/streadway/amqp"
)

// rmqQueueDeclare - declare RMQ queue
func (r *RMQHandler) rmqQueueDeclare(connectionPair *connectionPair, task RMQQueueDeclareSimpleTask) APIError {
	/*connectionPair.mutex.Lock()
	defer connectionPair.mutex.Unlock()
	connectionPair.rwMutex.RLock()
	defer connectionPair.rwMutex.RUnlock()*/

	args := amqp.Table{}
	if task.MessagesLifetime > 0 {
		args["x-message-ttl"] = task.MessagesLifetime
	}
	if task.QueueLength > 0 {
		args["x-max-length"] = task.QueueLength
	}
	if task.DisableOverflow {
		args["x-overflow"] = "reject-publish"
	}

	_, rmqErr := connectionPair.Channel.QueueDeclare(
		task.QueueName,  // name
		task.Durable,    // durable
		task.AutoDelete, // delete when unused
		false,           // exclusive
		false,           // no-wait
		args,            // arguments
	)
	if rmqErr != nil {
		return constants.Error(
			"SERVICE_REQ_FAILED",
			"failed to declare '"+task.QueueName+"' queue: "+rmqErr.Error(),
		)
	}
	return nil
}

// QueueDeclare - declare one RMQ queue
func (r *RMQHandler) QueueDeclare(task RMQQueueDeclareSimpleTask) APIError {
	return r.rmqQueueDeclare(&r.Connections.Publish, task)
}

// rmqQueueBind - bind queue to exchange
func (r *RMQHandler) rmqQueueBind(connectionPair *connectionPair, fromExchangeName, toQueueName, routingKey string) APIError {
	connectionPair.rwMutex.RLock()
	defer connectionPair.rwMutex.RUnlock()

	err := r.checkConnection()
	if err != nil {
		return err
	}

	rmqErr := connectionPair.Channel.QueueBind(
		toQueueName,      // queue name
		routingKey,       // routing key
		fromExchangeName, // exchange
		false,
		nil,
	)
	if rmqErr != nil {
		return constants.Error(
			"SERVICE_REQ_FAILED",
			"failed to bind '"+toQueueName+"' queue to '"+
				fromExchangeName+"' exchange: "+rmqErr.Error(),
		)
	}
	return nil
}

// RMQQueueDeclareAndBind - declare queue & bind to exchange
func (r *RMQHandler) RMQQueueDeclareAndBind(task RMQQueueDeclareTask) APIError {
	// declare
	err := r.rmqQueueDeclare(
		&r.Connections.Publish, // channel
		RMQQueueDeclareSimpleTask{
			QueueName:        task.QueueName,  // queue name
			Durable:          task.Durable,    // is queue durable
			AutoDelete:       task.AutoDelete, // auto-delete queue on consumer quit
			MessagesLifetime: task.MessagesLifetime,
			QueueLength:      task.QueueLength,
			DisableOverflow:  task.DisableOverflow,
		},
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

// DeclareQueues - declare RMQ queues list
func (r *RMQHandler) DeclareQueues(queues []string) APIError {
	for _, queueName := range queues {
		err := r.rmqQueueDeclare(
			&r.Connections.Publish, // RMQ channel
			RMQQueueDeclareSimpleTask{
				QueueName:  queueName,
				Durable:    true,
				AutoDelete: false,
			},
		)
		if err != nil {
			return err
		}
	}
	return nil
}

// DeclareQueuesExtended - declare RMQ queues list
func (r *RMQHandler) DeclareQueuesExtended(queues []RMQQueueDeclareSimpleTask) APIError {
	for _, queueData := range queues {
		err := r.rmqQueueDeclare(
			&r.Connections.Publish, // RMQ channel
			queueData,
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

	err := r.checkConnection()
	if err != nil {
		return err
	}

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
