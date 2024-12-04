package rmqworker

import (
	"fmt"

	"github.com/matrixbotio/constants-lib"
	"github.com/streadway/amqp"

	"github.com/matrixbotio/rmqworker-lib/pkg/errs"
)

// DeleteQueues - delete RMQ queues.
// map[manager name] -> array of queue names
func (r *RMQHandler) DeleteQueues(queueNames map[string][]string) errs.APIError {
	for managerName, queueNames := range queueNames {
		for _, queueName := range queueNames {
			err := r.queueDelete(managerName, queueName)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *RMQHandler) getChannel() (*amqp.Channel, errs.APIError) {
	ch, rmqErr := r.publisher.Channel()
	if rmqErr != nil {
		return nil, constants.Error(
			"SERVICE_REQ_FAILED",
			"failed to get channel: "+rmqErr.Error(),
		)
	}
	return ch, nil
}

func (r *RMQHandler) queueDelete(managerName, queueName string) errs.APIError {
	r.rlock()
	defer r.runlock()

	ch, err := r.getChannel()
	if err != nil {
		return err
	}

	_, rmqErr := ch.QueueDelete(
		queueName, // queue name
		false,     // if unused
		false,     // if empty
		true,      // no-wait
	)
	if rmqErr != nil {
		return constants.Error(
			"SERVICE_REQ_FAILED",
			fmt.Sprintf(
				"failed to delete %s queue by %q: %s",
				queueName, managerName, rmqErr.Error(),
			),
		)
	}
	return nil
}
