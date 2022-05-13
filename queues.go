package rmqworker

import "github.com/matrixbotio/constants-lib"

// DeleteQueues - delete RMQ queues.
// map[manager name] -> array of queue names
func (r *RMQHandler) DeleteQueues(queueNames map[string][]string) APIError {
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

func (r *RMQHandler) queueDelete(managerName, queueName string) APIError {
	r.rlock()
	defer r.runlock()

	_, err := r.connPoolLightning.Channel().QueueDelete(
		queueName, // queue name
		false,     // if unused
		false,     // if empty
		true,      // no-wait
	)
	if err != nil {
		return constants.Error(
			"SERVICE_REQ_FAILED",
			"failed to delete "+queueName+" queue: "+err.Error(),
		)
	}
	return nil
}
