package rmqworker

// DeleteQueues - delete RMQ queues.
// map[manager name] -> array of queue names
/*func (r *RMQHandler) DeleteQueues(queueNames map[string][]string) APIError {
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
}*/
