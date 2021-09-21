package rmqworker

// RMQHandler - RMQ connection handler
type RMQHandler struct {
	ConnectionData rmqConnectionData
}

// NewRMQHandler - create new RMQHandler
func NewRMQHandler(connData rmqConnectionData) *RMQHandler {
	return &RMQHandler{
		ConnectionData: connData,
	}
}
