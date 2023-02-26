package utils

type RMQDeliveryHandler interface {
	GetMessageBody() []byte
}
